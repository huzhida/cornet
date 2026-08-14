#include "cornet/http/server.h"

#include "cornet/http/trace.h"

#include <csignal>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>

#include <spdlog/spdlog.h>

#include "cornet/concurrency/combinators.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"
#include "cornet/scheduling/runtime.h"

namespace cornet::http {

namespace {

/**
 * @brief whether an accept error means the process is out of descriptors.
 *
 * This deserves its own branch because the naive reaction — keep looping — turns
 * a descriptor shortage into a busy loop that burns a whole core and starves the
 * connections that are already open.
 */
bool fd_exhausted(int err) {
  return err == EMFILE || err == ENFILE || err == ENOBUFS || err == ENOMEM;
}

} // namespace

server_t::server_t(context_t& ctx, server_options_t opt)
  : ctx_(ctx), opt_(std::move(opt)), wheel_(ctx, opt_.timer_tick),
    pool_(buffer_pool_t::local()) {
  opt_.load(ctx.config());
}

server_t::~server_t() = default;

expected<void> server_t::listen() {
  return listen(opt_.address, opt_.port);
}

expected<void> server_t::listen(std::string_view address, uint16_t port) {
  opt_.address = std::string(address);
  opt_.port = port;

  auto sock = std::make_unique<tcp::v4::socket_t>();
  sock->address_reuse(true);
  if (opt_.reuse_port) sock->port_reuse(true);
  if (auto ok = sock->listen(address, port); !ok) {
    return ok;
  }
  listener_ = std::move(sock);
  CORNET_HTTP_TRACE_LOG("listen: {}:{} fd={}", opt_.address, opt_.port, listener_->native_fd());
  return {};
}

coro_t<void> server_t::serve() {
  if (!listener_) {
    SPDLOG_ERROR("http: serve() called before listen()");
    co_return;
  }

  state_ = state_t::Running;
  scope_ = std::make_unique<scope_t>(ctx_);
  CORNET_HTTP_TRACE_LOG("serve: start, listener fd={}", listener_->native_fd());

  // One timeout SQE for the whole context, whatever the connection count.
  ctx_.spawn(wheel_.run());

  co_await accept_loop();
  CORNET_HTTP_TRACE_LOG("serve: accept loop exited, {} connection(s) still open", conns_);

  // Connections were already asked to wind up in drain(); waiting on the scope is
  // what guarantees each of them finished writing before we return.
  if (scope_) {
    auto joined = co_await scope_join_awaiter{*scope_};
    (void)joined;
  }
  wheel_.stop();
  state_ = state_t::Stopped;
  CORNET_HTTP_TRACE_LOG("serve: stopped (requests={} responses={})",
                        metrics_.requests, metrics_.responses);
  co_return;
}

coro_t<void> server_t::accept_loop() {
  while (state_ == state_t::Running) {
    // Deliberately NOT as_system(). A listener's pending accept is exactly what
    // should keep the context alive while the server sits idle. Marking it as
    // framework io makes user_idle() true the moment the accept is armed, and the
    // run loop reacts by switching to Canceling and sweeping away that very
    // accept — the process exits before the first client can connect.
    //
    // Ending this loop is drain()'s job instead: it closes the listener, which
    // completes the pending accept with an error. Once that happens and the
    // connections finish, user_idle() becomes true on its own and the context
    // winds itself down; nobody has to call ctx.shutdown() at all.
    auto sock = co_await listener_->accept(ctx_);
    if (!sock) {
      auto err = sock.error().code;
      CORNET_HTTP_TRACE_LOG("accept: failed ({})", sock.error().message());
      if (err == ECANCELED || err == EBADF || err == EINVAL) break;  // listener closed
      if (fd_exhausted(err)) {
        SPDLOG_WARN("http: accept out of descriptors ({}), backing off", sock.error().message());
        auto slept = co_await as_system(sleep(ctx_, std::chrono::milliseconds(50)));
        (void)slept;
        continue;
      }
      SPDLOG_DEBUG("http: accept failed: {}", sock.error().message());
      continue;
    }

    CORNET_HTTP_TRACE_LOG("accept: fd={} conns={}", sock->native_fd(), conns_ + 1);

    if (state_ != state_t::Running) {
      async_close(ctx_, sock->release());
      break;
    }

    if (conns_ >= opt_.max_connections) {
      // Refuse before spending a coroutine frame on it.
      SPDLOG_WARN("http: connection limit {} reached, refusing", opt_.max_connections);
      async_close(ctx_, sock->release());
      continue;
    }

    scope_->spawn(serve_connection(std::move(*sock)));
  }
  co_return;
}

coro_t<void> server_t::serve_connection(tcp::socket_t sock) {
  connection_t conn(ctx_, std::move(sock), opt_, pool_, wheel_, metrics_);
  ++conns_;
  active_.push_back(&conn);
  CORNET_HTTP_TRACE_LOG("conn fd={}: begin", conn.native_fd());

  co_await conn.run(router_);

  CORNET_HTTP_TRACE_LOG("conn: end, conns={}", conns_ - 1);
  auto it = std::find(active_.begin(), active_.end(), &conn);
  if (it != active_.end()) active_.erase(it);
  --conns_;
  co_return;
}

void server_t::drain() {
  if (state_ != state_t::Running) return;
  state_ = state_t::Draining;
  CORNET_HTTP_TRACE_LOG("drain: begin, {} connection(s) active", conns_);

  // 1. stop accepting. Closing the listener completes the pending accept with an
  //    error, which is what ends the accept loop — and, once the connections are
  //    done, lets user_idle() become true so the context can wind itself down.
  if (listener_) {
    ::shutdown(listener_->native_fd(), SHUT_RDWR);
    ::close(listener_->release());
  }

  // 2. ask each connection to finish its current exchange. This cancels only
  //    their reads: a response already being written still goes out, which is the
  //    difference between a graceful drain and a truncated one.
  for (auto* conn : active_) {
    conn->request_close();
  }
}

void server_t::stop() {
  drain();
  state_ = state_t::Stopped;
  if (scope_) scope_->cancel();
  wheel_.stop();
}

// ─────────────────────────── multi-threaded ───────────────────────────

void serve(runtime_t& rt, server_options_t opt,
           const std::function<void(router_t&)>& configure) {
  struct worker_t {
    std::unique_ptr<server_t> server;
  };
  // one server per worker; nothing is shared, so nothing needs a lock
  auto workers = std::make_shared<std::vector<worker_t>>(rt.size());

  rt.start([&opt, &configure, workers](size_t idx, context_t& ctx) {
    auto server = std::make_unique<server_t>(ctx, opt);
    configure(server->router());
    if (auto ok = server->listen(); !ok) {
      SPDLOG_ERROR("http: worker {} failed to listen: {}", idx, ok.error().message());
      return;
    }
    ctx.on_signal({SIGINT, SIGTERM}, [srv = server.get()](int) { srv->drain(); });
    ctx.spawn(server->serve());
    (*workers)[idx].server = std::move(server);
  });

  rt.join();
}

} // namespace cornet::http
