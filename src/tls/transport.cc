#include "cornet/tls/transport.h"

#include "cornet/concurrency/combinators.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"

namespace cornet::tls {

transport_t::transport_t(tcp::socket_t sock) : io_(std::move(sock)) {}
transport_t::~transport_t() = default;
transport_t::transport_t(transport_t&&) noexcept = default;
transport_t& transport_t::operator=(transport_t&&) noexcept = default;

bool transport_t::is_tls() const {
  return std::holds_alternative<tls_stream_t>(io_);
}

int transport_t::native_fd() const {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) return tls->native_fd();
  return std::get<tcp::socket_t>(io_).native_fd();
}

socket_t::connect_awaiter transport_t::connect(context_t& ctx,
                                               const resolved_address& resolved) {
  CORNET_ASSERT(!is_tls(), "connect after start_tls() would bypass the record layer");
  return std::get<tcp::socket_t>(io_).connect(ctx, resolved);
}

ccoro_t<expected<size_t>> transport_t::recv(context_t& ctx, void* buf, size_t len) {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) {
    co_return co_await tls->recv(ctx, buf, len);
  }
  auto n = co_await std::get<tcp::socket_t>(io_).recv(ctx, buf, len);
  if (!n) co_return unexpected(n.error());
  co_return *n;
}

ccoro_t<expected<size_t>> transport_t::writev(context_t& ctx, const struct iovec* iov,
                                              size_t iov_len) {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) {
    co_return co_await tls->writev(ctx, iov, iov_len);
  }
  auto n = co_await std::get<tcp::socket_t>(io_).writev(ctx, iov, iov_len);
  if (!n) co_return unexpected(n.error());
  co_return *n;
}

coro_t<void> transport_t::close_gracefully(context_t& ctx, deadline_t& deadline,
                                           canceler_t& canceler,
                                           std::chrono::milliseconds window,
                                           uint32_t max_reads) {
  // peer's write side may already be gone; FIN went out, and there is nothing
  // else left worth fixing on this connection
  auto sd = co_await shutdown_write(ctx);
  (void)sd;

  // The drain owns the deadline's window for the duration of this call: the
  // caller's budget is replaced, and if it fired while draining the latch
  // stays for whoever checks afterwards. Both callers are running down.
  deadline.set_budget(window);
  char scratch[512];
  for (uint32_t i = 0; i < max_reads; ++i) {
    expected<size_t> n = co_await with_cancel(ctx, recv(ctx, scratch, sizeof(scratch)),
                                              canceler);
    if (!n || *n == 0) break;
  }
  deadline.set_budget(std::chrono::milliseconds{0});
}

ccoro_t<expected<void>> transport_t::writev_all(context_t& ctx, struct iovec* iov, size_t n,
                                                writev_stat_t* stat) {
  size_t done = 0;
  while (done < n) {
    if (stat) ++stat->calls;
    expected<size_t> w;
    if (auto* tls = std::get_if<tls_stream_t>(&io_)) {
      w = co_await tls->writev(ctx, iov + done, n - done);
    } else {
      // leaf awaiter awaits inside this frame, so the plain loop costs no
      // per-iteration coroutine allocation
      auto r = co_await std::get<tcp::socket_t>(io_).writev(ctx, iov + done, n - done);
      w = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    }
    if (!w) co_return unexpected(w.error());
    if (*w == 0) co_return unexpected(ECONNRESET);
    size_t remaining = *w;
    for (size_t i = done; i < n && remaining > 0; ++i) {
      size_t take = remaining < iov[i].iov_len ? remaining : iov[i].iov_len;
      iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + take;
      iov[i].iov_len -= take;
      remaining -= take;
    }
    while (done < n && iov[done].iov_len == 0) ++done;
    if (stat && done < n) ++stat->partial;
  }
  co_return {};
}

ccoro_t<expected<void>> transport_t::start_tls(context_t& ctx,
                                               std::shared_ptr<tls_context_t> cx,
                                               engine_mode_t mode,
                                               std::string_view server_name) {
  if (is_tls()) co_return unexpected(EALREADY);
  if (!cx) co_return tls_unexpected(tls_error_t::Init);
  // create() owns the validity decision: an invalid context in a TLS build is
  // Init; the whole enterprise is Disabled in a --without-tls build
  auto engine = tls_engine_t::create(*cx, mode, server_name);
  if (!engine) co_return unexpected(engine.error());

  tls_stream_t stream(std::move(std::get<tcp::socket_t>(io_)), std::move(*engine));
  auto ok = co_await stream.handshake(ctx);
  if (!ok) {
    // never lose the fd: a failed handshake still owes the caller a socket it
    // can abandon()/close()
    io_ = stream.release_socket();
    co_return ok;
  }
  io_ = std::move(stream);
  co_return {};
}

ccoro_t<expected<void>> transport_t::shutdown_write(context_t& ctx) {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) {
    co_return co_await tls->shutdown_write(ctx);
  }
  auto sd = co_await as_system(async_shutdown(ctx, native_fd(), SHUT_WR));
  if (!sd) co_return unexpected(sd.error());
  co_return {};
}

void transport_t::abandon(context_t& ctx) {
  int fd = native_fd();
  if (fd < 0) return;
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) {
    tls->release();
  } else {
    std::get<tcp::socket_t>(io_).release();
  }
  async_close(ctx, fd);
}

std::string_view transport_t::tls_version() const {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) return tls->version();
  return {};
}

std::string_view transport_t::tls_cipher() const {
  if (auto* tls = std::get_if<tls_stream_t>(&io_)) return tls->cipher();
  return {};
}

} // namespace cornet::tls
