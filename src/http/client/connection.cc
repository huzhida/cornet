#include "cornet/http/client/connection.h"

#include <netinet/tcp.h>
#include <sys/socket.h>

#include <cerrno>

#include <spdlog/spdlog.h>

#include "cornet/http/common/trace.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

namespace {

constexpr std::string_view kKeepAlive = "keep-alive";
constexpr std::string_view kChunked = "chunked";
constexpr std::string_view kAcceptAny = "*/*";
constexpr std::string_view kContinueToken = "100-continue";
constexpr char kCrlf[2] = {'\r', '\n'};

/**
 * @brief whether a request with no body should still say so.
 *
 * A POST or PUT with an empty body needs "Content-Length: 0": without it the peer
 * cannot tell an empty body from a body that has not arrived yet. A GET does not,
 * and sending one there makes some caches and proxies unhappy.
 */
bool method_needs_zero_length(method_t m) {
  switch (m) {
    case method_t::Post:
    case method_t::Put:
    case method_t::Patch:
      return true;
    default:
      return false;
  }
}

} // namespace

// ─────────────────────────── client_options_t ───────────────────────────

void client_options_t::load(config_t* config) {
  if (!config) return;
  auto at = [config](const char* path) { return config->at_path(path); };

  max_header_bytes = at("cornet.http.client.max_header_bytes").value_or(max_header_bytes);
  max_headers = at("cornet.http.client.max_headers").value_or(max_headers);
  max_trailers = at("cornet.http.client.max_trailers").value_or(max_trailers);
  max_body_bytes = at("cornet.http.client.max_body_bytes").value_or(max_body_bytes);
  aggregate_threshold = at("cornet.http.client.aggregate_threshold").value_or(aggregate_threshold);
  head_buffer_bytes = at("cornet.http.client.head_buffer_bytes").value_or(head_buffer_bytes);
  hdr_buffer_bytes = at("cornet.http.client.hdr_buffer_bytes").value_or(hdr_buffer_bytes);
  chunk_buffer_bytes = at("cornet.http.client.chunk_buffer_bytes").value_or(chunk_buffer_bytes);

  max_conns_per_host = at("cornet.http.client.max_conns_per_host").value_or(max_conns_per_host);
  max_idle_per_host = at("cornet.http.client.max_idle_per_host").value_or(max_idle_per_host);
  max_total_conns = at("cornet.http.client.max_total_conns").value_or(max_total_conns);

  max_retries = at("cornet.http.client.max_retries").value_or(max_retries);
  max_redirects = at("cornet.http.client.max_redirects").value_or(max_redirects);
  tcp_nodelay = at("cornet.http.client.tcp_nodelay").value_or(tcp_nodelay);
  send_user_agent = at("cornet.http.client.send_user_agent").value_or(send_user_agent);
  send_accept = at("cornet.http.client.send_accept").value_or(send_accept);
  user_agent = at("cornet.http.client.user_agent").value_or(user_agent);
  lenient_headers = at("cornet.http.client.lenient_headers").value_or(lenient_headers);
  lenient_chunked_length =
      at("cornet.http.client.lenient_chunked_length").value_or(lenient_chunked_length);
  lenient_keep_alive = at("cornet.http.client.lenient_keep_alive").value_or(lenient_keep_alive);

  dns_cache_entries = at("cornet.http.client.dns_cache_entries").value_or(dns_cache_entries);

  auto duration = [&](const char* path, std::chrono::milliseconds& target) {
    if (auto s = at(path).value<std::string_view>()) {
      target = std::chrono::duration_cast<std::chrono::milliseconds>(parse_time_str(*s));
    } else if (auto ms = at(path).value<int64_t>()) {
      target = std::chrono::milliseconds(*ms);
    }
  };
  duration("cornet.http.client.dns_cache_ttl", dns_cache_ttl);
  duration("cornet.http.client.connect_timeout", connect_timeout);
  duration("cornet.http.client.write_timeout", write_timeout);
  duration("cornet.http.client.response_timeout", response_timeout);
  duration("cornet.http.client.body_timeout", body_timeout);
  duration("cornet.http.client.total_timeout", total_timeout);
  duration("cornet.http.client.idle_timeout", idle_timeout);
  duration("cornet.http.client.pool_wait_timeout", pool_wait_timeout);
  duration("cornet.http.client.timer_tick", timer_tick);
}

// ───────────────────────── construction / teardown ─────────────────────────

client_connection_t::client_connection_t(context_t& ctx, tcp::socket_t sock,
                                         const client_options_t& opt, buffer_pool_t& pool,
                                         timer_wheel_t& wheel, client_metrics_t& metrics,
                                         std::string host, uint16_t port)
  : ctx_(ctx), sock_(std::move(sock)), opt_(opt), pool_(pool), wheel_(wheel), metrics_(metrics),
    canceler_(ctx), host_(std::move(host)), port_(port) {
  if (opt_.tcp_nodelay) {
    int on = 1;
    ::setsockopt(sock_.native_fd(), IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  }
  parser_.set_limits(parser_limits_t{
    .max_headers = opt_.max_headers,
    .max_trailers = opt_.max_trailers,
    .max_body_bytes = opt_.max_body_bytes,
    .lenient_headers = opt_.lenient_headers,
    .lenient_chunked_length = opt_.lenient_chunked_length,
    .lenient_keep_alive = opt_.lenient_keep_alive,
  });

  timer_.owner = this;
  timer_.on_expire = [](void* owner) {
    auto* self = static_cast<client_connection_t*>(owner);
    self->timed_out_ = true;
    self->broken_ = true;
    ++self->metrics_.timeouts;
    // Only this connection's inflight operation is cancelled. A context-wide sweep
    // would also reap other connections' writes.
    self->canceler_.cancel();
  };
}

client_connection_t::~client_connection_t() { close(); }

expected<void> client_connection_t::attach() {
  auto head = pool_.acquire(opt_.head_buffer_bytes);
  if (!head) return unexpected(ENOMEM);
  head_out_.reset(std::move(head));
  return {};
}

expected<void> client_connection_t::ensure_node() {
  if (node_) return {};
  node_ = inbound_t::create(pool_, opt_.max_header_bytes);
  if (!node_) return unexpected(ENOMEM);
  // Binding also drops any feed window into the buffers we handed to the previous
  // response, which is the only reason this order matters.
  parser_.bind(node_->head, node_->spill, node_->headers);
  parser_.reset();
  return {};
}

void client_connection_t::close() {
  wheel_.cancel(timer_);
  wheel_.cancel(idle_timer_);
  if (node_) {
    node_->destroy();
    node_ = nullptr;
  }
  head_out_.release();
  chunk_out_.release();
  int fd = sock_.release();
  if (fd >= 0) {
    ++metrics_.conn_closed;
    // release() first: the socket destructor would otherwise close the same number
    // again, and by then it may belong to a different connection
    async_close(ctx_, fd);
  }
  broken_ = true;
}

void client_connection_t::abort() {
  broken_ = true;
  canceler_.cancel();
}

// ───────────────────────────── opening ─────────────────────────────

coro_t<expected<std::unique_ptr<client_connection_t>>> client_connection_t::open(
    context_t& ctx, const client_options_t& opt, buffer_pool_t& pool, timer_wheel_t& wheel,
    client_metrics_t& metrics, std::string_view host, uint16_t port,
    const resolved_address* pre) {
  resolved_address addr{};
  if (pre && *pre) {
    addr = *pre;
  } else {
    ++metrics.dns_lookups;
    auto r = co_await cornet::resolve(ctx, host, port);
    if (!r) {
      ++metrics.connect_errors;
      co_return unexpected(r.error());
    }
    addr = *r;
  }

  std::unique_ptr<client_connection_t> conn;
  if (addr.addr.ss_family == AF_INET6) {
    tcp::v6::socket_t sock;
    if (sock.native_fd() < 0) {
      ++metrics.connect_errors;
      co_return unexpected(errno);
    }
    conn = std::make_unique<client_connection_t>(ctx, std::move(sock), opt, pool, wheel, metrics,
                                                 std::string(host), port);
  } else {
    tcp::v4::socket_t sock;
    if (sock.native_fd() < 0) {
      ++metrics.connect_errors;
      co_return unexpected(errno);
    }
    conn = std::make_unique<client_connection_t>(ctx, std::move(sock), opt, pool, wheel, metrics,
                                                 std::string(host), port);
  }

  if (auto ok = conn->attach(); !ok) co_return unexpected(ok.error());

  conn->arm_phase(opt.connect_timeout);
  auto c = co_await with_cancel(ctx, conn->sock_.connect(ctx, addr), conn->canceler_);
  wheel.cancel(conn->timer_);
  if (!c) {
    ++metrics.connect_errors;
    CORNET_HTTP_TRACE_LOG("client: connect to {}:{} failed ({})", host, port,
                          c.error().message());
    co_return unexpected(conn->timed_out_ ? error_t{ETIMEDOUT, error_domain::System} : c.error());
  }

  ++metrics.conn_created;
  conn->keep_alive_ = true;
  CORNET_HTTP_TRACE_LOG("client: connected fd={} to {}:{}", conn->native_fd(), host, port);
  co_return std::move(conn);
}

expected<std::unique_ptr<client_connection_t>> client_connection_t::adopt(
    context_t& ctx, tcp::socket_t sock, const client_options_t& opt, buffer_pool_t& pool,
    timer_wheel_t& wheel, client_metrics_t& metrics, std::string host, uint16_t port) {
  auto conn = std::make_unique<client_connection_t>(ctx, std::move(sock), opt, pool, wheel,
                                                    metrics, std::move(host), port);
  if (auto ok = conn->attach(); !ok) return unexpected(ok.error());
  ++metrics.conn_created;
  conn->keep_alive_ = true;
  return conn;
}

// ───────────────────────────── deadlines ─────────────────────────────

void client_connection_t::set_deadline(std::chrono::milliseconds total) {
  deadline_ns_ = total.count() > 0
                     ? ctx_.coarse_now_ns() + uint64_t(total.count()) * 1'000'000ull
                     : 0;
}

void client_connection_t::arm_phase(std::chrono::milliseconds phase) {
  auto use = phase;
  if (deadline_ns_ != 0) {
    auto now = ctx_.coarse_now_ns();
    if (now >= deadline_ns_) {
      // Already past the overall deadline: fire immediately rather than arming a
      // timer that would let this phase run for free.
      timed_out_ = true;
      broken_ = true;
      ++metrics_.timeouts;
      canceler_.cancel();
      return;
    }
    auto left = std::chrono::milliseconds((deadline_ns_ - now) / 1'000'000ull + 1);
    if (left < use) use = left;
  }
  if (use.count() <= 0) use = std::chrono::milliseconds(1);
  wheel_.arm(timer_, use);
}

// ───────────────────────────── writing ─────────────────────────────

// ─────────────────────────── head framing ───────────────────────────

void frame_request_head(out_buffer_t& out, const client_request_t& req,
                        const client_options_t& opt, bool chunked_upload) {
  serializer_t::request_line(out, req.method(), req.url().path(), req.url().query());

  if (!req.saw_host()) {
    // authority as written: adding an explicit ":80" the caller did not write would
    // change the header that virtual hosts and caches key on
    serializer_t::header(out, field_t::Host, req.url().authority());
  }
  if (opt.send_user_agent && !req.saw_user_agent()) {
    serializer_t::header(out, field_t::UserAgent, opt.user_agent);
  }
  if (opt.send_accept && !req.saw_accept()) {
    serializer_t::header(out, field_t::Accept, kAcceptAny);
  }
  if (!req.saw_connection()) {
    serializer_t::header(out, field_t::Connection, kKeepAlive);
  }

  if (chunked_upload) {
    if (!req.saw_transfer_encoding()) {
      serializer_t::header(out, field_t::TransferEncoding, kChunked);
    }
  } else if (!req.saw_content_length() && !req.saw_transfer_encoding()) {
    if (req.body_length() > 0 || method_needs_zero_length(req.method())) {
      serializer_t::header_u64(out, field_t::ContentLength, req.body_length());
    }
  }

  if (req.expects_continue() && !req.saw_expect() && req.body_length() > 0) {
    serializer_t::header(out, field_t::Expect, kContinueToken);
  }
}

void client_connection_t::frame_head(client_request_t& req, bool chunked_upload) {
  head_out_.clear();
  frame_request_head(head_out_, req, opt_, chunked_upload);
}

void client_connection_t::stage_request(client_request_t& req, bool head_only) {
  iov_n_ = 0;
  iov_head_ = 0;

  if (head_out_.size() > 0) {
    iov_[iov_n_++] = {head_out_.data(), head_out_.size()};
  }
  if (!req.staged_headers().empty()) {
    auto hdr = req.staged_headers();
    iov_[iov_n_++] = {const_cast<char*>(hdr.data()), hdr.size()};
  }
  // The blank line that ends the header section is its own segment: the user headers
  // are staged in the request and the framework headers here, so there is nowhere to
  // append it that would not mean copying one of them or mutating the request (which
  // would then double it on a retry).
  iov_[iov_n_++] = {const_cast<char*>(kCrlf), 2};

  if (!head_only && req.body_length() > 0) {
    auto body = req.body_view();
    if (!body.empty()) {
      iov_[iov_n_++] = {const_cast<char*>(body.data()), body.size()};
    }
  }
}

void client_connection_t::advance_iovecs(uint32_t written) {
  uint32_t left = written;
  while (left > 0 && iov_head_ < iov_n_) {
    auto& v = iov_[iov_head_];
    if (left >= v.iov_len) {
      left -= uint32_t(v.iov_len);
      v.iov_len = 0;
      ++iov_head_;
    } else {
      v.iov_base = static_cast<char*>(v.iov_base) + left;
      v.iov_len -= left;
      left = 0;
    }
  }
}

coro_t<expected<void>> client_connection_t::write_staged() {
  while (iov_head_ < iov_n_) {
    ++metrics_.writev_calls;
    auto n = co_await with_cancel(
        ctx_, sock_.writev(ctx_, iov_ + iov_head_, iov_n_ - iov_head_), canceler_);
    if (!n) {
      broken_ = true;
      if (timed_out_) co_return unexpected(ETIMEDOUT);
      co_return unexpected(n.error());
    }
    if (*n == 0) {
      broken_ = true;
      co_return unexpected(ECONNRESET);
    }
    auto before = iov_head_;
    advance_iovecs(uint32_t(*n));
    if (iov_head_ == before) ++metrics_.writev_partial;
  }
  co_return expected<void>{};
}

// ───────────────────────────── reading ─────────────────────────────

coro_t<expected<uint32_t>> client_connection_t::fill() {
  auto w = node_->head.writable();
  if (w.empty()) {
    // The head buffer filled without the headers completing. During the body phase
    // the window is rewound before every read, so this can only be a header section
    // that does not fit.
    co_return http_unexpected(http_error_t::HeaderTooLarge);
  }
  auto n = co_await with_cancel(ctx_, sock_.recv(ctx_, w.data(), w.size()), canceler_);
  if (!n) {
    if (timed_out_) co_return unexpected(ETIMEDOUT);
    co_return unexpected(n.error());
  }
  if (*n == 0) co_return uint32_t(0);
  node_->head.commit(uint32_t(*n));
  responded_ = true;
  co_return uint32_t(*n);
}

void client_connection_t::record_headers() {
  node_->status = status_t(parser_.status_code());
  node_->version = parser_.version();
  node_->keep_alive = parser_.keep_alive();
  node_->chunked = parser_.chunked();
  node_->has_content_length = parser_.has_content_length();
  node_->content_length = parser_.content_length();
  keep_alive_ = parser_.keep_alive();
  headers_done_ = true;
}

void client_connection_t::finish_message() {
  body_complete_ = true;
  if (parser_.has_pending_input()) {
    // This connection never pipelines, so nothing may follow a finished response.
    // Whatever is sitting there, the peer sent something we did not ask for: the
    // response in hand is fine, the connection is not.
    CORNET_HTTP_TRACE_LOG("client: fd={} unsolicited bytes after the response", native_fd());
    keep_alive_ = false;
    ++metrics_.protocol_errors;
  }
}

coro_t<expected<bool>> client_connection_t::read_headers(method_t m, bool stop_at_continue) {
  parser_.set_response_to(m);

  bool need_fill = !parser_.has_pending_input();
  for (;;) {
    parser_t::result_t r;
    if (need_fill) {
      arm_phase(opt_.response_timeout);
      auto got = co_await fill();
      if (!got) {
        broken_ = true;
        co_return unexpected(got.error());
      }
      if (*got == 0) {
        // Nothing at all arrived: on a connection taken from the pool this is the
        // idle-close race, and the caller may retry. Mid-message it is a truncated
        // response and nobody may retry.
        broken_ = true;
        co_return http_unexpected(http_error_t::ResponseIncomplete);
      }
      r = parser_.execute(node_->head.write_pos() - *got, *got);
    } else {
      r = parser_.resume();
    }

    switch (r) {
      case parser_t::result_t::NeedMore:
        need_fill = true;
        continue;

      case parser_t::result_t::HeadersReady:
      case parser_t::result_t::MessageReady: {
        auto st = status_t(parser_.status_code());
        if (status_is_informational(st) && st != status_t::SwitchingProtocols) {
          CORNET_HTTP_TRACE_LOG("client: fd={} interim {}", native_fd(), uint16_t(st));
          if (stop_at_continue && st == status_t::Continue) {
            parser_.reset();
            co_return true;
          }
          // Swallow it and keep reading: an interim response is not an answer.
          parser_.reset();
          need_fill = !parser_.has_pending_input();
          continue;
        }

        record_headers();
        body_window_ = parser_.consumed_offset();
        body_complete_ = false;
        if (r == parser_t::result_t::MessageReady) finish_message();
        CORNET_HTTP_TRACE_LOG("client: fd={} {} (cl={} chunked={} keep_alive={})", native_fd(),
                              uint16_t(node_->status), node_->content_length, node_->chunked,
                              node_->keep_alive);
        co_return false;
      }

      case parser_t::result_t::Error:
        broken_ = true;
        ++metrics_.protocol_errors;
        co_return unexpected(parser_.error());

      case parser_t::result_t::Upgrade:
        broken_ = true;
        co_return http_unexpected(http_error_t::BadUpgrade);

      case parser_t::result_t::BodyPaused:
        // Unreachable: no body sink is bound before the headers are in.
        broken_ = true;
        co_return http_unexpected(http_error_t::InvalidState);
    }
  }
}

// ─────────────────────────── body handling ───────────────────────────

expected<void> client_connection_t::prepare_body(bool stream) {
  streaming_ = stream;
  if (body_complete_) {
    parser_.set_body_sink(nullptr);
    return {};
  }

  if (stream) {
    parser_.set_body_sink(nullptr);   // nullptr means "pause on every run"
    return {};
  }

  if (parser_.has_content_length()) {
    if (parser_.content_length() > opt_.max_body_bytes) {
      return http_unexpected(http_error_t::BodyTooLarge);
    }
    // Exact size known: one reservation, no growth, and the parser copies straight
    // into it.
    auto ok = node_->body.reserve_exact(pool_, parser_.content_length());
    if (!ok) return ok;
    parser_.set_body_sink(&node_->body);
    return {};
  }

  // Length unknown until the body ends (chunked, or delimited by close). Take the
  // runs by hand and grow as they come, instead of reserving max_body_bytes for
  // every small response.
  parser_.set_body_sink(nullptr);
  uint32_t initial = opt_.aggregate_threshold < (16u << 10) ? opt_.aggregate_threshold
                                                            : (16u << 10);
  auto ok = node_->body.reserve_exact(pool_, initial ? initial : 4096);
  return ok;
}

expected<void> client_connection_t::append_body(std::string_view run) {
  if (run.empty()) return {};
  uint64_t want = uint64_t(node_->body.size()) + run.size();
  if (want > opt_.max_body_bytes) return http_unexpected(http_error_t::BodyTooLarge);
  if (want > node_->body.capacity()) {
    uint64_t next = node_->body.capacity() ? uint64_t(node_->body.capacity()) * 2
                                           : uint64_t(opt_.aggregate_threshold);
    while (next < want) next *= 2;
    if (next > opt_.max_body_bytes) next = opt_.max_body_bytes;
    if (auto ok = node_->body.grow(pool_, uint32_t(next)); !ok) return ok;
  }
  return node_->body.append(run.data(), uint32_t(run.size()));
}

coro_t<expected<uint32_t>> client_connection_t::refill_body() {
  // Reuse the region behind the header section. Body bytes are consumed as they
  // arrive — copied into the aggregation buffer or handed to the reader — so nothing
  // still referenced lives there, while every header offset sits below the mark and
  // is untouched. Without this a body could never exceed the head buffer.
  CORNET_ASSERT(!parser_.mid_header(),
                "rewinding under a half-accumulated header would splice its bytes");
  node_->head.rewind_to(body_window_);
  arm_phase(opt_.body_timeout);
  auto got = co_await fill();
  if (!got) co_return unexpected(got.error());
  co_return *got;
}

coro_t<expected<std::string_view>> client_connection_t::read_body() {
  if (!node_) co_return http_unexpected(http_error_t::InvalidState);

  for (;;) {
    auto pending = parser_.pending_body();
    if (!pending.empty()) {
      parser_.clear_pending_body();
      co_return pending;
    }
    if (body_complete_) co_return std::string_view{};

    auto r = parser_.resume();
    switch (r) {
      case parser_t::result_t::BodyPaused:
        continue;   // a run is pending now; the top of the loop takes it

      case parser_t::result_t::MessageReady:
        finish_message();
        co_return std::string_view{};

      case parser_t::result_t::Error:
        broken_ = true;
        ++metrics_.protocol_errors;
        co_return unexpected(parser_.error());

      case parser_t::result_t::NeedMore: {
        auto got = co_await refill_body();
        if (!got) {
          broken_ = true;
          co_return unexpected(got.error());
        }
        if (*got == 0) {
          // The peer closed. For a response with no framing that *is* the end of the
          // body; otherwise it is a truncated message.
          keep_alive_ = false;
          if (!parser_.has_content_length() && !parser_.chunked()) {
            if (parser_.finish() == parser_t::result_t::MessageReady) {
              finish_message();
              auto tail = parser_.pending_body();
              if (!tail.empty()) {
                parser_.clear_pending_body();
                co_return tail;
              }
              co_return std::string_view{};
            }
          }
          broken_ = true;
          co_return http_unexpected(http_error_t::ResponseIncomplete);
        }
        auto r2 = parser_.execute(body_window_, *got);
        if (r2 == parser_t::result_t::Error) {
          broken_ = true;
          ++metrics_.protocol_errors;
          co_return unexpected(parser_.error());
        }
        if (r2 == parser_t::result_t::MessageReady) {
          finish_message();
          auto tail = parser_.pending_body();
          if (!tail.empty()) {
            parser_.clear_pending_body();
            co_return tail;
          }
          co_return std::string_view{};
        }
        continue;
      }

      default:
        broken_ = true;
        co_return unexpected(parser_.error() ? parser_.error()
                                             : http_error(http_error_t::InvalidState));
    }
  }
}

coro_t<expected<void>> client_connection_t::drain_body() {
  for (;;) {
    auto run = co_await read_body();
    if (!run) co_return unexpected(run.error());
    if (run->empty()) co_return expected<void>{};
  }
}

coro_t<expected<void>> client_connection_t::aggregate_body() {
  if (body_complete_) co_return expected<void>{};

  // Unknown length: the runs come to us and we grow the buffer ourselves.
  if (!parser_.has_content_length()) {
    for (;;) {
      auto run = co_await read_body();
      if (!run) co_return unexpected(run.error());
      if (run->empty()) co_return expected<void>{};
      if (auto ok = append_body(*run); !ok) {
        broken_ = true;
        co_return ok;
      }
    }
  }

  // Known length: the parser copies into the reservation, so all we do is keep
  // feeding it.
  for (;;) {
    auto r = parser_.resume();
    if (r == parser_t::result_t::MessageReady) {
      finish_message();
      co_return expected<void>{};
    }
    if (r == parser_t::result_t::Error) {
      broken_ = true;
      ++metrics_.protocol_errors;
      co_return unexpected(parser_.error());
    }
    if (r != parser_t::result_t::NeedMore) {
      broken_ = true;
      co_return http_unexpected(http_error_t::InvalidState);
    }

    auto got = co_await refill_body();
    if (!got) {
      broken_ = true;
      co_return unexpected(got.error());
    }
    if (*got == 0) {
      broken_ = true;
      keep_alive_ = false;
      co_return http_unexpected(http_error_t::ResponseIncomplete);
    }
    auto r2 = parser_.execute(body_window_, *got);
    if (r2 == parser_t::result_t::Error) {
      broken_ = true;
      ++metrics_.protocol_errors;
      co_return unexpected(parser_.error());
    }
    if (r2 == parser_t::result_t::MessageReady) {
      finish_message();
      co_return expected<void>{};
    }
  }
}

// ─────────────────────────── the exchange ───────────────────────────

expected<client_response_t> client_connection_t::take_response() {
  if (!node_ || !headers_done_ || !body_complete_) {
    return http_unexpected(http_error_t::InvalidState);
  }
  wheel_.cancel(timer_);
  auto* node = node_;
  node_ = nullptr;
  headers_done_ = false;
  streaming_ = false;
  ++metrics_.responses;
  // The parser still points at the buffers we are handing over; ensure_node() rebinds
  // it before the next exchange touches it.
  return client_response_t{node};
}

coro_t<expected<client_response_t>> client_connection_t::exchange(client_request_t& req) {
  auto sent = co_await send_and_read_headers(req);
  if (!sent) co_return unexpected(sent.error());

  if (auto ok = prepare_body(false); !ok) {
    broken_ = true;
    co_return unexpected(ok.error());
  }
  if (auto ok = co_await aggregate_body(); !ok) co_return unexpected(ok.error());
  co_return take_response();
}

coro_t<expected<void>> client_connection_t::begin_exchange(client_request_t& req) {
  auto sent = co_await send_and_read_headers(req);
  if (!sent) co_return unexpected(sent.error());
  if (auto ok = prepare_body(true); !ok) {
    broken_ = true;
    co_return unexpected(ok.error());
  }
  co_return expected<void>{};
}

coro_t<expected<void>> client_connection_t::send_and_read_headers(client_request_t& req) {
  if (req.failed()) co_return unexpected(req.error());
  if (auto ok = ensure_node(); !ok) co_return unexpected(ok.error());

  ++metrics_.requests;
  ++exchanges_;
  responded_ = false;
  body_complete_ = false;
  headers_done_ = false;
  chunked_upload_ = false;

  frame_head(req, false);
  if (head_out_.failed()) {
    broken_ = true;
    co_return unexpected(head_out_.error());
  }

  bool expect_continue = req.expects_continue() && req.body_length() > 0;

  // With Expect: 100-continue the head goes out alone; the body waits for the peer's
  // permission, which is the only thing that makes the header worth sending.
  stage_request(req, expect_continue);
  arm_phase(opt_.write_timeout);
  if (auto ok = co_await write_staged(); !ok) co_return unexpected(ok.error());

  if (expect_continue) {
    auto paused = co_await read_headers(req.method(), true);
    if (!paused) co_return unexpected(paused.error());
    if (*paused) {
      // Green light: send the body, then read the real response.
      iov_n_ = 0;
      iov_head_ = 0;
      auto body = req.body_view();
      if (!body.empty()) {
        iov_[iov_n_++] = {const_cast<char*>(body.data()), body.size()};
      }
      arm_phase(opt_.write_timeout);
      if (auto ok = co_await write_staged(); !ok) co_return unexpected(ok.error());
    } else {
      // A final response instead: the peer refused the body, so it never went out.
      // The connection cannot be reused — we announced a body and did not send it.
      keep_alive_ = false;
      co_return expected<void>{};
    }
  }

  auto got = co_await read_headers(req.method(), false);
  if (!got) co_return unexpected(got.error());
  co_return expected<void>{};
}

// ────────────────────────── chunked upload ──────────────────────────

coro_t<expected<void>> client_connection_t::begin_chunked(client_request_t& req) {
  if (req.failed()) co_return unexpected(req.error());
  if (req.body_length() > 0) {
    // A staged body and a streamed body are two different requests.
    co_return http_unexpected(http_error_t::InvalidState);
  }
  if (auto ok = ensure_node(); !ok) co_return unexpected(ok.error());
  if (!chunk_out_.attached()) {
    auto lease = pool_.acquire(opt_.chunk_buffer_bytes);
    if (!lease) co_return unexpected(ENOMEM);
    chunk_out_.reset(std::move(lease));
  }

  ++metrics_.requests;
  ++exchanges_;
  responded_ = false;
  body_complete_ = false;
  headers_done_ = false;
  chunked_upload_ = true;

  frame_head(req, true);
  if (head_out_.failed()) {
    broken_ = true;
    co_return unexpected(head_out_.error());
  }

  stage_request(req, true);
  arm_phase(opt_.write_timeout);
  co_return co_await write_staged();
}

coro_t<expected<void>> client_connection_t::write_chunk(std::string_view data) {
  if (!chunked_upload_) co_return http_unexpected(http_error_t::InvalidState);
  if (data.empty()) co_return expected<void>{};

  chunk_out_.clear();
  char line[24];
  auto n = write_chunk_size(line, data.size());
  chunk_out_.put(line, n);
  if (chunk_out_.failed()) {
    broken_ = true;
    co_return unexpected(chunk_out_.error());
  }

  // size line, data (referenced, never copied), trailing CRLF
  iov_n_ = 0;
  iov_head_ = 0;
  iov_[iov_n_++] = {chunk_out_.data(), chunk_out_.size()};
  iov_[iov_n_++] = {const_cast<char*>(data.data()), data.size()};
  iov_[iov_n_++] = {const_cast<char*>(kCrlf), 2};

  arm_phase(opt_.write_timeout);
  co_return co_await write_staged();
}

coro_t<expected<void>> client_connection_t::finish_chunks() {
  if (!chunked_upload_) co_return http_unexpected(http_error_t::InvalidState);
  static constexpr char kLastChunk[5] = {'0', '\r', '\n', '\r', '\n'};
  iov_n_ = 0;
  iov_head_ = 0;
  iov_[iov_n_++] = {const_cast<char*>(kLastChunk), sizeof(kLastChunk)};
  arm_phase(opt_.write_timeout);
  co_return co_await write_staged();
}

coro_t<expected<client_response_t>> client_connection_t::read_response(method_t m) {
  auto got = co_await read_headers(m, false);
  if (!got) co_return unexpected(got.error());
  if (auto ok = prepare_body(false); !ok) {
    broken_ = true;
    co_return unexpected(ok.error());
  }
  if (auto ok = co_await aggregate_body(); !ok) co_return unexpected(ok.error());
  co_return take_response();
}

// ─────────────────────────── introspection ───────────────────────────

status_t client_connection_t::status() const {
  return node_ && headers_done_ ? node_->status : status_t(0);
}

const headers_t* client_connection_t::headers() const {
  return node_ && headers_done_ ? &node_->headers : nullptr;
}

bool client_connection_t::alive_hint() const {
  int fd = sock_.native_fd();
  if (fd < 0) return false;
  char probe = 0;
  auto n = ::recv(fd, &probe, 1, MSG_PEEK | MSG_DONTWAIT);
  if (n == 0) return false;   // FIN while the connection sat idle
  if (n > 0) return false;    // bytes we never asked for: the stream is desynchronised
  return errno == EAGAIN || errno == EWOULDBLOCK;
}

} // namespace cornet::http
