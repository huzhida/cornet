#include "cornet/http/connection.h"

#include <netinet/tcp.h>
#include <sys/socket.h>

#include <spdlog/spdlog.h>

#include "cornet/http/trace.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

namespace {

constexpr std::string_view kConnClose = "close";
constexpr std::string_view kConnKeepAlive = "keep-alive";
constexpr std::string_view kServerName = "cornet";

/**
 * @brief a status a keep-alive connection cannot survive: the framing is either
 * unknown or the peer is expected to go away.
 */
bool status_forces_close(status_t s) {
  switch (s) {
    case status_t::BadRequest:
    case status_t::RequestTimeout:
    case status_t::ContentTooLarge:
    case status_t::UriTooLong:
    case status_t::RequestHeaderFieldsTooLarge:
    case status_t::InternalServerError:
    case status_t::NotImplemented:
    case status_t::ServiceUnavailable:
    case status_t::HttpVersionNotSupported:
      return true;
    default:
      return false;
  }
}

} // namespace

// ─────────────────────────── server_options_t ───────────────────────────

void server_options_t::load(config_t* config) {
  if (!config) return;
  auto at = [config](const char* path) { return config->at_path(path); };

  port = at("cornet.http.server.port").value_or(port);
  address = at("cornet.http.server.address").value_or(address);
  max_header_bytes = at("cornet.http.server.max_header_bytes").value_or(max_header_bytes);
  max_headers = at("cornet.http.server.max_headers").value_or(max_headers);
  max_body_bytes = at("cornet.http.server.max_body_bytes").value_or(max_body_bytes);
  aggregate_threshold = at("cornet.http.server.aggregate_threshold").value_or(aggregate_threshold);
  stream_window_bytes = at("cornet.http.server.stream_window_bytes").value_or(stream_window_bytes);
  max_pipelined = at("cornet.http.server.max_pipelined").value_or(max_pipelined);
  max_connections = at("cornet.http.server.max_connections").value_or(max_connections);
  tcp_nodelay = at("cornet.http.server.tcp_nodelay").value_or(tcp_nodelay);
  reuse_port = at("cornet.http.server.reuse_port").value_or(reuse_port);
  serve_date_header = at("cornet.http.server.serve_date_header").value_or(serve_date_header);
  serve_server_header = at("cornet.http.server.serve_server_header").value_or(serve_server_header);
  lenient_headers = at("cornet.http.server.lenient_headers").value_or(lenient_headers);
  lenient_chunked_length = at("cornet.http.server.lenient_chunked_length").value_or(lenient_chunked_length);
  lenient_keep_alive = at("cornet.http.server.lenient_keep_alive").value_or(lenient_keep_alive);

  auto duration = [&](const char* path, std::chrono::milliseconds& target) {
    if (auto s = at(path).value<std::string_view>()) {
      target = std::chrono::duration_cast<std::chrono::milliseconds>(parse_time_str(*s));
    } else if (auto ms = at(path).value<int64_t>()) {
      target = std::chrono::milliseconds(*ms);
    }
  };
  duration("cornet.http.server.idle_timeout", idle_timeout);
  duration("cornet.http.server.header_timeout", header_timeout);
  duration("cornet.http.server.body_timeout", body_timeout);
  duration("cornet.http.server.timer_tick", timer_tick);
  duration("cornet.http.server.drain_timeout", drain_timeout);
}

// ──────────────────────────── connection_t ────────────────────────────

connection_t::connection_t(context_t& ctx, tcp::socket_t sock, const server_options_t& opt,
                           buffer_pool_t& pool, timer_wheel_t& wheel,
                           connection_metrics_t& metrics)
  : ctx_(ctx), sock_(std::move(sock)), opt_(opt), pool_(pool), wheel_(wheel),
    metrics_(metrics), canceler_(ctx), parser_(parser_t::type_t::Request), reader_(*this) {
  if (opt_.tcp_nodelay) {
    int on = 1;
    ::setsockopt(sock_.native_fd(), IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  }
  parser_.bind(in_, spill_, headers_);
  parser_.set_limits(parser_limits_t{
    .max_headers = opt_.max_headers,
    .max_body_bytes = opt_.max_body_bytes,
    .lenient_headers = opt_.lenient_headers,
    .lenient_chunked_length = opt_.lenient_chunked_length,
    .lenient_keep_alive = opt_.lenient_keep_alive,
  });
  req_.bind(parser_, headers_, params_);
  resp_.bind(hdr_out_, body_out_);

  timer_.owner = this;
  timer_.on_expire = [](void* owner) {
    auto* self = static_cast<connection_t*>(owner);
    self->timed_out_ = true;
    ++self->metrics_.timeouts;
    // Cancel only this connection's inflight read. A context-wide sweep would also
    // reap writes in flight and truncate a response that is already on its way.
    self->canceler_.cancel();
  };
}

connection_t::~connection_t() {
  wheel_.cancel(timer_);
  release_buffers();
}

expected<void> connection_t::attach_buffers() {
  auto head = pool_.acquire(opt_.max_header_bytes);
  if (!head) return unexpected(ENOMEM);
  in_.reset(std::move(head));

  auto ho = pool_.acquire(opt_.head_buffer_bytes);
  if (!ho) return unexpected(ENOMEM);
  head_out_.reset(std::move(ho));

  auto hd = pool_.acquire(opt_.hdr_buffer_bytes);
  if (!hd) return unexpected(ENOMEM);
  hdr_out_.reset(std::move(hd));

  auto bo = pool_.acquire(opt_.body_buffer_bytes);
  if (!bo) return unexpected(ENOMEM);
  body_out_.reset(std::move(bo));

  return {};
}

void connection_t::release_buffers() {
  in_.release();
  head_out_.release();
  hdr_out_.release();
  body_out_.release();
  body_.release();
}

void connection_t::request_close() {
  closing_ = true;
  // Only the read side is cancelled; anything already queued for write still goes
  // out, which is what keeps a graceful drain from truncating responses.
  canceler_.cancel();
}

// ─────────────────────────────── reading ───────────────────────────────

coro_t<expected<uint32_t>> connection_t::fill() {
  auto w = in_.writable();
  if (w.empty()) {
    // The header section filled the whole buffer without completing.
    CORNET_HTTP_TRACE_LOG("fd={}: header buffer full ({}B), answering 431",
                          sock_.native_fd(), in_.capacity());
    co_return http_unexpected(http_error_t::HeaderTooLarge);
  }
  CORNET_HTTP_TRACE_LOG("fd={}: recv armed, window={}", sock_.native_fd(), w.size());
  auto n = co_await with_cancel(ctx_, sock_.recv(ctx_, w.data(), w.size()), canceler_);
  if (!n) {
    CORNET_HTTP_TRACE_LOG("fd={}: recv failed ({}), timed_out={}",
                          sock_.native_fd(), n.error().message(), timed_out_);
    if (timed_out_) co_return unexpected(ETIMEDOUT);
    co_return unexpected(n.error());
  }
  CORNET_HTTP_TRACE_LOG("fd={}: recv {} bytes", sock_.native_fd(), *n);
  if (*n == 0) co_return uint32_t(0);
  in_.commit(uint32_t(*n));
  co_return uint32_t(*n);
}

// ────────────────────────── body mode selection ──────────────────────────

expected<void> connection_t::prepare_body(const route_t* route) {
  auto policy = route ? route->body : body_policy_t::Auto;

  bool has_body = parser_.chunked() || parser_.content_length() > 0;
  if (!has_body) {
    body_mode_ = body_mode_t::Empty;
    body_.clear();
    req_.set_body({});
    req_.set_reader(nullptr);
    streaming_ = false;
    body_complete_ = true;
    parser_.set_body_sink(nullptr);
    return {};
  }

  if (parser_.has_content_length() && parser_.content_length() > opt_.max_body_bytes) {
    return http_unexpected(http_error_t::BodyTooLarge);
  }

  bool aggregate;
  switch (policy) {
    case body_policy_t::Aggregate:
      aggregate = true;
      break;
    case body_policy_t::Stream:
      aggregate = false;
      break;
    case body_policy_t::Auto:
    default:
      // Aggregate only when the exact size is known up front and small enough to
      // reserve in one go. Without Content-Length the size is unknown until the
      // last chunk, so buffering would mean growing without bound.
      aggregate = parser_.has_content_length() &&
                  parser_.content_length() <= opt_.aggregate_threshold;
      break;
  }

  if (aggregate) {
    if (!parser_.has_content_length()) {
      // chunked but the route insists on aggregating: cap at max_body_bytes
      auto ok = body_.reserve_exact(pool_, opt_.max_body_bytes);
      if (!ok) return ok;
    } else {
      auto ok = body_.reserve_exact(pool_, parser_.content_length());
      if (!ok) return ok;
    }
    parser_.set_body_sink(&body_);
    body_mode_ = body_mode_t::Exact;
    streaming_ = false;
    body_complete_ = false;
    req_.set_reader(nullptr);
    return {};
  }

  auto ok = body_.reserve_window(pool_, opt_.stream_window_bytes);
  if (!ok) return ok;
  parser_.set_body_sink(nullptr);   // nullptr means "pause on each run"
  body_mode_ = body_mode_t::Stream;
  streaming_ = true;
  body_complete_ = false;
  req_.set_body({});
  req_.set_reader(&reader_);
  return {};
}

// ──────────────────────────── streaming body ────────────────────────────

coro_t<expected<std::string_view>> connection_t::read_body_chunk() {
  while (true) {
    auto pending = parser_.pending_body();
    if (!pending.empty()) {
      parser_.clear_pending_body();
      co_return pending;
    }
    if (body_complete_) co_return std::string_view{};

    auto r = parser_.resume();
    switch (r) {
      case parser_t::result_t::BodyPaused:
        continue;   // a run is now pending; loop picks it up
      case parser_t::result_t::MessageReady:
        body_complete_ = true;
        co_return std::string_view{};
      case parser_t::result_t::Error:
        co_return unexpected(parser_.error());
      case parser_t::result_t::NeedMore: {
        // Nothing buffered: the rest of the body is still on the wire. Compaction
        // is safe here only because body bytes are consumed immediately and no
        // header view points into the region being reclaimed.
        wheel_.arm(timer_, opt_.body_timeout);
        auto got = co_await fill();
        if (!got) co_return unexpected(got.error());
        if (*got == 0) co_return unexpected(ECONNRESET);
        auto r2 = parser_.execute(in_.write_pos() - *got, *got);
        if (r2 == parser_t::result_t::Error) co_return unexpected(parser_.error());
        if (r2 == parser_t::result_t::MessageReady) {
          body_complete_ = true;
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
        co_return unexpected(parser_.error() ? parser_.error()
                                             : http_error(http_error_t::InvalidState));
    }
  }
}

coro_t<expected<std::string_view>> body_reader_t::read() {
  auto r = co_await conn_->read_body_chunk();
  if (!r) co_return r;
  if (r->empty()) complete_ = true;
  co_return r;
}

coro_t<expected<void>> body_reader_t::drain() {
  while (!complete_) {
    auto r = co_await conn_->read_body_chunk();
    if (!r) co_return unexpected(r.error());
    if (r->empty()) {
      complete_ = true;
      break;
    }
  }
  co_return expected<void>{};
}

// ──────────────────────────── dispatch ────────────────────────────

coro_t<void> connection_t::invoke(const route_t& route) {
  // A synchronous handler is called directly: no frame, no suspension, no trip
  // through the scheduler. That is the whole reason the two kinds are separate.
  if (route.kind == route_t::kind_t::Sync) {
    route.sync_fn(req_, resp_);
    co_return;
  }
  co_await route.async_fn(req_, resp_);
}

coro_t<bool> connection_t::run_filters(const router_t& router) {
  for (const auto& f : router.filters()) {
    if (f.kind == filter_entry_t::kind_t::Sync) {
      if (!f.sync_fn(req_, resp_)) co_return false;
    } else {
      if (!co_await f.async_fn(req_, resp_)) co_return false;
    }
  }
  co_return true;
}

// ──────────────────────────── framing ────────────────────────────

void connection_t::frame_response(bool close_after) {
  resp_.seal_headers();

  auto status = resp_.status();
  bool head_request = req_.method() == method_t::Head;
  bool no_body_allowed = status_forbids_body(status);

  pending_t p{};
  p.head_off = head_out_.size();

  serializer_t::status_line(head_out_, status);

  if (opt_.serve_date_header && !resp_.saw_date()) {
    // The date string is rendered once per event-loop turn by the context's coarse
    // clock, so every response here is a memcpy rather than a gmtime plus format.
    serializer_t::date_header(head_out_, ctx_.http_date(), ctx_.http_date_len());
  }
  if (opt_.serve_server_header) {
    serializer_t::header(head_out_, field_t::Server, kServerName);
  }
  if (!resp_.saw_connection()) {
    serializer_t::header(head_out_, field_t::Connection,
                         close_after ? kConnClose : kConnKeepAlive);
  }
  if (!no_body_allowed && !resp_.saw_content_length() && !resp_.saw_transfer_encoding()) {
    // Emitted even for HEAD: the response must describe the body it would have
    // sent, while sending none.
    serializer_t::header_u64(head_out_, field_t::ContentLength, resp_.body_length());
  }

  p.head_len = head_out_.size() - p.head_off;
  p.hdr_off = resp_.hdr_offset();
  p.hdr_len = resp_.hdr_length();

  if (!no_body_allowed && !head_request && resp_.body_length() > 0) {
    p.source = resp_.body_source();
    p.body_len = uint32_t(resp_.body_length());
    if (p.source == body_source_t::Inline) {
      p.body_off = resp_.inline_body_offset();
    } else {
      p.external = resp_.external_body();
      p.owned = resp_.take_owned();
    }
  }

  CORNET_HTTP_TRACE_LOG("fd={}: framed status={} head={}B hdr={}B body={}B close_after={}",
                        sock_.native_fd(), uint16_t(status), p.head_len, p.hdr_len,
                        p.body_len, close_after);
  if (pending_n_ < std::size(pending_)) {
    pending_[pending_n_++] = std::move(p);
  } else {
    head_out_.fail(http_error(http_error_t::OutputOverflow));
  }
  ++metrics_.responses;
}

void connection_t::write_error(status_t status) {
  resp_.begin();
  resp_.status(status);
  const char* phrase = reason_phrase(status);
  resp_.header(field_t::ContentType, "text/plain; charset=utf-8");
  resp_.body_static(std::string_view(phrase));
  frame_response(true);
  close_after_flush_ = true;
  ++metrics_.protocol_errors;
}

void connection_t::write_continue() {
  // 100 Continue is a bare status line with no headers and no body; it precedes
  // the real response on the same connection.
  pending_t p{};
  p.head_off = head_out_.size();
  serializer_t::status_line(head_out_, status_t::Continue);
  head_out_.put_crlf();
  p.head_len = head_out_.size() - p.head_off;
  if (pending_n_ < std::size(pending_)) {
    pending_[pending_n_++] = std::move(p);
  }
}

// ──────────────────────────── writing ────────────────────────────

uint32_t connection_t::build_iovecs() {
  iov_n_ = 0;
  iov_head_ = 0;
  for (uint32_t i = 0; i < pending_n_; ++i) {
    auto& p = pending_[i];
    if (iov_n_ + 3 > kMaxPendingIov) {
      // IOV_MAX and our own array both cap a batch; the rest goes in the next
      // writev rather than being dropped.
      ++metrics_.iov_batch_split;
      break;
    }
    if (p.head_len) {
      iov_[iov_n_++] = {head_out_.data() + p.head_off, p.head_len};
    }
    if (p.hdr_len) {
      iov_[iov_n_++] = {hdr_out_.data() + p.hdr_off, p.hdr_len};
    }
    if (p.body_len) {
      if (p.source == body_source_t::Inline) {
        iov_[iov_n_++] = {body_out_.data() + p.body_off, p.body_len};
      } else {
        iov_[iov_n_++] = {const_cast<char*>(p.external.data()), p.body_len};
      }
    }
  }
  return iov_n_;
}

void connection_t::advance_iovecs(uint32_t written) {
  while (written > 0 && iov_head_ < iov_n_) {
    auto& v = iov_[iov_head_];
    if (written >= v.iov_len) {
      written -= uint32_t(v.iov_len);
      ++iov_head_;
    } else {
      // Partial write: advance in place rather than rebuilding the list.
      v.iov_base = static_cast<char*>(v.iov_base) + written;
      v.iov_len -= written;
      written = 0;
    }
  }
}

coro_t<expected<void>> connection_t::flush() {
  if (pending_n_ == 0) co_return expected<void>{};

  if (head_out_.failed() || hdr_out_.failed() || body_out_.failed()) {
    co_return unexpected(head_out_.failed() ? head_out_.error()
                        : hdr_out_.failed() ? hdr_out_.error()
                                            : body_out_.error());
  }

  build_iovecs();
  uint32_t total = 0;
  for (uint32_t i = 0; i < iov_n_; ++i) total += uint32_t(iov_[i].iov_len);
  CORNET_HTTP_TRACE_LOG("fd={}: flush {} response(s), {} iovec(s), {} bytes",
                        sock_.native_fd(), pending_n_, iov_n_, total);
  while (iov_head_ < iov_n_) {
    ++metrics_.writev_calls;
    auto n = co_await with_cancel(
        ctx_, sock_.writev(ctx_, iov_ + iov_head_, iov_n_ - iov_head_), canceler_);
    if (!n) {
      CORNET_HTTP_TRACE_LOG("fd={}: writev failed ({})", sock_.native_fd(), n.error().message());
      co_return unexpected(n.error());
    }
    CORNET_HTTP_TRACE_LOG("fd={}: writev wrote {} bytes", sock_.native_fd(), *n);
    if (*n == 0) co_return unexpected(ECONNRESET);
    auto before = iov_head_;
    advance_iovecs(uint32_t(*n));
    if (iov_head_ == before) ++metrics_.writev_partial;
  }
  co_return expected<void>{};
}

void connection_t::reset_round() {
  for (uint32_t i = 0; i < pending_n_; ++i) {
    pending_[i].owned.release();
    pending_[i] = pending_t{};
  }
  pending_n_ = 0;
  head_out_.clear();
  hdr_out_.clear();
  body_out_.clear();
  iov_n_ = 0;
  iov_head_ = 0;
}

// ──────────────────────────── shutdown ────────────────────────────

coro_t<void> connection_t::shutdown_gracefully() {
  // Half-close, then read briefly. Closing outright can make the peer see an RST
  // and discard a response we already wrote.
  CORNET_HTTP_TRACE_LOG("fd={}: half-close then drain", sock_.native_fd());
  auto sd = co_await as_system(async_shutdown(ctx_, sock_.native_fd(), SHUT_WR));
  (void)sd;

  char scratch[512];
  wheel_.arm(timer_, std::chrono::milliseconds(200));
  for (int i = 0; i < 4; ++i) {
    auto n = co_await with_cancel(ctx_, sock_.recv(ctx_, scratch, sizeof(scratch)), canceler_);
    if (!n || *n == 0) break;
  }
  wheel_.cancel(timer_);
  co_return;
}

// ──────────────────────────── the loop ────────────────────────────

coro_t<void> connection_t::run(const router_t& router) {
  router_ = &router;

  if (auto ok = attach_buffers(); !ok) {
    SPDLOG_WARN("http: connection rejected, no buffers: {}", ok.error().message());
    async_close(ctx_, sock_.release());
    co_return;
  }
  CORNET_HTTP_TRACE_LOG("fd={}: run start, header buffer {}B",
                        sock_.native_fd(), in_.capacity());

  wheel_.arm(timer_, opt_.idle_timeout);
  bool first_read = true;
  uint32_t pipelined = 0;

  while (!closing_ && !timed_out_) {
    // ── 1. read. No link_timeout: the wheel owns the deadline, so this costs one
    //        SQE, not two.
    auto got = co_await fill();
    if (!got) {
      if (got.error().code == EAGAIN) continue;
      break;
    }
    if (*got == 0) {
      CORNET_HTTP_TRACE_LOG("fd={}: peer closed", sock_.native_fd());
      break;
    }
    first_read = false;
    wheel_.arm(timer_, opt_.header_timeout);

    auto r = parser_.execute(in_.write_pos() - *got, *got);
    CORNET_HTTP_TRACE_LOG("fd={}: parse -> {}", sock_.native_fd(), parser_t::to_string(r));

    // ── 2. parse and dispatch, draining every complete request in this read
    bool round_done = false;
    while (!round_done) {
      switch (r) {
        case parser_t::result_t::NeedMore:
          round_done = true;
          break;

        case parser_t::result_t::Error: {
          auto err = parser_.error();
          SPDLOG_DEBUG("http: protocol error: {}", err.message());
          write_error(status_for_error(err));
          round_done = true;
          break;
        }

        case parser_t::result_t::Upgrade:
          // No upgrade protocol is wired up yet; refuse rather than leave the
          // connection in a state neither side agrees on.
          write_error(status_t::NotImplemented);
          round_done = true;
          break;

        case parser_t::result_t::HeadersReady: {
          ++metrics_.requests;
          CORNET_HTTP_TRACE_LOG("fd={}: {} {} (cl={} chunked={} keep_alive={})",
                                sock_.native_fd(), method_name(parser_.method()),
                                parser_.target(), parser_.content_length(),
                                parser_.chunked(), parser_.keep_alive());
          req_.reset();
          resp_.begin();
          params_.clear();

          if (parser_.version() == version_t::Unknown) {
            write_error(status_t::HttpVersionNotSupported);
            round_done = true;
            break;
          }

          auto m = router.match(req_.method(), req_.path(), params_);
          CORNET_HTTP_TRACE_LOG("fd={}: route path='{}' matched={} method_mismatch={}",
                                sock_.native_fd(), req_.path(), m.route != nullptr,
                                m.method_mismatch);
          route_ = m.route;
          if (!route_) {
            if (m.method_mismatch) {
              write_error(status_t::MethodNotAllowed);
              round_done = true;
              break;
            }
            route_ = router.fallback_route();
          }

          if (auto ok = prepare_body(route_); !ok) {
            write_error(status_for_error(ok.error()));
            round_done = true;
            break;
          }
          CORNET_HTTP_TRACE_LOG("fd={}: body mode={} streaming={}", sock_.native_fd(),
                                int(body_mode_), streaming_);

          if (parser_.expects_continue()) {
            // Answered before any body byte is read, which is the only point at
            // which it means anything.
            write_continue();
          }

          if (streaming_) {
            // The handler consumes the body itself, so it must run now.
            reader_ = body_reader_t(*this);
            req_.set_reader(&reader_);
            if (route_) {
              bool proceed = true;
              if (router.has_filters()) proceed = co_await run_filters(router);
              if (proceed) co_await invoke(*route_);
            } else {
              resp_.not_found();
            }
            // A connection cannot be reused until the body is consumed.
            if (!reader_.complete()) {
              auto drained = co_await reader_.drain();
              if (!drained) {
                write_error(status_for_error(drained.error()));
                round_done = true;
                break;
              }
            }
            bool close_after = !parser_.keep_alive() || closing_ ||
                               status_forces_close(resp_.status());
            if (resp_.failed()) {
              SPDLOG_ERROR("http: response failed: {}", resp_.error().message());
              write_error(status_t::InternalServerError);
              round_done = true;
              break;
            }
            frame_response(close_after);
            if (close_after) {
              close_after_flush_ = true;
              round_done = true;
              break;
            }
            parser_.reset();
            if (++pipelined >= opt_.max_pipelined) {
              round_done = true;
              break;
            }
            r = parser_.has_pending_input() ? parser_.resume() : parser_t::result_t::NeedMore;
            break;
          }

          // Aggregating: keep feeding until the body is complete.
          r = parser_.resume();
          break;
        }

        case parser_t::result_t::BodyPaused:
          // Only reachable with no body sink, i.e. streaming, which is handled
          // above. Treat it as a bug rather than spinning.
          SPDLOG_ERROR("http: unexpected BodyPaused in aggregate mode");
          write_error(status_t::InternalServerError);
          round_done = true;
          break;

        case parser_t::result_t::MessageReady: {
          req_.set_body(body_.view());
          body_complete_ = true;
          CORNET_HTTP_TRACE_LOG("fd={}: dispatch (body={}B, {})", sock_.native_fd(),
                                body_.size(),
                                route_ ? (route_->kind == route_t::kind_t::Sync ? "sync" : "async")
                                       : "no route");

          if (route_) {
            bool proceed = true;
            if (router.has_filters()) proceed = co_await run_filters(router);
            if (proceed) co_await invoke(*route_);
          } else {
            resp_.not_found();
          }

          if (resp_.failed()) {
            SPDLOG_ERROR("http: response failed: {}", resp_.error().message());
            write_error(status_t::InternalServerError);
            round_done = true;
            break;
          }

          bool close_after = !parser_.keep_alive() || closing_ ||
                             status_forces_close(resp_.status());
          frame_response(close_after);
          if (close_after) {
            close_after_flush_ = true;
            round_done = true;
            break;
          }

          parser_.reset();
          body_.release();
          if (++pipelined >= opt_.max_pipelined) {
            round_done = true;
            break;
          }
          // Anything still buffered is the next pipelined request.
          r = parser_.has_pending_input() ? parser_.resume() : parser_t::result_t::NeedMore;
          CORNET_HTTP_TRACE_LOG("fd={}: after response -> {} (pipelined={})",
                                sock_.native_fd(), parser_t::to_string(r), pipelined);
          break;
        }
      }
    }

    // ── 3. write everything this round produced in one gather-write
    if (pending_n_ > 1) ++metrics_.pipelined_batches;
    if (auto ok = co_await flush(); !ok) {
      CORNET_HTTP_TRACE_LOG("fd={}: flush failed ({})", sock_.native_fd(), ok.error().message());
      SPDLOG_DEBUG("http: write failed: {}", ok.error().message());
      reset_round();
      break;
    }
    reset_round();
    if (close_after_flush_) {
      CORNET_HTTP_TRACE_LOG("fd={}: closing after flush", sock_.native_fd());
      break;
    }

    // ── 4. reclaim the header buffer. Safe only between messages, which is
    //        exactly where we are.
    if (!parser_.has_pending_input()) {
      in_.consume(in_.readable());
      in_.compact();
    }
    pipelined = 0;
    wheel_.arm(timer_, opt_.idle_timeout);
  }

  (void)first_read;
  if (spill_.used() > 0) ++metrics_.spill_used;

  CORNET_HTTP_TRACE_LOG("fd={}: loop exit (closing={} timed_out={} requests={})",
                        sock_.native_fd(), closing_, timed_out_, metrics_.requests);

  wheel_.cancel(timer_);
  if (!timed_out_) {
    co_await shutdown_gracefully();
  }
  release_buffers();
  // release() first: without it the socket destructor would close the same fd a
  // second time, and by then the number may belong to another connection.
  async_close(ctx_, sock_.release());
  co_return;
}

} // namespace cornet::http
