#include "cornet/http/server/connection.h"

#include <netinet/tcp.h>
#include <sys/socket.h>

#include <spdlog/spdlog.h>

#include "cornet/concurrency/combinators.h"
#include "cornet/http/common/trace.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"
#include "cornet/websocket/common/handshake.h"
#include "cornet/websocket/server.h"

namespace cornet::http {

namespace {

// Pre-composed constant header lines: framework-originated values contain no
// CR/LF by construction, so the scan serializer_t::header() runs on every
// emission is provably dead work here. Emitting them as one literal shortens
// the per-request hot path to single put() calls.
constexpr std::string_view kHdrServer = "Server: cornet\r\n";
constexpr std::string_view kHdrConnKeepAlive = "Connection: keep-alive\r\n";
constexpr std::string_view kHdrConnClose = "Connection: close\r\n";
// Common-case bundle: both Server and Connection headers in one put. Placed
// adjacent in the literal so a single memcpy emits both lines.
constexpr std::string_view kHdrServerKeepAlive =
    "Server: cornet\r\nConnection: keep-alive\r\n";
constexpr std::string_view kHdrServerClose =
    "Server: cornet\r\nConnection: close\r\n";

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
  max_trailers = at("cornet.http.server.max_trailers").value_or(max_trailers);
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
  duration("cornet.http.server.handshake_timeout", handshake_timeout);

  ws.load(config, "cornet.http.server.ws");
}

// ──────────────────────────── connection_t ────────────────────────────

connection_t::connection_t(context_t& ctx, tls::transport_t transport,
                           const server_options_t& opt,
                           buffer_pool_t& pool, timer_wheel_t& wheel,
                           connection_metrics_t& metrics)
  : ctx_(ctx), tr_(std::move(transport)), opt_(opt), pool_(pool), wheel_(wheel),
    metrics_(metrics), read_canceler_(ctx), drain_canceler_(ctx),
    parser_(parser_t::type_t::Request), reader_(*this) {
  if (opt_.tcp_nodelay) {
    int on = 1;
    ::setsockopt(tr_.native_fd(), IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  }
  parser_.bind(in_, spill_, headers_);
  parser_.set_limits(parser_limits_t{
    .max_headers = opt_.max_headers,
    .max_trailers = opt_.max_trailers,
    .max_body_bytes = opt_.max_body_bytes,
    .lenient_headers = opt_.lenient_headers,
    .lenient_chunked_length = opt_.lenient_chunked_length,
    .lenient_keep_alive = opt_.lenient_keep_alive,
  });
  req_.bind(parser_, headers_, params_);
  resp_.bind(hdr_out_, body_out_);
  resp_.bind(*this);

  timer_.owner = this;
  timer_.on_expire = [](void* owner) {
    auto* self = static_cast<connection_t*>(owner);
    self->timed_out_ = true;
    ++self->metrics_.timeouts;
    // A read deadline interrupts reads only. Writes are deliberately out of
    // scope: cancelling a writev that is already mid-response truncates the
    // reply on the wire, and latching the canceler would silently drop every
    // response queued after it.
    self->read_canceler_.cancel();
    self->drain_canceler_.cancel();
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

  auto so = pool_.acquire(opt_.body_buffer_bytes);  // reuse same size for streaming
  if (!so) return unexpected(ENOMEM);
  stream_out_.reset(std::move(so));

  return {};
}

void connection_t::release_buffers() {
  in_.release();
  head_out_.release();
  hdr_out_.release();
  body_out_.release();
  stream_out_.release();
  body_.release();
  if (splice_pipe_[0] >= 0) {
    ::close(splice_pipe_[0]);
    ::close(splice_pipe_[1]);
    splice_pipe_[0] = splice_pipe_[1] = -1;
  }
}

void connection_t::request_close() {
  closing_ = true;
  // Only the read side is cancelled; anything already queued for write still
  // goes out, which is what keeps a graceful drain from truncating responses.
  // drain_canceler_ is left alone so shutdown_gracefully() keeps its window.
  read_canceler_.cancel();
  // An upgraded connection parks in the session's recv, not ours: the drain
  // reaches it the same way, and the session winds down with GoingAway.
  if (ws_active_) ws_active_->request_close();
}

// ─────────────────────────────── reading ───────────────────────────────

coro_t<expected<uint32_t>> connection_t::fill() {
  auto w = in_.writable();
  if (w.empty()) {
    // The header section filled the whole buffer without completing.
    CORNET_HTTP_TRACE_LOG("fd={}: header buffer full ({}B), answering 431",
                          tr_.native_fd(), in_.capacity());
    co_return http_unexpected(http_error_t::HeaderTooLarge);
  }
  CORNET_HTTP_TRACE_LOG("fd={}: recv armed, window={}", tr_.native_fd(), w.size());
  // Plain TCP takes the socket's leaf awaiter directly — no transport
  // coroutine frame per read. TLS goes through transport_t::recv (the record
  // layer's pump genuinely needs a coroutine). is_tls() is stable after
  // connection setup, so this branch predicts perfectly.
  expected<size_t> n;
  if (!tr_.is_tls()) {
    auto r = co_await with_cancel(ctx_, tr_.plain_recv(ctx_, w.data(), w.size()), read_canceler_);
    n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
  } else {
    n = co_await with_cancel(ctx_, tr_.recv(ctx_, w.data(), w.size()), read_canceler_);
  }
  if (!n) {
    CORNET_HTTP_TRACE_LOG("fd={}: recv failed ({}), timed_out={}",
                          tr_.native_fd(), n.error().message(), timed_out_);
    if (timed_out_) co_return unexpected(ETIMEDOUT);
    co_return unexpected(n.error());
  }
  CORNET_HTTP_TRACE_LOG("fd={}: recv {} bytes", tr_.native_fd(), *n);
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
        // Nothing buffered: the rest of the body is still on the wire. Reuse the region
        // behind the header section rather than asking for more room — body bytes are
        // handed to the reader as they arrive, so nothing still referenced lives there,
        // while every header offset stays below the mark and is untouched.
        CORNET_ASSERT(!parser_.mid_header(),
                      "rewinding under a half-accumulated header would splice its bytes");
        in_.rewind_to(body_window_);
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

// Runs a filter chain that is known to be sync-only straight through, without
// a run_filters() coroutine frame. Callers must check has_async_filters().
static bool run_sync_filters(const router_t& router, request_t& req, response_t& resp) {
  for (const auto& f : router.filters()) {
    if (!f.sync_fn(req, resp)) return false;
  }
  return true;
}

coro_t<void> connection_t::invoke(const route_t& route) {
  // A synchronous handler is called directly: no frame, no suspension, no trip
  // through the scheduler. That is the whole reason the two kinds are separate.
  // (Both dispatch sites in run() branch on kind first, so this path only runs
  // for async handlers and pays for a frame only when one can actually suspend.)
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

// ──────────────────────── streaming write ────────────────────────

void connection_t::frame_head(bool close_after) {
  resp_.seal_headers();

  auto status = resp_.status();

  serializer_t::status_line(head_out_, status);

  if (opt_.serve_date_header && !resp_.saw_date()) {
    serializer_t::date_header(head_out_, ctx_.http_date(), ctx_.http_date_len());
  }
  // Static-block fast path: Server and Connection are framework constants, so
  // emit them as pre-composed literals — no per-value CR/LF scan, and when
  // both are present a single put() copies the two lines together.
  const bool emit_server = opt_.serve_server_header;
  const bool emit_conn = !resp_.saw_connection();
  if (emit_server && emit_conn) {
    head_out_.put(close_after ? kHdrServerClose : kHdrServerKeepAlive);
  } else if (emit_server) {
    head_out_.put(kHdrServer);
  } else if (emit_conn) {
    head_out_.put(close_after ? kHdrConnClose : kHdrConnKeepAlive);
  }
  // RFC 9110 §8.6: Content-Length in a 1xx/204/304 response is a malformed
  // message — the peer would wait for a body that never comes and the next
  // pipelined exchange slides out of sync. HEAD is not affected: forbids-body
  // is false for its 200, and Content-Length there is required semantics.
  if (!resp_.saw_content_length() && !resp_.saw_transfer_encoding() &&
      resp_.body_source() != body_source_t::Streaming &&
      !status_forbids_body(status)) {
    serializer_t::header_u64(head_out_, field_t::ContentLength, resp_.body_length());
  }
  if (resp_.body_source() == body_source_t::Streaming &&
      !resp_.saw_transfer_encoding()) {
    serializer_t::header(head_out_, field_t::TransferEncoding, "chunked");
  }
}

void connection_t::stage_headers() {
  // Frame status + headers.  The actual socket write is deferred to the first
  // flush_stream() call, which sends head + hdr + the first body chunk in a
  // single writev.  This keeps chunked() non-coroutine while ensuring headers
  // and the first chunk leave the kernel in one syscall.
  //
  // close_after is not known yet — the handler has not returned, so neither has the
  // decision that depends on its status — so the header says keep-alive. When the
  // request asked for close the connection still closes after the last chunk; only
  // that one header line disagrees.
  frame_head(false);
  headers_staged_ = true;
}

CORNET_NODISCARD coro_t<expected<void>> connection_t::flush_stream() {
  if (stream_out_.failed()) {
    co_return unexpected(stream_out_.error());
  }
  if (stream_out_.size() == 0) co_return expected<void>{};

  // On the first flush after stage_headers(), send head + hdr + chunk-data
  // in a single writev so headers and the first body chunk leave the kernel
  // in one syscall.  Subsequent flushes only send the chunk-data.
  bool first_flush = headers_staged_;
  if (first_flush) headers_staged_ = false;

  uint32_t total_len = stream_out_.size();
  if (first_flush) {
    total_len += head_out_.size() + resp_.hdr_length();
  }

  // 3 iovec segments max: head + hdr + chunk-data
  struct iovec iov[3];
  uint32_t iov_n = 0;
  if (first_flush && head_out_.size()) {
    iov[iov_n++] = {head_out_.data(), head_out_.size()};
  }
  if (first_flush && resp_.hdr_length()) {
    iov[iov_n++] = {hdr_out_.data(), resp_.hdr_length()};
  }
  if (stream_out_.size()) {
    iov[iov_n++] = {stream_out_.data(), stream_out_.size()};
  }

  uint32_t written_total = 0;
  while (written_total < total_len) {
    ++metrics_.writev_calls;
    // no cancellation on the write path: an interrupted writev is a truncated
    // response (see the comment on the cancelers in connection.h). Plain TCP
    // skips the transport coroutine frame; TLS keeps it (record layer pump).
    expected<size_t> n;
    if (!tr_.is_tls()) {
      auto r = co_await tr_.plain_writev(ctx_, iov, iov_n);
      n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      n = co_await tr_.writev(ctx_, iov, iov_n);
    }
    if (!n) {
      CORNET_HTTP_TRACE_LOG("fd={}: writev failed ({})", tr_.native_fd(), n.error().message());
      co_return unexpected(n.error());
    }
    CORNET_HTTP_TRACE_LOG("fd={}: writev wrote {} bytes", tr_.native_fd(), *n);
    if (*n == 0) co_return unexpected(ECONNRESET);

    // Advance through iovecs until we've accounted for *n bytes
    uint32_t remaining = uint32_t(*n);
    for (uint32_t i = 0; i < iov_n && remaining > 0; ++i) {
      auto& v = iov[i];
      uint32_t take = remaining < v.iov_len ? remaining : v.iov_len;
      v.iov_base = static_cast<char*>(v.iov_base) + take;
      v.iov_len -= take;
      remaining -= take;
    }
    written_total += uint32_t(*n);
  }

  stream_out_.clear();
  co_return expected<void>{};
}

coro_t<expected<void>> connection_t::write_direct(std::string_view data) {
  const char* p = data.data();
  size_t left = data.size();
  while (left > 0) {
    struct iovec iov{const_cast<char*>(p), left};
    ++metrics_.writev_calls;
    // no cancellation on the write path, same rationale as flush_stream()
    expected<size_t> n;
    if (!tr_.is_tls()) {
      auto r = co_await tr_.plain_writev(ctx_, &iov, 1);
      n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      n = co_await tr_.writev(ctx_, &iov, 1);
    }
    if (!n) {
      CORNET_HTTP_TRACE_LOG("fd={}: direct write failed ({})", tr_.native_fd(),
                            n.error().message());
      co_return unexpected(n.error());
    }
    if (*n == 0) co_return unexpected(ECONNRESET);
    p += *n;
    left -= *n;
  }
  co_return expected<void>{};
}

// A streaming response whose route came home early cannot just hang in the
// connection contract: either it can still be swapped for a 500 (nothing on
// the wire yet) or it has to be truncated so the peer sees what happened.
void connection_t::settle_streaming(bool& close_after) {
  if (headers_staged_) {
    SPDLOG_DEBUG("http: route ended a streaming response before anything was "
                 "sent; answering 500");
    head_out_.clear();
    hdr_out_.clear();
    stream_out_.clear();
    write_error(status_t::InternalServerError);  // counts the protocol error
    close_after = true;
    return;
  }
  // A 500 would now lie about the status line, and "0\r\n\r\n" would lie about
  // a complete body. The honest answer is a truncation the client can detect.
  ++metrics_.protocol_errors;
  SPDLOG_DEBUG("http: route ended a streaming response mid-body; truncating");
  closing_ = true;
  close_after = true;
}

// ──────────────────────────── framing ────────────────────────────

void connection_t::frame_response(bool close_after) {
  // Streaming: headers were already framed by stage_headers() during
  // response_t::chunked(), so the body_writer_t flushes them incrementally
  // on its own.  No pending entry, no head_out_ append, no batch flush.
  if (resp_.body_source() == body_source_t::Streaming) {
    streaming_write_ = true;
    CORNET_HTTP_TRACE_LOG("fd={}: framed streaming response (chunked)",
                          tr_.native_fd());
    ++metrics_.responses;
    return;
  }

  auto status = resp_.status();
  bool head_request = req_.method() == method_t::Head;
  bool no_body_allowed = status_forbids_body(status);

  pending_t p{};
  p.head_off = head_out_.size();

  frame_head(close_after);

  p.head_len = head_out_.size() - p.head_off;
  p.hdr_off = resp_.hdr_offset();
  p.hdr_len = resp_.hdr_length();

  if (!no_body_allowed && !head_request && resp_.body_length() > 0) {
    p.source = resp_.body_source();
    p.body_len = uint32_t(resp_.body_length());
    if (p.source == body_source_t::Inline) {
      p.body_off = resp_.inline_body_offset();
    } else if (p.source == body_source_t::File) {
      // ownership moves to the pending slot: the fd closes after the body leaves
      p.file_fd = resp_.take_file_fd();
      p.file_pos = 0;
    } else {
      p.external = resp_.external_body();
      p.owned = resp_.take_owned();
      p.pinned = resp_.take_pinned();
    }
  } else if (resp_.file_fd() >= 0) {
    // bodyless answer on a file response (HEAD, 204/304): the descriptor must
    // not leak into the next request's arena
    resp_.close_file();
  }

  CORNET_HTTP_TRACE_LOG("fd={}: framed status={} head={}B hdr={}B body={}B close_after={}",
                        tr_.native_fd(), uint16_t(status), p.head_len, p.hdr_len,
                        p.body_len, close_after);
  if (pending_n_ < std::size(pending_)) {
    pending_[pending_n_++] = std::move(p);
  } else {
    head_out_.fail(http_error(http_error_t::OutputOverflow));
  }
  ++metrics_.responses;
}

// ──────────────────────────── protocol upgrade ────────────────────────────

coro_t<void> connection_t::run_websocket(const route_t& route, std::string_view key,
                                         std::string_view subprotocol) {
  // Responses to requests pipelined ahead of the upgrade were framed but not
  // written: they go out first — a peer that answered earlier requests must
  // not lose them because a later one happened to switch protocols.
  if (auto flushed = co_await flush(); !flushed) {
    // the main loop's failed-flush cleanup; pending slots can hold file fds
    reset_round();
    co_return;
  }
  reset_round();

  // The 101 travels in one gather-write, straight from staging. It bypasses
  // frame_head() deliberately: a 101 forbids Content-Length and every other
  // framing header that function would add (RFC 9110 §8.6).
  websocket::frame_handshake_response(head_out_, key, subprotocol);
  ++metrics_.responses;
  if (head_out_.failed()) co_return;
  struct iovec iov{head_out_.data(), head_out_.size()};
  auto sent = co_await writev_span(&iov, 1);
  head_out_.clear();
  if (!sent) co_return;

  // llhttp stopped at the end of the header section; anything already read
  // past it belongs to the websocket stream (RFC 6455 allows the client to
  // send frames immediately after its opening request).
  std::string_view leftover =
      in_.view(parser_.consumed_offset(), in_.write_pos() - parser_.consumed_offset());

  // The http buffers (header window, staging areas) are released before the
  // session starts: an upgrade can last hours, and a long-lived session must
  // not pin five pooled leases it will never parse another request with.
  websocket::session_t ws(ctx_, std::move(tr_), websocket::role_t::Server, pool_,
                          wheel_, opt_.ws, leftover, &metrics_);
  if (!subprotocol.empty()) ws.set_subprotocol(subprotocol);
  ws_active_ = &ws;
  CORNET_HTTP_TRACE_LOG("fd={}: upgraded to websocket (subprotocol='{}')",
                        ws.native_fd(), subprotocol);

  co_await route.ws_fn(ws);

  ws_active_ = nullptr;
  // Whatever close state the handler left: settle it, then the session
  // shuts down and abandons the transport itself.
  co_await ws.finish();
  release_buffers();
}

void connection_t::write_error(status_t status) {
  // The request in flight is being abandoned, so the receive buffer no longer holds a
  // message anybody will look at. Every error here closes the connection anyway, but
  // leaving the flag set would keep the buffer pinned for as long as it lived.
  in_body_ = false;
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
  //
  // It is queued, not sent: the round flushes once, after the handler has run, so
  // the interim response reaches the peer together with the real one. A client that
  // strictly waits for it therefore waits out its own timer and then sends the body
  // anyway — the request still completes, it just does not save the round trip.
  // Sending it here on its own would cost an extra write per such request, so the
  // handshake stays unimplemented rather than half-paid-for.
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

/**
 * @brief gather the buffer parts of pendings [begin, end).
 * File bodies are not here by construction of flush(); skipping them here is
 * belt and braces, not a second code path.
 */
uint32_t connection_t::build_iovecs(uint32_t begin, uint32_t end) {
  iov_n_ = 0;
  iov_head_ = 0;
  for (uint32_t i = begin; i < end; ++i) {
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
      } else if (p.source == body_source_t::External) {
        iov_[iov_n_++] = {const_cast<char*>(p.external.data()), p.body_len};
      }
      // Streaming flushes incrementally via body_writer_t; File via the
      // splice/stream path — neither puts bytes in iovecs.
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

// Write one assembled iovec span to the socket, handling short writes.
// Extracted verbatim from the old flush loop so pipelined responses and file
// bodies share exactly the same write discipline.
coro_t<expected<void>> connection_t::writev_span(struct iovec* iov, uint32_t n) {
  uint32_t done = 0;
  while (done < n) {
    ++metrics_.writev_calls;
    // no cancellation on the write path, same rationale as send_stream().
    // Plain TCP takes the frame-free socket awaiter; TLS keeps its pump.
    expected<size_t> w;
    if (!tr_.is_tls()) {
      auto r = co_await tr_.plain_writev(ctx_, iov + done, n - done);
      w = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      w = co_await tr_.writev(ctx_, iov + done, n - done);
    }
    if (!w) {
      CORNET_HTTP_TRACE_LOG("fd={}: writev failed ({})", tr_.native_fd(), w.error().message());
      co_return unexpected(w.error());
    }
    if (*w == 0) co_return unexpected(ECONNRESET);
    uint32_t remaining = uint32_t(*w);
    for (size_t i = done; i < n && remaining > 0; ++i) {
      uint32_t take = remaining < iov[i].iov_len ? remaining : iov[i].iov_len;
      iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + take;
      iov[i].iov_len -= take;
      remaining -= take;
    }
    while (done < n && iov[done].iov_len == 0) ++done;
    if (done < n) ++metrics_.writev_partial;
  }
  co_return expected<void>{};
}

bool connection_t::ensure_splice_pipe() {
  if (splice_pipe_[0] >= 0) return true;
  return ::pipe(splice_pipe_) == 0;
}

// file body on the plain transport: page-cache → pipe → socket, zero copies of
// the payload. pos tracks the read offset explicitly so a resumed flush never
// asks the file for bytes it already sent.
coro_t<expected<void>> connection_t::splice_file_to_socket(int fd, int64_t& pos,
                                                           uint64_t len) {
  if (!ensure_splice_pipe()) co_return unexpected(errno);
  static constexpr uint64_t kChunk = 64 * 1024;
  int out_fd = tr_.native_fd();
  while (len > 0) {
    uint32_t chunk = uint32_t(std::min<uint64_t>(len, kChunk));
    auto n = co_await splice_awaiter(ctx_, fd, pos, splice_pipe_[1], -1, chunk,
                                     SPLICE_F_MOVE);
    if (!n) co_return unexpected(n.error());
    if (*n == 0) co_return unexpected(ECONNRESET);  // the file shrank under us
    pos += int64_t(*n);
    uint32_t left = uint32_t(*n);
    len -= left;
    while (left > 0) {
      auto w = co_await splice_awaiter(ctx_, splice_pipe_[0], -1, out_fd, -1, left,
                                       SPLICE_F_MOVE);
      if (!w) co_return unexpected(w.error());
      if (*w == 0) co_return unexpected(ECONNRESET);
      left -= uint32_t(*w);
    }
  }
  co_return {};
}

// file body under TLS: the record layer is userspace, so the bytes have to
// come up through a buffer once — a sync pread is the right bufferer. Page-
// cache reads are microseconds; an executor offload for cold-page workloads
// is a deliberate future knob, not a default.
coro_t<expected<void>> connection_t::stream_file_over_tls(int fd, int64_t& pos,
                                                          uint64_t len) {
  static constexpr size_t kBuf = 64 * 1024;
  if (!file_scratch_) file_scratch_ = std::make_unique<char[]>(kBuf);
  while (len > 0) {
    size_t want = size_t(std::min<uint64_t>(len, kBuf));
    ssize_t n = ::pread(fd, file_scratch_.get(), want, off_t(pos));
    if (n < 0) co_return unexpected(errno);
    if (n == 0) co_return unexpected(EIO);
    pos += int64_t(n);
    len -= uint64_t(n);
    struct iovec iov{file_scratch_.get(), size_t(n)};
    while (iov.iov_len > 0) {
      auto w = co_await tr_.writev(ctx_, &iov, 1);
      if (!w) co_return unexpected(w.error());
      if (*w == 0) co_return unexpected(ECONNRESET);
      iov = {static_cast<char*>(iov.iov_base) + *w, iov.iov_len - *w};
    }
  }
  co_return {};
}

coro_t<expected<void>> connection_t::flush() {
  if (pending_n_ == 0) co_return expected<void>{};

  if (head_out_.failed() || hdr_out_.failed() || body_out_.failed()) {
    co_return unexpected(head_out_.failed() ? head_out_.error()
                        : hdr_out_.failed() ? hdr_out_.error()
                                            : body_out_.error());
  }

  // fast path: nothing file-backed, one batch for everything
  uint32_t first_file = 0;
  while (first_file < pending_n_ && pending_[first_file].source != body_source_t::File) {
    ++first_file;
  }

  if (first_file == pending_n_) {
    build_iovecs(0, pending_n_);
    if (auto ok = co_await writev_span(iov_, iov_n_); !ok) co_return unexpected(ok.error());
    co_return {};
  }

  // slow path: preserve wire order around each File body
  uint32_t i = 0;
  while (i < pending_n_) {
    uint32_t j = i;
    while (j < pending_n_ && pending_[j].source != body_source_t::File) ++j;
    if (j > i) {
      build_iovecs(i, j);
      if (auto ok = co_await writev_span(iov_, iov_n_); !ok) co_return unexpected(ok.error());
      i = j;
    }
    if (i < pending_n_) {
      auto& p = pending_[i];
      // this response's head+hdr still come from the staging buffers
      struct iovec hv[2];
      uint32_t hn = 0;
      if (p.head_len) hv[hn++] = {head_out_.data() + p.head_off, p.head_len};
      if (p.hdr_len) hv[hn++] = {hdr_out_.data() + p.hdr_off, p.hdr_len};
      if (hn) {
        auto ok = co_await writev_span(hv, hn);
        if (!ok) co_return unexpected(ok.error());
      }

      expected<void> ok;
      if (tr_.is_tls()) {
        ok = co_await stream_file_over_tls(p.file_fd, p.file_pos, p.body_len);
      } else {
        ok = co_await splice_file_to_socket(p.file_fd, p.file_pos, p.body_len);
      }
      ::close(p.file_fd);
      p.file_fd = -1;
      p.source = body_source_t::None;
      if (!ok) co_return unexpected(ok.error());
      ++i;
    }
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
  stream_out_.clear();
  // body_ is still needed when mid-body (NeedMore path); release only after
  // the entire body has been consumed and the handler finished.
  if (!in_body_) body_.release();
  streaming_write_ = false;
  iov_n_ = 0;
  iov_head_ = 0;
  // A route that bailed mid-stream leaves these set; the next response must
  // not inherit them (its own first flush would look like a stream's first).
  headers_staged_ = false;
  stream_finished_ = false;
}

// ──────────────────────────── shutdown ────────────────────────────

coro_t<void> connection_t::shutdown_gracefully() {
  // Half-close, then read briefly. Closing outright can make the peer see an RST
  // and discard a response we already wrote.
  CORNET_HTTP_TRACE_LOG("fd={}: half-close then drain", tr_.native_fd());
  auto sd = co_await tr_.shutdown_write(ctx_);
  (void)sd;

  char scratch[512];
  wheel_.arm(timer_, std::chrono::milliseconds(200));
  for (int i = 0; i < 4; ++i) {
    expected<size_t> n;
    if (!tr_.is_tls()) {
      auto r = co_await with_cancel(ctx_, tr_.plain_recv(ctx_, scratch, sizeof(scratch)),
                                    drain_canceler_);
      n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      n = co_await with_cancel(ctx_, tr_.recv(ctx_, scratch, sizeof(scratch)),
                               drain_canceler_);
    }
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
    tr_.abandon(ctx_);
    co_return;
  }
  CORNET_HTTP_TRACE_LOG("fd={}: run start, header buffer {}B",
                        tr_.native_fd(), in_.capacity());

  wheel_.arm(timer_, opt_.idle_timeout);
  bool first_read = true;
  uint32_t pipelined = 0;

  while (!closing_ && !timed_out_) {
    // ── 1. read (or reuse leftover pipelined bytes) and (re)start parsing.
    if (in_body_ && !parser_.has_pending_input()) {
      // Mid-body: the header section stays where it is and the tail region is reused.
      CORNET_ASSERT(!parser_.mid_header(),
                    "rewinding under a half-accumulated header would splice its bytes");
      in_.rewind_to(body_window_);
    }
    parser_t::result_t r;
    if (parser_.has_pending_input()) {
      // A previous round stopped at the pipelining cap with requests already
      // read but not yet parsed. Feeding only newly-read bytes here would skip
      // them forever (execute() resets the feed window), and waiting on a read
      // first would deadlock against a peer that is waiting for our responses.
      r = parser_.resume();
    } else {
      // No link_timeout: the wheel owns the deadline, so this costs one
      // SQE, not two.
      auto got = co_await fill();
      if (!got) {
        if (got.error().code == EAGAIN) continue;
        if (got.error().domain == error_domain::Http) {
          // RFC 9110 §8.4: a header section that never fits still deserves an
          // answer (431 via status_for_error), not a silent close.
          write_error(status_for_error(got.error()));
          if (auto flush_ok = co_await flush(); !flush_ok) {
            reset_round();
          }
        }
        break;
      }
      if (*got == 0) {
        CORNET_HTTP_TRACE_LOG("fd={}: peer closed", tr_.native_fd());
        break;
      }
      first_read = false;
      wheel_.arm(timer_, in_body_ ? opt_.body_timeout : opt_.header_timeout);

      r = parser_.execute(in_.write_pos() - *got, *got);
    }
    CORNET_HTTP_TRACE_LOG("fd={}: parse -> {}", tr_.native_fd(), parser_t::to_string(r));

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

        case parser_t::result_t::Upgrade: {
          if (!route_ || route_->kind != route_t::kind_t::WebSocket) {
            // No upgrade protocol is wired up for this route; refuse rather
            // than leave the connection in a state neither side agrees on.
            write_error(status_t::NotImplemented);
            round_done = true;
            break;
          }

          // RFC 6455 §4.2.1, gate by gate. The route matched at HeadersReady,
          // so req_ is populated; llhttp has vetted the HTTP half already.
          auto key = websocket::validate_request(req_);
          if (!key) {
            ++metrics_.protocol_errors;
            write_error(status_t::BadRequest);
            round_done = true;
            break;
          }

          if (router.has_filters()) {
            bool proceed = router.has_async_filters()
                               ? co_await run_filters(router)
                               : run_sync_filters(router, req_, resp_);
            if (!proceed) {
              frame_response(true);
              close_after_flush_ = true;
              round_done = true;
              break;
            }
          }

          std::string_view subprotocol;
          if (route_->ws_accept) {
            websocket::accept_info_t accept{};
            if (!route_->ws_accept(req_, accept)) {
              write_error(accept.refuse_with);
              round_done = true;
              break;
            }
            subprotocol = accept.subprotocol;
          }

          // The session takes over the whole connection; run()'s tail only
          // has bookkeeping left to do. close_after_flush_ merely exits the
          // keep-alive loop — the flush under it finds nothing staged.
          co_await run_websocket(*route_, *key, subprotocol);
          upgraded_ = true;
          close_after_flush_ = true;
          round_done = true;
          break;
        }

        case parser_t::result_t::HeadersReady: {
          ++metrics_.requests;
          CORNET_HTTP_TRACE_LOG("fd={}: {} {} (cl={} chunked={} keep_alive={})",
                                tr_.native_fd(), method_name(parser_.method()),
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
                                tr_.native_fd(), req_.path(), m.route != nullptr,
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

          // A plain request on a websocket endpoint: it had its chance to ask
          // for the upgrade. Without this branch, a non-upgrade GET would
          // reach the dispatch sites as kind WebSocket — which holds no http
          // handler — and call an empty std::function.
          if (route_ && route_->kind == route_t::kind_t::WebSocket &&
              !parser_.upgrade()) {
            write_error(status_t::UpgradeRequired);
            round_done = true;
            break;
          }

          if (auto ok = prepare_body(route_); !ok) {
            write_error(status_for_error(ok.error()));
            round_done = true;
            break;
          }
          // Everything up to here has been consumed, so this is where the body starts.
          body_window_ = parser_.consumed_offset();
          in_body_ = !body_complete_;
          CORNET_HTTP_TRACE_LOG("fd={}: body mode={} streaming={} window={}",
                                tr_.native_fd(), int(body_mode_), streaming_,
                                body_window_);

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
              if (router.has_filters()) {
                proceed = router.has_async_filters()
                              ? co_await run_filters(router)
                              : run_sync_filters(router, req_, resp_);
              }
              if (proceed) {
                if (route_->kind == route_t::kind_t::Sync) {
                  route_->sync_fn(req_, resp_);
                } else {
                  co_await invoke(*route_);
                }
              }
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
            in_body_ = false;
            bool close_after = !parser_.keep_alive() || closing_ ||
                               status_forces_close(resp_.status());
            if (resp_.failed()) {
              SPDLOG_ERROR("http: response failed: {}", resp_.error().message());
              write_error(status_t::InternalServerError);
              round_done = true;
              break;
            }
            if (needs_streaming_settlement()) {
              // settle frames its own 500 on the swap path, so skip frame_response
              settle_streaming(close_after);
              close_after_flush_ = true;
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
          in_body_ = false;
          CORNET_HTTP_TRACE_LOG("fd={}: dispatch (body={}B, {})", tr_.native_fd(),
                                body_.size(),
                                route_ ? (route_->kind == route_t::kind_t::Sync ? "sync" : "async")
                                       : "no route");

          if (route_) {
            bool proceed = true;
            if (router.has_filters()) {
              proceed = router.has_async_filters()
                            ? co_await run_filters(router)
                            : run_sync_filters(router, req_, resp_);
            }
            if (proceed) {
              if (route_->kind == route_t::kind_t::Sync) {
                route_->sync_fn(req_, resp_);
              } else {
                co_await invoke(*route_);
              }
            }
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
          if (needs_streaming_settlement()) {
            // settle frames its own 500 on the swap path, so skip frame_response
            settle_streaming(close_after);
            close_after_flush_ = true;
            round_done = true;
            break;
          }
          frame_response(close_after);
          if (close_after) {
            close_after_flush_ = true;
            round_done = true;
            break;
          }

          // body_ is no longer needed once the handler has consumed the body;
          // release it here so it returns to the pool before the next request.
          body_.release();
          parser_.reset();
          if (++pipelined >= opt_.max_pipelined) {
            round_done = true;
            break;
          }
          // Anything still buffered is the next pipelined request.
          r = parser_.has_pending_input() ? parser_.resume() : parser_t::result_t::NeedMore;
          CORNET_HTTP_TRACE_LOG("fd={}: after response -> {} (pipelined={})",
                                tr_.native_fd(), parser_t::to_string(r), pipelined);
          break;
        }
      }
    }

    // ── 3. write everything this round produced in one gather-write
    if (pending_n_ > 1) ++metrics_.pipelined_batches;
    if (auto ok = co_await flush(); !ok) {
      CORNET_HTTP_TRACE_LOG("fd={}: flush failed ({})", tr_.native_fd(), ok.error().message());
      SPDLOG_DEBUG("http: write failed: {}", ok.error().message());
      reset_round();
      break;
    }
    reset_round();
    if (close_after_flush_) {
      CORNET_HTTP_TRACE_LOG("fd={}: closing after flush", tr_.native_fd());
      break;
    }

    // ── 4. reclaim the header buffer. Only legal between messages: compaction moves
    //        bytes, and every header view of a message still being parsed is an offset
    //        into them. in_body_ is exactly that condition — a round can end mid-body
    //        (the body simply has not all arrived yet), and reclaiming here would
    //        clobber the headers of the request in flight.
    if (!in_body_ && !parser_.has_pending_input()) {
      in_.consume(in_.readable());
      in_.compact();
    }
    pipelined = 0;
    wheel_.arm(timer_, opt_.idle_timeout);
  }

  (void)first_read;
  if (spill_.used() > 0) ++metrics_.spill_used;

  CORNET_HTTP_TRACE_LOG("fd={}: loop exit (closing={} timed_out={} requests={})",
                        tr_.native_fd(), closing_, timed_out_, metrics_.requests);

  wheel_.cancel(timer_);
  if (!timed_out_ && !upgraded_) {
    // An upgraded connection had its close ceremony inside the session
    // already; a second half-close would be protocol noise.
    co_await shutdown_gracefully();
  }
  if (!upgraded_) {
    release_buffers();
    // release() first: without it the socket destructor would close the same fd a
    // second time, and by then the number may belong to another connection.
    tr_.abandon(ctx_);
  }
  co_return;
}

} // namespace cornet::http
