#ifndef CORNET_HTTP_SERVER_CONNECTION_H
#define CORNET_HTTP_SERVER_CONNECTION_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/uio.h>

#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/coro.h"
#include "cornet/http/common/buffer.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/headers.h"
#include "cornet/http/server/message.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/server/router.h"
#include "cornet/http/common/serializer.h"
#include "cornet/concurrency/deadline.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/tls/transport.h"
#include "cornet/websocket/session.h"

namespace cornet::http {

/**
 * @brief server tunables. Defaults are deliberately conservative.
 */
struct server_options_t {
  uint16_t    port{8080};
  std::string address{"0.0.0.0"};

  // header buffer capacity; a header section that does not fit is a 431
  uint32_t max_header_bytes{16u << 10};
  uint16_t max_headers{64};
  // chunked trailers to record; 0 drops them, which is the default. See
  // parser_limits_t::max_trailers for why.
  uint16_t max_trailers{0};
  uint64_t max_body_bytes{8ull << 20};
  // bodies larger than this are streamed rather than aggregated
  uint32_t aggregate_threshold{256u << 10};
  // window size for streaming reads
  uint32_t stream_window_bytes{32u << 10};
  // how many pipelined requests to answer before flushing (back pressure)
  uint16_t max_pipelined{32};
  uint32_t max_connections{100000};
  // output staging capacities
  uint32_t head_buffer_bytes{4u << 10};
  uint32_t hdr_buffer_bytes{8u << 10};
  uint32_t body_buffer_bytes{16u << 10};

  std::chrono::milliseconds idle_timeout{60000};
  std::chrono::milliseconds header_timeout{10000};
  std::chrono::milliseconds body_timeout{30000};
  std::chrono::milliseconds timer_tick{500};
  // Loaded, but nothing consults it yet: drain() waits for every connection to
  // finish on its own. A hard bound is the caller's to impose (stop(), or a
  // with_timeout around serve()) until this is wired up.
  std::chrono::milliseconds drain_timeout{10000};

  // TLS: set and every accepted connection handshakes before HTTP speaks a
  // word. Built programmatically (tls::tls_context_t::make_server) — private
  // key paths are deliberately not loadable from the config file; secrets do
  // not belong in TOML. See docs/tls.md.
  std::shared_ptr<tls::tls_context_t> tls;
  std::chrono::milliseconds handshake_timeout{10000};

  bool tcp_nodelay{true};
  bool reuse_port{true};
  bool serve_date_header{true};
  bool serve_server_header{true};
  // parser leniency; every one of these re-admits a smuggling variant
  bool lenient_headers{false};
  bool lenient_chunked_length{false};
  bool lenient_keep_alive{false};

  // websocket sessions on upgraded connections ([cornet.http.server.ws])
  websocket::session_options_t ws{};

  /**
   * @brief load overrides from [cornet.http.server].
   */
  void load(config_t* config);
};

/**
 * @brief per-connection counters, aggregated by the server.
 */
struct connection_metrics_t {
  uint64_t requests{0};
  uint64_t responses{0};
  uint64_t pipelined_batches{0};
  uint64_t writev_calls{0};
  uint64_t writev_partial{0};
  uint64_t iov_batch_split{0};
  uint64_t spill_used{0};
  uint64_t protocol_errors{0};
  uint64_t timeouts{0};
  uint64_t tls_handshake_errors{0};
};

/**
 * @brief one client connection: read, parse, dispatch, write, repeat.
 *
 * The loop is two-phase on purpose. Routing, `Expect: 100-continue` and the
 * aggregate-or-stream decision all happen at HeadersReady, before a single body
 * byte has been consumed; only aggregated requests wait for MessageReady. A
 * single-phase loop that dispatches at MessageReady cannot express a streaming
 * upload at all, and has nowhere to answer 100-continue. (The interim response is
 * only *staged* there, though: it leaves with the round's flush, so a peer that
 * strictly waits for it waits out its own timer instead.)
 *
 * Deadlines come from the context's timer wheel rather than a link_timeout on each
 * recv, which keeps the SQE count at one per read instead of two.
 *
 * Only the steps that actually touch the socket are coroutines — fill(), flush(),
 * read_body_chunk(). Framing (frame_head/frame_response) and iovec building are
 * plain functions, so a synchronous handler costs no frame beyond run()'s own:
 * each `co_await` of a helper coroutine is an allocation, and a few of them per
 * request would undo the point of having synchronous handlers at all.
 */
class connection_t {
 public:
  connection_t(context_t& ctx, tls::transport_t transport, const server_options_t& opt,
               buffer_pool_t& pool, std::shared_ptr<timer_wheel_t> wheel,
               connection_metrics_t& metrics);
  ~connection_t();

  connection_t(const connection_t&) = delete;
  connection_t& operator=(const connection_t&) = delete;

  /**
   * @brief serve this connection until it closes. The connection's lifetime is
   * this coroutine's lifetime.
   */
  CORNET_NODISCARD coro_t<void> run(const router_t& router);

  /**
   * @brief ask this connection to finish its current exchange and close.
   * Cancels only this connection's inflight read, so a response already being
   * written is never truncated.
   */
  void request_close();

  CORNET_NODISCARD int native_fd() const { return tr_.native_fd(); }

 private:
  friend class body_reader_t;
  friend class body_writer_t;
  friend class response_t;

  // one response waiting to be written
  struct pending_t {
    uint32_t head_off{0};
    uint32_t head_len{0};
    uint32_t hdr_off{0};
    uint32_t hdr_len{0};
    body_source_t source{body_source_t::None};
    uint32_t body_off{0};
    uint32_t body_len{0};
    std::string_view external{};
    buffer_lease_t owned{};
    // pin() objects backing `external` live here until the writev completes;
    // without the move the shared response_t's reset for the next pipelined
    // request would free them under our feet
    pin_arena_t pinned{};
    // File body: owned until the splice/stream finishes (source == File).
    int     file_fd{-1};
    int64_t file_pos{0};
  };

  // ── setup / teardown ──
  CORNET_NODISCARD expected<void> attach_buffers();
  void release_buffers();

  // ── streaming write support ──
  /**
   * @brief stage status line + headers for deferred async send.
   *
   * Called by response_t::chunked() so that the first body_writer_t::write()
   * sends head + hdr + the first chunk in a single writev.
   */
  void stage_headers();
  /**
   * @brief send body chunk(s) from stream_out_ to socket.
   *
   * On the first call (after stage_headers()), sends head + hdr + chunk-data
   * in one writev. Subsequent calls send only chunk-data.
   * Coroutine that handles short writes and cancellation.
   */
  CORNET_NODISCARD coro_t<expected<void>> flush_stream();

  /**
   * @brief write a run of bytes straight to the socket, bypassing the staging
   * buffers. Used by body_writer_t for chunks that do not fit stream_out_:
   * the staging capacity is an implementation choice, not something the
   * public writer API should ever force a caller to know about.
   */
  CORNET_NODISCARD coro_t<expected<void>> write_direct(std::string_view data);

  // ── the loop's phases ──
  void frame_head(bool close_after);
  CORNET_NODISCARD coro_t<expected<uint32_t>> fill();
  CORNET_NODISCARD expected<void> prepare_body(const route_t* route);
  CORNET_NODISCARD coro_t<void> invoke(const route_t& route);
  CORNET_NODISCARD coro_t<bool> run_filters(const router_t& router);
  void frame_response(bool close_after);
  CORNET_NODISCARD coro_t<expected<void>> flush();
  CORNET_NODISCARD coro_t<void> shutdown_gracefully();

  // File-body path: head/hdr leave through the same writev batch as everything
  // else; the body then goes by double-splice (internal pipe) on the plain
  // transport, or is streamed through the record layer under TLS. The pipe is
  // per-connection, opened on first use.
  CORNET_NODISCARD coro_t<expected<void>> splice_file_to_socket(int fd, int64_t& pos,
                                                                uint64_t len);
  CORNET_NODISCARD coro_t<expected<void>> stream_file_over_tls(int fd, int64_t& pos,
                                                               uint64_t len);
  CORNET_NODISCARD bool ensure_splice_pipe();

  // ── error paths ──
  void write_error(status_t status);
  void write_continue();

  // ── protocol upgrade ──
  /**
   * @brief flush anything already answered, write the 101, then hand the
   * transport over to a websocket session driven by the route's handler.
   * Returns with the transport gone; run() must only abandon bookkeeping.
   */
  CORNET_NODISCARD coro_t<void> run_websocket(const route_t& route,
                                              std::string_view key,
                                              std::string_view subprotocol);

  // ── streaming body support, driven by body_reader_t ──
  CORNET_NODISCARD coro_t<expected<std::string_view>> read_body_chunk();

  // assemble one iovec span and drain it to the socket (short-write safe);
  // shared by pipelined batches and the head/hdr of a file response
  CORNET_NODISCARD coro_t<expected<void>> writev_span(struct iovec* iov, uint32_t n);

  uint32_t build_iovecs(uint32_t begin, uint32_t end);
  void     reset_round();

  // A streaming response whose route ended without finish(). Either it can
  // still be replaced by a 500 (zero wire bytes) or it must be truncated.
  CORNET_NODISCARD bool needs_streaming_settlement() const {
    return resp_.body_source() == body_source_t::Streaming && !stream_finished_;
  }
  // Frames its own 500 on the swap path; settling always plans a close.
  void settle_streaming(bool& close_after);

  context_t&               ctx_;
  tls::transport_t         tr_;
  const server_options_t&  opt_;
  buffer_pool_t&           pool_;
  std::shared_ptr<timer_wheel_t> wheel_;
  connection_metrics_t&    metrics_;
  // the idle/header/body/close-wait deadlines; fires its single internal
  // canceler, cascading into both scopes below
   deadline_t               deadline_;
  // The two kill scopes, children of deadline_'s canceler. A deadline fire
  // cascades into both; request_close() latches read_scope_ ONLY — so writes
  // (never wrapped at all, a truncated response is worse than a late timeout)
  // and the drain window (drain_scope_) survive until their own moments.
  // Declared after deadline_: their ctors take its canceler as parent.
  canceler_t               read_scope_;
  canceler_t               drain_scope_;
  // file bodies: per-connection pipe + TLS staging, both lazy
  int                       splice_pipe_[2]{-1, -1};
  std::unique_ptr<char[]>   file_scratch_;

  head_buffer_t  in_;
  spill_buffer_t spill_;
  headers_t      headers_;
  parser_t       parser_;
  body_buffer_t  body_;

  // Three append-only staging buffers rather than one. The status line and
  // framework headers can only be written once the body length is known, which is
  // after the handler returns, while user headers are written during it. Keeping
  // them apart and letting writev stitch the segments together avoids both
  // rewriting and copying the header block.
  out_buffer_t head_out_;
  out_buffer_t hdr_out_;
  out_buffer_t body_out_;
  out_buffer_t stream_out_;   // chunk-size lines + data for streaming writes

  request_t     req_;
  response_t    resp_;
  param_slots_t params_;
  body_reader_t reader_;

  static constexpr uint32_t kMaxPendingIov = 3 * 64;
  pending_t pending_[64]{};
  uint32_t  pending_n_{0};

  // msghdr/iovec live here, not in a coroutine frame local: the kernel reads them
  // after the write is submitted
  struct iovec iov_[kMaxPendingIov]{};
  uint32_t     iov_n_{0};
  uint32_t     iov_head_{0};

  const router_t* router_{nullptr};
  const route_t*  route_{nullptr};
  body_mode_t     body_mode_{body_mode_t::Empty};

  // Offset in the receive buffer where the in-flight request's body starts. The region
  // from here on is rewound and refilled for every body read, which is what lets a body
  // be larger than the buffer: compact() is illegal while a message is being parsed (it
  // would move the bytes every header view points at), but body bytes are consumed as
  // they arrive, so nothing still referenced lives above this mark.
  uint32_t body_window_{0};
  // headers parsed, body not complete. Says "the receive buffer holds a message that is
  // still being parsed", which is precisely when compaction must not happen.
  bool     in_body_{false};

  bool close_after_flush_{false};
  bool closing_{false};
  // set once run_websocket() took the transport: run()'s tail skips the
  // graceful shutdown and the abandon — the session owns both now
  bool upgraded_{false};
  // the live session while run_websocket() is inside it, so request_close()
  // (drain) reaches a connection parked in a websocket recv
  websocket::session_t* ws_active_{nullptr};
  bool streaming_{false};
  bool body_complete_{false};
  bool streaming_write_{false};  // body_writer_t is currently streaming
  bool headers_staged_{false};   // head+hdr staged, waiting for first flush_stream
  // body_writer_t::finish() ran for the response being streamed. A route that
  // returns without finishing is settled when the response would be framed:
  // swapped for a 500 if nothing was sent yet, truncated deterministically
  // otherwise — a client must never wait on bytes that will never come.
  bool stream_finished_{false};
};

} // namespace cornet::http

#endif // CORNET_HTTP_SERVER_CONNECTION_H
