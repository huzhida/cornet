#ifndef CORNET_HTTP_CONNECTION_H
#define CORNET_HTTP_CONNECTION_H

#include <chrono>
#include <cstdint>
#include <string>
#include <sys/uio.h>

#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/coro.h"
#include "cornet/http/buffer.h"
#include "cornet/http/common.h"
#include "cornet/http/headers.h"
#include "cornet/http/message.h"
#include "cornet/http/parser.h"
#include "cornet/http/router.h"
#include "cornet/http/serializer.h"
#include "cornet/http/timer_wheel.h"
#include "cornet/net/socket.h"

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
  std::chrono::milliseconds drain_timeout{10000};

  bool tcp_nodelay{true};
  bool reuse_port{true};
  bool serve_date_header{true};
  bool serve_server_header{true};
  // parser leniency; every one of these re-admits a smuggling variant
  bool lenient_headers{false};
  bool lenient_chunked_length{false};
  bool lenient_keep_alive{false};

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
};

/**
 * @brief one client connection: read, parse, dispatch, write, repeat.
 *
 * The loop is two-phase on purpose. Routing, `Expect: 100-continue` and the
 * aggregate-or-stream decision all happen at HeadersReady, before a single body
 * byte has been consumed; only aggregated requests wait for MessageReady. A
 * single-phase loop that dispatches at MessageReady cannot express a streaming
 * upload at all, and has nowhere to answer 100-continue.
 *
 * Deadlines come from the context's timer wheel rather than a link_timeout on each
 * recv, which keeps the SQE count at one per read instead of two.
 *
 * dispatch and flush are deliberately not coroutines. Each `co_await` of a helper
 * coroutine is a frame allocation, and three of them per request would undo the
 * point of having synchronous handlers at all.
 */
class connection_t {
 public:
  connection_t(context_t& ctx, tcp::socket_t sock, const server_options_t& opt,
               buffer_pool_t& pool, timer_wheel_t& wheel, connection_metrics_t& metrics);
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

  CORNET_NODISCARD int native_fd() const { return sock_.native_fd(); }

 private:
  friend class body_reader_t;

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
  };

  // ── setup / teardown ──
  CORNET_NODISCARD expected<void> attach_buffers();
  void release_buffers();

  // ── the loop's phases ──
  CORNET_NODISCARD coro_t<expected<uint32_t>> fill();
  CORNET_NODISCARD expected<void> prepare_body(const route_t* route);
  CORNET_NODISCARD coro_t<void> invoke(const route_t& route);
  CORNET_NODISCARD coro_t<bool> run_filters(const router_t& router);
  void frame_response(bool close_after);
  CORNET_NODISCARD coro_t<expected<void>> flush();
  CORNET_NODISCARD coro_t<void> shutdown_gracefully();

  // ── error paths ──
  void write_error(status_t status);
  void write_continue();

  // ── streaming body support, driven by body_reader_t ──
  CORNET_NODISCARD coro_t<expected<std::string_view>> read_body_chunk();

  uint32_t build_iovecs();
  void     advance_iovecs(uint32_t written);
  void     reset_round();

  context_t&               ctx_;
  tcp::socket_t            sock_;
  const server_options_t&  opt_;
  buffer_pool_t&           pool_;
  timer_wheel_t&           wheel_;
  connection_metrics_t&    metrics_;
  canceler_t               canceler_;
  timer_node_t             timer_{};

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

  bool close_after_flush_{false};
  bool closing_{false};
  bool timed_out_{false};
  bool streaming_{false};
  bool body_complete_{false};
};

} // namespace cornet::http

#endif // CORNET_HTTP_CONNECTION_H
