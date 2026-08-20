#ifndef CORNET_HTTP_CLIENT_CONNECTION_H
#define CORNET_HTTP_CLIENT_CONNECTION_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/uio.h>

#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/coro.h"
#include "cornet/http/client/message.h"
#include "cornet/http/common/buffer.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/serializer.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/net/socket.h"
#include "cornet/utils/config.h"

namespace cornet::http {

class client_pool_t;

/**
 * @brief client tunables. Defaults are deliberately conservative.
 */
struct client_options_t {
  // response head buffer; a status line plus headers that does not fit is an error
  uint32_t max_header_bytes{16u << 10};
  uint16_t max_headers{64};
  // chunked trailers to record; 0 drops them, which is the default
  uint16_t max_trailers{0};
  uint64_t max_body_bytes{8ull << 20};
  // first reservation for a body of unknown length; it grows from here
  uint32_t aggregate_threshold{256u << 10};
  // request-side staging capacities
  uint32_t head_buffer_bytes{4u << 10};
  uint32_t hdr_buffer_bytes{4u << 10};
  uint32_t chunk_buffer_bytes{4u << 10};

  // ── pool ──
  uint16_t max_conns_per_host{8};
  uint16_t max_idle_per_host{4};
  uint32_t max_total_conns{1024};

  // ── behaviour ──
  uint8_t max_retries{1};
  uint8_t max_redirects{0};
  bool    tcp_nodelay{true};
  bool    send_user_agent{true};
  bool    send_accept{true};
  std::string user_agent{"cornet"};
  // parser leniency; each one re-admits a response-splitting variant
  bool lenient_headers{false};
  bool lenient_chunked_length{false};
  bool lenient_keep_alive{false};

  // ── dns ──
  uint32_t dns_cache_entries{256};
  std::chrono::milliseconds dns_cache_ttl{30000};

  // ── deadlines ──
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds write_timeout{10000};
  std::chrono::milliseconds response_timeout{10000};
  std::chrono::milliseconds body_timeout{30000};
  std::chrono::milliseconds total_timeout{60000};
  std::chrono::milliseconds idle_timeout{60000};
  std::chrono::milliseconds pool_wait_timeout{5000};
  std::chrono::milliseconds timer_tick{500};

  /**
   * @brief load overrides from [cornet.http.client].
   */
  void load(config_t* config);
};

/**
 * @brief counters for one client, aggregated across its connections.
 */
struct client_metrics_t {
  uint64_t requests{0};
  uint64_t responses{0};
  uint64_t conn_created{0};
  uint64_t conn_reused{0};
  uint64_t conn_closed{0};
  uint64_t connect_errors{0};
  uint64_t stale_discarded{0};
  uint64_t retries{0};
  uint64_t redirects{0};
  uint64_t timeouts{0};
  uint64_t protocol_errors{0};
  uint64_t dns_lookups{0};
  uint64_t dns_cache_hits{0};
  uint64_t writev_calls{0};
  uint64_t writev_partial{0};
  uint64_t pool_waits{0};
};

/**
 * @brief write the request line and the framework headers a client has to add.
 *
 * A pure function of the request and the options — no connection state is involved —
 * which is why it lives outside client_connection_t: framing is the part worth
 * testing byte for byte, and doing that should not need a socket.
 *
 * Emits, in order: the request line, Host (unless the caller wrote one), User-Agent,
 * Accept, Connection, and the framing header the body implies. It does *not* write
 * the blank line that ends the header section: the user's headers go between this
 * block and that line, and they are staged in the request.
 *
 * @param chunked_upload frame for a streamed body rather than a staged one
 */
void frame_request_head(out_buffer_t& out, const client_request_t& req,
                        const client_options_t& opt, bool chunked_upload);

/**
 * @brief one client-side connection: write a request, read its response, repeat.
 *
 * The mirror image of the server's connection_t, and deliberately not a shared base
 * class with it: the loops run in opposite directions ("write, then read" against
 * "read, then write"), and the two have different ideas about who owns the buffers
 * at the end. What they do share is the whole common layer underneath.
 *
 * One exchange is in flight at a time. Pipelining several requests down one
 * connection is not supported on purpose: it buys throughput only when responses are
 * uniformly fast, and it makes both head-of-line blocking and retry semantics much
 * harder to reason about. Concurrency comes from having several connections instead,
 * which is what client_pool_t is for. That choice also makes the response handover
 * sound: after a complete response, anything still in the receive buffer can only be
 * something we did not ask for.
 *
 * Deadlines come from the shared timer wheel rather than a link_timeout on each
 * operation, for the same reason as on the server: one SQE per read instead of two.
 */
class client_connection_t {
 public:
  client_connection_t(context_t& ctx, tcp::socket_t sock, const client_options_t& opt,
                      buffer_pool_t& pool, timer_wheel_t& wheel, client_metrics_t& metrics,
                      std::string host, uint16_t port);
  ~client_connection_t();

  client_connection_t(const client_connection_t&) = delete;
  client_connection_t& operator=(const client_connection_t&) = delete;

  /**
   * @brief resolve, connect, and hand back a ready connection.
   * @param pre a pre-resolved address, e.g. from the client's dns cache
   */
  CORNET_NODISCARD static coro_t<expected<std::unique_ptr<client_connection_t>>> open(
      context_t& ctx, const client_options_t& opt, buffer_pool_t& pool, timer_wheel_t& wheel,
      client_metrics_t& metrics, std::string_view host, uint16_t port,
      const resolved_address* pre = nullptr);

  /**
   * @brief adopt an already connected socket. For tests and for callers that did
   * their own connect.
   */
  CORNET_NODISCARD static expected<std::unique_ptr<client_connection_t>> adopt(
      context_t& ctx, tcp::socket_t sock, const client_options_t& opt, buffer_pool_t& pool,
      timer_wheel_t& wheel, client_metrics_t& metrics, std::string host, uint16_t port);

  // ── the ordinary exchange ──

  /**
   * @brief write the request, read the whole response.
   * @return a response owning its buffers, or the first error that stopped it
   */
  CORNET_NODISCARD coro_t<expected<client_response_t>> exchange(client_request_t& req);

  // ── streaming download ──

  /**
   * @brief write the request and stop once the response headers are in.
   *
   * The body is then the caller's to pull with read_body(). Until it is finished the
   * connection cannot be reused, and the response's views live in buffers this
   * connection still owns — which is why the facade wraps this in a stream object
   * that owns both ends.
   */
  CORNET_NODISCARD coro_t<expected<void>> begin_exchange(client_request_t& req);

  /**
   * @brief next run of body bytes; empty view at end of body.
   * The view is valid until the following read_body().
   */
  CORNET_NODISCARD coro_t<expected<std::string_view>> read_body();

  /**
   * @brief consume and discard the rest of the body, so the connection can be reused.
   */
  CORNET_NODISCARD coro_t<expected<void>> drain_body();

  /**
   * @brief hand the receive buffers over as a response object.
   * Only legal once the body is complete.
   */
  CORNET_NODISCARD expected<client_response_t> take_response();

  // ── chunked upload ──

  /**
   * @brief write the head with Transfer-Encoding: chunked and nothing else.
   */
  CORNET_NODISCARD coro_t<expected<void>> begin_chunked(client_request_t& req);

  /**
   * @brief send one chunk. The data is referenced, not copied.
   */
  CORNET_NODISCARD coro_t<expected<void>> write_chunk(std::string_view data);

  /**
   * @brief send the terminating zero-length chunk.
   */
  CORNET_NODISCARD coro_t<expected<void>> finish_chunks();

  /**
   * @brief read the response to a chunked upload.
   */
  CORNET_NODISCARD coro_t<expected<client_response_t>> read_response(method_t m);

  // ── state the pool and the retry logic ask about ──

  /**
   * @brief whether this connection can serve another request.
   */
  CORNET_NODISCARD bool reusable() const {
    return !broken_ && !timed_out_ && keep_alive_ && body_complete_;
  }

  /**
   * @brief whether any response byte has arrived for the current exchange.
   *
   * The retry rule turns on this: replaying a request the peer may have already
   * started answering is not a retry, it is a second request.
   */
  CORNET_NODISCARD bool responded() const { return responded_; }

  /**
   * @brief cheap liveness check before reusing an idle connection.
   *
   * A peek that returns 0 means the peer sent FIN while the connection sat in the
   * pool; anything readable means the stream is desynchronised. Both make this
   * connection unusable. One non-blocking syscall, which is much cheaper than
   * discovering the same thing by having a request fail.
   */
  CORNET_NODISCARD bool alive_hint() const;

  /**
   * @brief set the deadline for the whole exchange. Zero clears it.
   */
  void set_deadline(std::chrono::milliseconds total);

  /**
   * @brief ask the current operation to stop; the connection is not reusable after.
   */
  void abort();

  /**
   * @brief release every buffer and close the socket.
   */
  void close();

  CORNET_NODISCARD int native_fd() const { return sock_.native_fd(); }
  CORNET_NODISCARD std::string_view host() const { return host_; }
  CORNET_NODISCARD uint16_t port() const { return port_; }
  CORNET_NODISCARD uint32_t exchanges() const { return exchanges_; }
  CORNET_NODISCARD bool timed_out() const { return timed_out_; }

  // ── the headers of a response still being streamed ──

  CORNET_NODISCARD status_t status() const;
  CORNET_NODISCARD const headers_t* headers() const;
  CORNET_NODISCARD bool body_complete() const { return body_complete_; }

  /**
   * @brief the timer node the pool uses for this connection's idle deadline.
   * Owned here so that arming it costs no allocation.
   */
  CORNET_NODISCARD timer_node_t& idle_timer() { return idle_timer_; }

  /**
   * @brief the pool this connection belongs to, if any.
   * Set by client_pool_t so that an expiring idle timer can find its way home
   * without a per-connection allocation for the callback.
   */
  void set_pool(client_pool_t* pool) { pool_owner_ = pool; }
  CORNET_NODISCARD client_pool_t* pool_owner() const { return pool_owner_; }

 private:
  CORNET_NODISCARD expected<void> attach();
  CORNET_NODISCARD expected<void> ensure_node();

  void frame_head(client_request_t& req, bool chunked_upload);
  void stage_request(client_request_t& req, bool head_only);
  CORNET_NODISCARD coro_t<expected<void>> write_staged();
  void advance_iovecs(uint32_t written);

  /**
   * @brief write the request and read until the final response headers are in.
   * Shared by the aggregating and the streaming entry points.
   */
  CORNET_NODISCARD coro_t<expected<void>> send_and_read_headers(client_request_t& req);

  CORNET_NODISCARD coro_t<expected<uint32_t>> fill();

  /**
   * @brief read until the final response headers are in, swallowing 1xx.
   * @param stop_at_continue return as soon as a 100-continue arrives
   * @return true when it stopped at a 100-continue, false when final headers are in
   */
  CORNET_NODISCARD coro_t<expected<bool>> read_headers(method_t m, bool stop_at_continue);

  void record_headers();

  /**
   * @brief mark the response complete, and notice anything the peer sent behind it.
   */
  void finish_message();

  CORNET_NODISCARD expected<void> prepare_body(bool stream);
  CORNET_NODISCARD coro_t<expected<void>> aggregate_body();
  CORNET_NODISCARD coro_t<expected<uint32_t>> refill_body();
  CORNET_NODISCARD expected<void> append_body(std::string_view run);

  void arm_phase(std::chrono::milliseconds phase);

  context_t&               ctx_;
  tcp::socket_t            sock_;
  const client_options_t&  opt_;
  buffer_pool_t&           pool_;
  timer_wheel_t&           wheel_;
  client_metrics_t&        metrics_;
  canceler_t               canceler_;
  timer_node_t             timer_{};
  timer_node_t             idle_timer_{};

  parser_t   parser_{parser_t::type_t::Response};
  // the receive state for the exchange in flight; handed to the response when it
  // completes, and replaced by a fresh one for the next request
  inbound_t* node_{nullptr};

  out_buffer_t head_out_;   // request line + framework headers
  out_buffer_t chunk_out_;  // chunk-size lines for a streaming upload

  // Four segments cover every request: framework head, user headers, the CRLF that
  // ends them, and the body. They live here rather than in a coroutine frame because
  // the kernel reads them after the write is submitted.
  static constexpr uint32_t kMaxIov = 4;
  struct iovec iov_[kMaxIov]{};
  uint32_t     iov_n_{0};
  uint32_t     iov_head_{0};

  std::string host_;
  uint16_t    port_{0};
  client_pool_t* pool_owner_{nullptr};

  // offset in the head buffer where this response's body region starts; the region
  // is rewound and refilled for every read, which is what lets a body be larger
  // than the buffer
  uint32_t body_window_{0};
  uint64_t deadline_ns_{0};
  uint32_t exchanges_{0};

  bool headers_done_{false};
  bool body_complete_{true};
  bool keep_alive_{false};
  bool responded_{false};
  bool broken_{false};
  bool timed_out_{false};
  bool streaming_{false};
  bool chunked_upload_{false};
};

} // namespace cornet::http

#endif // CORNET_HTTP_CLIENT_CONNECTION_H
