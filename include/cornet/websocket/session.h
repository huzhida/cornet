#ifndef CORNET_WEBSOCKET_SESSION_H
#define CORNET_WEBSOCKET_SESSION_H

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <sys/uio.h>

#include "cornet/concurrency/deadline.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/coro.h"
#include "cornet/http/common/buffer.h"
#include "cornet/tls/transport.h"
#include "cornet/websocket/common/frame.h"
#include "cornet/websocket/common/protocol.h"

namespace cornet {

struct config_t;

namespace http {

struct connection_metrics_t;

} // namespace http

namespace websocket {

/**
 * @brief per-session tunables.
 *
 * Deliberately few: masking and frame shape are protocol obligations, not
 * preferences, so there is nothing to configure about them. What remains is
 * resource budgets and deadlines.
 */
struct session_options_t {
  // an aggregated message past this limit is answered with close 1009. Frames
  // arrive in runs, so the check bounds the aggregation buffer, not the wire.
  uint64_t max_message_bytes{16ull << 20};
  // no frame from the peer for this long ends the session; 0 disables
  std::chrono::milliseconds idle_timeout{0};
  // how long finish() waits for the peer's Close before closing the transport
  std::chrono::milliseconds close_timeout{2000};
  // timing-wheel tick for idle/close/drain deadlines. Wheel tenants are keyed
  // by tick on the context, so the default shares one coarse wheel with
  // whoever else picked 100ms — do NOT point these deadlines at the
  // coroutine-deadline wheel (its 5ms tick means thousands of empty rounds).
  std::chrono::milliseconds timer_tick{100};
  // receive window; a frame larger than this is aggregated from runs, so this
  // is a memory budget, not a wire limit
  uint32_t recv_buffer_bytes{64u << 10};

  /**
   * @brief load overrides from keys below `prefix`, e.g. "cornet.http.server.ws".
   */
  void load(config_t* config, const char* prefix);
};

/**
 * @brief one received message.
 *
 * The payload is a view into session-owned buffers and stays valid until the
 * next recv() — the contract is the body_reader_t one, for the same reason:
 * a copy per message is the difference between websocket being a framing
 * layer and being a memcpy layer.
 *
 * Control frames never surface as Ping/Pong (those are answered inside the
 * session); Close does, exactly once, so the handler can read the code and
 * reason before the connection winds down.
 */
struct message_t {
  opcode_t         opcode{opcode_t::Text};
  std::string_view payload{};

  CORNET_NODISCARD bool text() const { return opcode == opcode_t::Text; }
  CORNET_NODISCARD bool binary() const { return opcode == opcode_t::Binary; }

  /**
   * @brief close code for a Close message, NoStatus for a bodyless one.
   * Meaningless for other opcodes.
   */
  CORNET_NODISCARD close_code_t close_code() const;

  /**
   * @brief close reason for a Close message, UTF-8 as received (not validated).
   */
  CORNET_NODISCARD std::string_view close_reason() const;
};

/**
 * @brief one websocket session, post-handshake. Shared by server and client.
 *
 * The lifetime contract is the connection_t one: the object lives on a
 * coroutine frame and the driving coroutine runs the handler — no heap, no
 * refcount, no cross-thread story. On the server the http connection builds
 * the session after writing the 101 and hands over the transport and any
 * bytes read past the upgrade request; websocket::connect() does the
 * equivalent on the client.
 *
 * recv() and send() may each be driven by one coroutine at a time (a reader
 * and a writer in parallel is fine and is the common fan-out shape); two
 * concurrent readers would interleave frames and are not supported.
 *
 * Close semantics follow the protocol's half-duplex handshake: receiving a
 * Close frame is answered immediately, delivering a Close message once; after
 * the handler returns, finish() completes whatever the handshake still needs
 * and only then closes the transport, so a peer that waits for the echo does
 * not watch the socket die first.
 */
class session_t {
 public:
  /**
   * @param wheel shared wheel ownership: a session co-owns its deadline wheel
   * for its whole life, so the wheel cannot be reclaimed out from under an
   * armed idle deadline (client sessions otherwise reference a wheel nobody
   * keeps once the connect() frame is gone).
   */
  session_t(context_t& ctx, tls::transport_t tr, role_t role, http::buffer_pool_t& pool,
            std::shared_ptr<timer_wheel_t> wheel, const session_options_t& opt,
            std::string_view leftover = {}, http::connection_metrics_t* metrics = nullptr);
  ~session_t();

  session_t(const session_t&) = delete;
  session_t& operator=(const session_t&) = delete;

  /**
   * @brief next complete message.
   *
   * Fragmented messages are reassembled; a peer's Ping is already answered by
   * the time recv() returns, and its Pong is consumed silently. A Close frame
   * is delivered once as a Close message (the echo has been sent), after which
   * recv() fails with websocket_error_t::Closed.
   * @return the message, or an error: transport failure, a protocol violation
   *         (the peer has been sent the matching close code), ETIMEDOUT on the
   *         idle deadline, ECANCELED on request_close()
   */
  CORNET_NODISCARD coro_t<expected<message_t>> recv();

  /**
   * @brief send one unfragmented message.
   * @param op Text or Binary; control opcodes go through ping()/close()
   */
  CORNET_NODISCARD coro_t<expected<void>> send(std::string_view payload,
                                               opcode_t op = opcode_t::Text);

  CORNET_NODISCARD coro_t<expected<void>> send_text(std::string_view payload) {
    co_return co_await send(payload, opcode_t::Text);
  }

  CORNET_NODISCARD coro_t<expected<void>> send_binary(std::string_view payload) {
    co_return co_await send(payload, opcode_t::Binary);
  }

  /**
   * @brief send a Ping; the peer's Pong is consumed by recv().
   * Payload is limited to 125 bytes by the protocol (silently truncated).
   */
  CORNET_NODISCARD coro_t<expected<void>> ping(std::string_view payload = {});

  /**
   * @brief begin the close handshake by sending a Close frame.
   *
   * Idempotent: a no-op once closing has begun from either side. The peer's
   * answering Close is received by the next recv() or waited out by finish().
   */
  CORNET_NODISCARD coro_t<expected<void>> close(close_code_t code = close_code_t::Normal,
                                                std::string_view reason = {});

  /**
   * @brief settle the session after the handler is done.
   *
   * Sends Close if the handler never did, waits out the peer's answer up to
   * close_timeout, half-closes, drains briefly (so the peer does not read
   * RST), and releases the transport. Called by the server after every
   * websocket route returns; client code should do the same or let the
   * destructor close uncleanly.
   */
  CORNET_NODISCARD coro_t<void> finish();

  /**
   * @brief interrupt the session: pending and future recv() fail with
   * ECANCELED. The handler should wind up; finish() then closes with
   * GoingAway. This is what the server's drain() reaches into.
   */
  void request_close();

  CORNET_NODISCARD bool is_open() const;
  CORNET_NODISCARD role_t role() const { return role_; }
  CORNET_NODISCARD std::string_view subprotocol() const { return subprotocol_; }
  CORNET_NODISCARD int native_fd() const { return tr_.native_fd(); }

  /**
   * @brief record the negotiated subprotocol. Called by the handshake code on
   * both sides; there is nothing to negotiate post-handshake.
   */
  void set_subprotocol(std::string_view proto) { subprotocol_ = proto; }

 private:
  enum class state_t : uint8_t {
    Open,           // data may flow both ways
    CloseSent,      // our Close is out, waiting for the peer's
    CloseReceived,  // the peer's Close arrived (and was echoed if we were Open)
  };

  CORNET_NODISCARD coro_t<expected<uint32_t>> fill();
  CORNET_NODISCARD coro_t<expected<void>> writev_span(struct iovec* iov, uint32_t n);
  CORNET_NODISCARD coro_t<expected<void>> send_control(opcode_t op, std::string_view payload);
  // a protocol violation: Close with the matching code, then fail the caller
  CORNET_NODISCARD coro_t<expected<message_t>> fail_protocol(websocket_error_t err,
                                                             close_code_t code);
  // one step of the receive machinery: consumes the next interesting event.
  // Data completes a message only when `deliver` (finish() drains instead);
  // a Close always produces its message, since both recv() and finish() key
  // on it
  CORNET_NODISCARD coro_t<expected<message_t>> step(bool deliver);
  // next frame-masking key from the session's xorshift state; never 0
  uint32_t next_mask_key();

  context_t&               ctx_;
  tls::transport_t         tr_;
  role_t                   role_;
  http::buffer_pool_t&     pool_;
  session_options_t        opt_;
  http::connection_metrics_t* metrics_;

  frame_decoder_t decoder_;
  // idle/close-wait/drain deadlines; its canceler IS the read kill switch
  // (no scope needs shielding here: once reads die the session is closing)
   deadline_t deadline_;

  http::head_buffer_t in_;
  http::body_buffer_t msg_;   // aggregation for fragmented / window-spanning messages
  http::body_buffer_t out_;   // client-side masking needs a payload copy

  // the frame currently being consumed: header parsed, payload arriving in runs
  frame_t  cur_{};
  bool     frame_open_{false};
  uint64_t cur_remaining_{0};

  opcode_t frag_opcode_{opcode_t::Continue};
  bool     fragment_open_{false};

  state_t   state_{state_t::Open};
  bool      closing_{false};    // request_close() latched; close reason GoingAway
  // marks finish()'s Close wait: fill() arms this window with close_timeout
  // instead of the idle deadline
  bool      close_wait_{false};
  bool      finished_{false};   // finish() ran; the transport is abandoned
  uint32_t  mask_seed_{0};      // xorshift state; nonzero by construction
  std::string subprotocol_{};
};

} // namespace websocket
} // namespace cornet

#endif // CORNET_WEBSOCKET_SESSION_H
