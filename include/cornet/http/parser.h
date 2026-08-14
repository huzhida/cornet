#ifndef CORNET_HTTP_PARSER_H
#define CORNET_HTTP_PARSER_H

#include <cstdint>

#include "cornet/http/buffer.h"
#include "cornet/http/common.h"
#include "cornet/http/headers.h"

namespace cornet::http {

// defined in parser.cc, where llhttp's types are visible
struct parser_callbacks_t;

/**
 * @brief limits applied while parsing one message.
 */
struct parser_limits_t {
  uint32_t max_headers{64};
  uint64_t max_body_bytes{8ull << 20};
  // allow the lenient_* llhttp escape hatches; off by default, see parser.cc
  bool lenient_headers{false};
  bool lenient_chunked_length{false};
  bool lenient_keep_alive{false};
};

/**
 * @brief llhttp wrapper. Records header positions, never copies them.
 *
 * llhttp.h is deliberately absent from this header: the core promises to depend
 * only on liburing, so the protocol library stays private to the module. The
 * parser state is held in an inline byte array sized with headroom and checked by
 * a static_assert in the .cc, which keeps the dependency private without paying
 * for a pimpl allocation per connection.
 */
class parser_t {
 public:
  enum class type_t : uint8_t { Request, Response };

  // Storage size for the parser state. Public only so the .cc can assert it
  // against the real llhttp_t; treat it as an implementation detail.
  static constexpr uint32_t kStateSize = 128;

  /**
   * @brief outcome of feeding bytes to the parser.
   *
   * HeadersReady is what makes two-phase dispatch possible. Handing control back
   * as soon as the headers are complete — before any body byte is consumed — is
   * the only point at which a server can still route the request, answer
   * `Expect: 100-continue`, reject an oversized body without reading it, or start
   * a streaming handler that will consume the body itself. A parser that only
   * reports MessageReady forces every handler to run after the whole body has
   * been buffered, which makes streaming uploads impossible to express.
   */
  enum class result_t : uint8_t {
    NeedMore,
    HeadersReady,
    MessageReady,
    BodyPaused,
    Upgrade,
    Error,
  };

  /**
   * @brief result name, for logs.
   */
  static const char* to_string(result_t r);

  explicit parser_t(type_t type);
  ~parser_t();

  parser_t(const parser_t&) = delete;
  parser_t& operator=(const parser_t&) = delete;

  /**
   * @brief bind the buffers this parser writes into. Call once per connection.
   */
  void bind(head_buffer_t& head, spill_buffer_t& spill, headers_t& headers);

  /**
   * @brief reset for the next message on the same connection.
   * Keeps the type, callbacks and lenient flags.
   */
  void reset();

  void set_limits(const parser_limits_t& limits);

  /**
   * @brief feed a range of the header buffer.
   *
   * Continues from wherever the previous call stopped, so a caller can hand over
   * only the newly received bytes.
   * @param off offset into the bound header buffer
   * @param len number of bytes at that offset
   */
  CORNET_NODISCARD result_t execute(uint32_t off, uint32_t len);

  /**
   * @brief resume after BodyPaused and continue from the pause point.
   */
  CORNET_NODISCARD result_t resume();

  /**
   * @brief signal end of input (peer half-closed).
   * Completes messages whose length is delimited by connection close.
   */
  CORNET_NODISCARD result_t finish();

  /**
   * @brief where to send aggregated body bytes. nullptr pauses on each run
   * instead, which is how streaming reads are driven.
   */
  void set_body_sink(body_buffer_t* sink) { body_sink_ = sink; }

  /**
   * @brief most recent run of body bytes, valid until the next resume().
   * Used by the streaming reader while the parser is paused.
   */
  CORNET_NODISCARD std::string_view pending_body() const {
    return {body_run_, body_run_len_};
  }
  void clear_pending_body() { body_run_ = nullptr; body_run_len_ = 0; }

  // ── parsed message facts ──

  CORNET_NODISCARD method_t  method() const { return method_; }
  CORNET_NODISCARD version_t version() const { return version_; }
  CORNET_NODISCARD uint16_t  status_code() const { return status_code_; }
  CORNET_NODISCARD std::string_view target() const;
  CORNET_NODISCARD bool      keep_alive() const { return keep_alive_; }
  CORNET_NODISCARD bool      upgrade() const { return upgrade_; }
  CORNET_NODISCARD bool      has_content_length() const { return has_content_length_; }
  CORNET_NODISCARD uint64_t  content_length() const { return content_length_; }
  CORNET_NODISCARD bool      chunked() const { return chunked_; }
  CORNET_NODISCARD bool      expects_continue() const { return expects_continue_; }
  CORNET_NODISCARD uint64_t  body_received() const { return body_received_; }

  CORNET_NODISCARD error_t error() const { return error_; }

  /**
   * @brief bytes consumed from the buffer for the current message so far.
   * Drives pipelining: whatever follows belongs to the next request.
   */
  CORNET_NODISCARD uint32_t consumed() const { return consumed_; }

  /**
   * @brief offset in the bound header buffer of the first byte not yet parsed.
   * After MessageReady this is exactly where the next pipelined request starts.
   */
  CORNET_NODISCARD uint32_t consumed_offset() const { return consumed_offset_; }

  /**
   * @brief whether the current feed window still holds unparsed bytes.
   * True after MessageReady when the peer pipelined another request.
   */
  CORNET_NODISCARD bool has_pending_input() const { return feed_pos_ < feed_end_; }

 private:
  friend struct parser_callbacks_t;

  /**
   * @brief drive llhttp over the remaining feed window and classify the outcome.
   */
  CORNET_NODISCARD result_t pump();

  CORNET_NODISCARD expected<void> accumulate(uint32_t& off, uint16_t& len, bool& spilled,
                                             const char* at, uint32_t n, bool allow_spill);
  CORNET_NODISCARD expected<void> finish_header();

  // Storage for llhttp_t. Its real size is asserted in parser.cc with headroom,
  // so a library upgrade that grows the struct fails to compile rather than
  // corrupting memory at run time.
  alignas(16) unsigned char state_[kStateSize]{};

  head_buffer_t*  head_{nullptr};
  spill_buffer_t* spill_{nullptr};
  headers_t*      headers_{nullptr};
  body_buffer_t*  body_sink_{nullptr};

  parser_limits_t limits_{};
  type_t          type_;

  // current feed window into the header buffer
  const char* feed_pos_{nullptr};
  const char* feed_end_{nullptr};

  // header being assembled across callbacks
  header_ref pending_{};
  bool       pending_valid_{false};

  // request target, assembled the same way as a header value
  uint32_t target_off_{0};
  uint16_t target_len_{0};
  bool     target_spilled_{false};

  // most recent body run, for the streaming reader
  const char* body_run_{nullptr};
  uint32_t    body_run_len_{0};

  method_t  method_{method_t::Unknown};
  version_t version_{version_t::Unknown};
  uint16_t  status_code_{0};
  uint64_t  content_length_{0};
  uint64_t  body_received_{0};
  uint32_t  consumed_{0};
  uint32_t  consumed_offset_{0};

  bool has_content_length_{false};
  bool chunked_{false};
  bool keep_alive_{true};
  bool upgrade_{false};
  bool expects_continue_{false};
  bool headers_signal_{false};
  bool message_done_{false};
  bool body_paused_{false};
  // guards the zero-length execute that lets a body-less message finish
  bool zero_fed_{false};

  error_t error_{};
};

} // namespace cornet::http

#endif // CORNET_HTTP_PARSER_H
