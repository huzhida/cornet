#ifndef CORNET_HTTP_COMMON_PARSER_H
#define CORNET_HTTP_COMMON_PARSER_H

#include <cstdint>

#include "cornet/http/common/buffer.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/headers.h"

namespace cornet::http {

// defined in parser.cc, where llhttp's types are visible
struct parser_callbacks_t;

/**
 * @brief limits applied while parsing one message.
 */
struct parser_limits_t {
  uint32_t max_headers{64};
  /**
   * @brief how many chunked trailers to record; 0 means drop them.
   *
   * Off by default. Trailers arrive after every framing and routing decision has been
   * made, so recording them is only useful to code that explicitly goes looking (a
   * checksum, a gRPC status), while merging them into the header table by default
   * would hand every handler a value the peer appended after the fact. They get their
   * own budget so that a trailer flood cannot push a message over max_headers, and
   * fields that must never appear in a trailer are dropped whatever this is set to.
   */
  uint32_t max_trailers{0};
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
   * @brief tell a response parser which method the request used.
   *
   * A response to HEAD may carry Content-Length and still have no body, and nothing
   * in the response bytes says so. Without this the parser waits for a body that
   * never comes — and on a keep-alive connection it would eventually read the
   * *next* response as this one's body. Survives reset(), since a connection keeps
   * being reused for the same exchange shape until the caller says otherwise.
   */
  void set_response_to(method_t m) { response_to_ = m; }
  CORNET_NODISCARD method_t response_to() const { return response_to_; }

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

  /**
   * @brief whether a header name or value is half-accumulated right now.
   *
   * Such a run is recorded as an offset into the bound buffer and expects its
   * continuation to land right behind it, so rewinding the write cursor underneath it
   * would silently splice unrelated bytes into the name or the value. Trailers are
   * dropped rather than recorded, which is what keeps this false at every point where
   * a caller rewinds; the predicate exists so that assertion can be stated.
   */
  CORNET_NODISCARD bool mid_header() const {
    bool name_live = pending_valid_ && pending_.name_len > 0 && !pending_.name_spilled;
    bool value_live = pending_.value_len > 0 && !pending_.spilled;
    return name_live || value_live;
  }

 private:
  friend struct parser_callbacks_t;

  /**
   * @brief drive llhttp over the remaining feed window and classify the outcome.
   */
  CORNET_NODISCARD result_t pump();

  /**
   * @brief whether the message whose headers just completed can have a body.
   *
   * 1xx / 204 / 304 forbid one outright, and a response to HEAD only describes the
   * body it is not sending. In all three cases the message ends with its headers.
   */
  CORNET_NODISCARD bool headers_end_message() const;

  /**
   * @brief append a run to a name or value.
   * @param allow_spill copy to the spill buffer when the runs are not adjacent
   * @param must_copy   copy from the very first run, for bytes that live in a region
   *                    the reader is going to overwrite (i.e. trailers)
   */
  CORNET_NODISCARD expected<void> accumulate(uint32_t& off, uint16_t& len, bool& spilled,
                                             const char* at, uint32_t n, bool allow_spill,
                                             bool must_copy = false);
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
  // which request this is the response to; only meaningful for a response parser
  method_t        response_to_{method_t::Unknown};

  // current feed window into the header buffer
  const char* feed_pos_{nullptr};
  const char* feed_end_{nullptr};

  // header being assembled across callbacks
  header_ref pending_{};
  bool       pending_valid_{false};
  // a trailer we decided not to keep: swallow the rest of its runs rather than
  // recording half of it
  bool       skip_trailer_{false};

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

#endif // CORNET_HTTP_COMMON_PARSER_H
