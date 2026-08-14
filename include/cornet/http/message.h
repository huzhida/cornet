#ifndef CORNET_HTTP_MESSAGE_H
#define CORNET_HTTP_MESSAGE_H

#include <cstdint>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cornet/coroutine/coro.h"
#include "cornet/http/buffer.h"
#include "cornet/http/common.h"
#include "cornet/http/headers.h"
#include "cornet/http/parser.h"
#include "cornet/http/serializer.h"

namespace cornet::http {

class connection_t;

/**
 * @brief route parameter slots, filled by the router into caller-owned storage.
 * Eight is well past what real routes use, and keeping them inline avoids an
 * allocation per request.
 */
struct param_slots_t {
  static constexpr uint32_t kMax = 8;

  struct slot_t {
    std::string_view name;
    std::string_view value;
  };

  slot_t   slots[kMax]{};
  uint32_t count{0};

  void clear() { count = 0; }

  bool add(std::string_view name, std::string_view value) {
    if (count >= kMax) return false;
    slots[count++] = {name, value};
    return true;
  }

  CORNET_NODISCARD std::string_view get(std::string_view name) const {
    for (uint32_t i = 0; i < count; ++i) {
      if (slots[i].name == name) return slots[i].value;
    }
    return {};
  }
};

/**
 * @brief lazy query-string view. Nothing is parsed until it is asked for, and
 * values are handed back raw (no percent-decoding, which would need to allocate).
 */
class query_t {
 public:
  query_t() = default;
  explicit query_t(std::string_view raw) : raw_(raw) {}

  CORNET_NODISCARD std::string_view raw() const { return raw_; }
  CORNET_NODISCARD bool empty() const { return raw_.empty(); }

  /**
   * @brief first value for a key, or empty.
   */
  CORNET_NODISCARD std::string_view get(std::string_view key) const;

  struct entry_t {
    std::string_view key;
    std::string_view value;
  };

  class iterator {
   public:
    iterator(std::string_view raw, size_t pos) : raw_(raw), pos_(pos) { advance(); }
    entry_t operator*() const { return cur_; }
    iterator& operator++() { pos_ = next_; advance(); return *this; }
    bool operator!=(const iterator& o) const { return pos_ != o.pos_; }
   private:
    void advance();
    std::string_view raw_;
    size_t  pos_{0};
    size_t  next_{0};
    entry_t cur_{};
  };

  CORNET_NODISCARD iterator begin() const { return {raw_, 0}; }
  CORNET_NODISCARD iterator end() const { return {raw_, raw_.size() + 1}; }

 private:
  std::string_view raw_;
};

/**
 * @brief streaming body reader, handed to handlers on streaming routes.
 *
 * Each read() yields the next run of body bytes as a view into the receive
 * buffer — no copy — valid until the following read(). An empty view means the
 * body is complete.
 */
class body_reader_t {
 public:
  explicit body_reader_t(connection_t& conn) : conn_(&conn) {}

  /**
   * @brief next run of body bytes.
   * @return view valid until the next read(); empty view at end of body
   */
  CORNET_NODISCARD coro_t<expected<std::string_view>> read();

  /**
   * @brief consume and discard whatever is left of the body.
   * A keep-alive connection cannot be reused until the body has been read, so the
   * connection runs this for handlers that return early.
   */
  CORNET_NODISCARD coro_t<expected<void>> drain();

  CORNET_NODISCARD bool complete() const { return complete_; }
  void mark_complete() { complete_ = true; }

 private:
  connection_t* conn_;
  bool complete_{false};
};

/**
 * @brief one inbound request. Every view points into connection-owned buffers and
 * remains valid until the handler returns.
 */
class request_t {
 public:
  request_t() = default;

  void bind(const parser_t& parser, const headers_t& headers, param_slots_t& params) {
    parser_ = &parser;
    headers_ = &headers;
    params_ = &params;
  }

  void set_body(std::string_view body) { body_ = body; }
  void set_reader(body_reader_t* reader) { reader_ = reader; }

  void reset() {
    body_ = {};
    reader_ = nullptr;
    path_cached_ = false;
    path_cache_ = {};
  }

  CORNET_NODISCARD method_t  method() const { return parser_->method(); }
  CORNET_NODISCARD version_t version() const { return parser_->version(); }
  CORNET_NODISCARD bool      keep_alive() const { return parser_->keep_alive(); }

  /**
   * @brief the raw request-target, query string included.
   */
  CORNET_NODISCARD std::string_view target() const { return parser_->target(); }

  /**
   * @brief the path portion of the target, query stripped.
   * Percent-escapes are left intact; decoding would have to allocate and most
   * routes never look.
   */
  CORNET_NODISCARD std::string_view path() const;

  CORNET_NODISCARD query_t query() const;

  CORNET_NODISCARD const headers_t& headers() const { return *headers_; }

  /**
   * @brief a route parameter, e.g. param("id") for "/users/:id".
   */
  CORNET_NODISCARD std::string_view param(std::string_view name) const {
    return params_ ? params_->get(name) : std::string_view{};
  }

  /**
   * @brief the aggregated body, contiguous even for a chunked request.
   * Empty on streaming routes; use stream() there.
   */
  CORNET_NODISCARD std::string_view body() const { return body_; }

  /**
   * @brief the streaming reader, or nullptr when the body was aggregated.
   */
  CORNET_NODISCARD body_reader_t* stream() const { return reader_; }

  CORNET_NODISCARD bool     has_content_length() const { return parser_->has_content_length(); }
  CORNET_NODISCARD uint64_t content_length() const { return parser_->content_length(); }
  CORNET_NODISCARD bool     chunked() const { return parser_->chunked(); }

 private:
  const parser_t*  parser_{nullptr};
  const headers_t* headers_{nullptr};
  param_slots_t*   params_{nullptr};
  body_reader_t*   reader_{nullptr};
  std::string_view body_{};
  mutable std::string_view path_cache_{};
  mutable bool     path_cached_{false};
};

/**
 * @brief where a response body lives.
 */
enum class body_source_t : uint8_t {
  None,      // no body
  Inline,    // copied into the connection's body output buffer
  External,  // referenced in place (static storage, or a block we own)
};

/**
 * @brief the response being built for one request.
 *
 * Body writes come in four flavours because "who owns these bytes until the write
 * completes" cannot be left implicit. Responses flush *after* the handler
 * returns — several pipelined responses share one writev — so a view into a
 * handler local is already dangling when the kernel reads it, and nothing at the
 * call site hints at that. So the API makes the choice explicit:
 *
 *   body()        copies into the output buffer. Always safe; the default.
 *   body_static() references bytes of static storage duration. Zero copy.
 *   body_owned()  takes a pooled block, released once the response is written.
 *   pin()         moves an object into the response arena, so a value computed
 *                 inside the handler can be referenced safely.
 *
 * Errors latch instead of being returned per call. A response is written by a
 * chain of small calls, and there is exactly one useful reaction to "it did not
 * fit" — the connection turns it into a 500 — so checking each call would add
 * noise without adding a decision. failed() reports it once, at the end.
 *
 * Ordering note: the status line and framework headers can only be written once
 * the body length is known, i.e. after the handler returns. User headers are
 * therefore staged in their own buffer and stitched together by writev, so
 * nothing has to be copied or rewritten to get the wire order right.
 */
class response_t {
 public:
  response_t() = default;
  ~response_t() { release_owned(); }

  response_t(const response_t&) = delete;
  response_t& operator=(const response_t&) = delete;

  /**
   * @brief bind the connection's staging buffers.
   * @param hdr  user headers are appended here
   * @param body inline body bytes are appended here
   */
  void bind(out_buffer_t& hdr, out_buffer_t& body) {
    hdr_ = &hdr;
    body_out_ = &body;
  }

  /**
   * @brief start a fresh response, recording where its staging regions begin.
   */
  void begin();

  // ── status and headers ──

  response_t& status(status_t s) {
    status_ = s;
    return *this;
  }
  CORNET_NODISCARD status_t status() const { return status_; }

  response_t& header(field_t f, std::string_view value);
  response_t& header(std::string_view name, std::string_view value);
  response_t& header(field_t f, uint64_t value);

  // ── body, by ownership ──

  /**
   * @brief copy the bytes out. Safe with any lifetime; the default choice.
   */
  response_t& body(std::string_view data);

  /**
   * @brief reference bytes that outlive the connection — string literals, static
   * tables, mapped files. Nothing is copied.
   */
  response_t& body_static(std::string_view data);

  /**
   * @brief hand over a pooled block, released once the response is written.
   */
  response_t& body_owned(buffer_lease_t lease, uint32_t len);

  /**
   * @brief move an object into the response arena and get a stable reference.
   *
   * Lets a handler build a value and reference it without copying:
   *   auto& s = resp.pin(render_json(user));
   *   resp.body_static(s);
   * The arena lives until the response has been written.
   */
  template <typename T>
  T& pin(T&& value) {
    using U = std::decay_t<T>;
    auto* slot = new U(std::forward<T>(value));
    pinned_.push_back(pinned_t{slot, [](void* p) { delete static_cast<U*>(p); }});
    return *slot;
  }

  // ── convenience, covering the common cases ──

  response_t& text(std::string_view s) {
    header(field_t::ContentType, "text/plain; charset=utf-8");
    return body(s);
  }

  response_t& json(std::string_view s) {
    header(field_t::ContentType, "application/json");
    return body(s);
  }

  response_t& html(std::string_view s) {
    header(field_t::ContentType, "text/html; charset=utf-8");
    return body(s);
  }

  response_t& not_found() {
    status(status_t::NotFound);
    return text("Not Found");
  }

  response_t& redirect(std::string_view location, status_t s = status_t::Found) {
    status(s);
    return header(field_t::Location, location);
  }

  // ── error latch ──

  CORNET_NODISCARD error_t error() const;
  CORNET_NODISCARD bool failed() const { return bool(error()); }
  void fail(error_t e) { if (!err_) err_ = e; }

  // ── inspected by connection_t while framing ──

  CORNET_NODISCARD body_source_t body_source() const { return source_; }
  CORNET_NODISCARD uint64_t body_length() const { return body_len_; }
  CORNET_NODISCARD std::string_view external_body() const { return external_; }
  CORNET_NODISCARD uint32_t inline_body_offset() const { return body_off_; }
  CORNET_NODISCARD uint32_t hdr_offset() const { return hdr_off_; }
  CORNET_NODISCARD uint32_t hdr_length() const;

  /**
   * @brief whether the handler already emitted this framing header itself.
   * The connection consults these so it never writes a second Content-Length or
   * Connection — duplicates are a framing error, not a cosmetic one.
   */
  CORNET_NODISCARD bool saw_content_length() const { return saw_content_length_; }
  CORNET_NODISCARD bool saw_connection() const { return saw_connection_; }
  CORNET_NODISCARD bool saw_date() const { return saw_date_; }
  CORNET_NODISCARD bool saw_transfer_encoding() const { return saw_transfer_encoding_; }

  /**
   * @brief close the user-header block with the terminating CRLF.
   * Called once the handler has returned and before the head is framed.
   */
  void seal_headers();

  /**
   * @brief take ownership of the body block, if any, for the flush to hold.
   */
  buffer_lease_t take_owned() { return std::move(owned_); }

  /**
   * @brief drop pinned objects and any owned block.
   */
  void release_owned();

 private:
  struct pinned_t {
    void* ptr;
    void (*destroy)(void*);
  };

  void note_framing_header(field_t f);

  out_buffer_t* hdr_{nullptr};
  out_buffer_t* body_out_{nullptr};

  status_t      status_{status_t::Ok};
  body_source_t source_{body_source_t::None};

  uint32_t hdr_off_{0};
  uint32_t hdr_end_{0};
  uint32_t body_off_{0};
  uint64_t body_len_{0};

  std::string_view external_{};
  buffer_lease_t   owned_{};

  bool saw_content_length_{false};
  bool saw_connection_{false};
  bool saw_date_{false};
  bool saw_transfer_encoding_{false};

  std::vector<pinned_t> pinned_{};
  error_t err_{};
};

} // namespace cornet::http

#endif // CORNET_HTTP_MESSAGE_H
