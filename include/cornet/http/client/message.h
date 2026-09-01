#ifndef CORNET_HTTP_CLIENT_MESSAGE_H
#define CORNET_HTTP_CLIENT_MESSAGE_H

#include <chrono>
#include <cstdint>
#include <string_view>

#include "cornet/coroutine/coro.h"
#include "cornet/http/common/buffer.h"
#include "cornet/http/common/headers.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/serializer.h"
#include "cornet/http/common/url.h"

namespace cornet::http {

class client_t;
class client_connection_t;

/**
 * @brief everything one inbound response needs, in a single pooled node.
 *
 * The client's central lifetime problem: `co_await send()` hands the connection
 * back to the pool, yet the caller still wants to read the body — and every view in
 * a parsed message points into the buffers it was parsed from. Copying the response
 * out would defeat the point of a zero-copy parser.
 *
 * So the receive state is parsed *into its final home* from the start. The
 * connection allocates one of these per exchange, parses into it, and hands the
 * pointer to client_response_t when the message completes; the connection then takes
 * a fresh node for its next request. Nothing moves, nothing is re-bound, and the
 * header offsets stay valid because the buffers they index never change hands
 * except as a whole.
 *
 * The node lives inside a pooled block, so a steady-state exchange asks the
 * allocator for nothing at all.
 */
struct inbound_t {
  head_buffer_t  head;    // status line, headers, and the body window behind them
  spill_buffer_t spill;   // values the parser could not keep as a view
  headers_t      headers; // offset entries into the two buffers above
  body_buffer_t  body;    // aggregated body, or the streaming window

  status_t  status{status_t::Ok};
  version_t version{version_t::Unknown};
  uint64_t  content_length{0};
  // stamped by the connection at handoff: the FINAL keep-alive decision,
  // which may have been downgraded after the headers were parsed
  bool      keep_alive{false};
  bool      chunked{false};
  bool      has_content_length{false};

  /**
   * @brief take a node from the pool, with a header buffer of `head_bytes`.
   * @return the node, or nullptr when the allocator is out
   */
  CORNET_NODISCARD static inbound_t* create(buffer_pool_t& pool, uint32_t head_bytes);

  /**
   * @brief run the destructor and return every block, including the node's own.
   */
  void destroy();

  inbound_t(const inbound_t&) = delete;
  inbound_t& operator=(const inbound_t&) = delete;

 private:
  inbound_t() = default;
  ~inbound_t() = default;

  // the block *this* lives in; released last, by destroy()
  buffer_lease_t self_{};
};

/**
 * @brief one inbound response, owning the buffers it was parsed from.
 *
 * Views stay valid for as long as this object does — that is the whole contract,
 * and the reason it is move-only: two owners would mean two returns to the pool.
 */
class client_response_t {
 public:
  client_response_t() = default;
  ~client_response_t() { reset(); }

  client_response_t(const client_response_t&) = delete;
  client_response_t& operator=(const client_response_t&) = delete;

  client_response_t(client_response_t&& o) noexcept : node_(o.node_) { o.node_ = nullptr; }
  client_response_t& operator=(client_response_t&& o) noexcept {
    if (this != &o) {
      reset();
      node_ = o.node_;
      o.node_ = nullptr;
    }
    return *this;
  }

  CORNET_NODISCARD bool valid() const { return node_ != nullptr; }
  explicit operator bool() const { return node_ != nullptr; }

  CORNET_NODISCARD status_t status() const;
  CORNET_NODISCARD uint16_t status_code() const { return uint16_t(status()); }

  /**
   * @brief whether the status is 2xx. Not an error channel — a 404 arrived fine.
   */
  CORNET_NODISCARD bool ok() const;

  CORNET_NODISCARD version_t version() const;
  CORNET_NODISCARD const headers_t& headers() const;

  /**
   * @brief the aggregated body. Empty on a streamed response, where the bytes went
   * to the caller's reader instead.
   */
  CORNET_NODISCARD std::string_view body() const;

  CORNET_NODISCARD std::string_view header(field_t f) const { return headers().get(f); }
  CORNET_NODISCARD std::string_view header(std::string_view name) const {
    return headers().get(name);
  }

  /**
   * @brief value of a chunked trailer. Empty unless client_options_t::max_trailers is
   * set, and only complete once the body has been read.
   */
  CORNET_NODISCARD std::string_view trailer(field_t f) const { return headers().trailer(f); }
  CORNET_NODISCARD std::string_view trailer(std::string_view name) const {
    return headers().trailer(name);
  }

  CORNET_NODISCARD bool has_content_length() const;
  CORNET_NODISCARD uint64_t content_length() const;
  CORNET_NODISCARD bool chunked() const;

  /**
   * @brief whether the peer agreed to keep the connection alive.
   */
  CORNET_NODISCARD bool keep_alive() const;

  /**
   * @brief drop the buffers early.
   */
  void reset();

 private:
  friend class client_connection_t;
  explicit client_response_t(inbound_t* node) : node_(node) {}

  inbound_t* node_{nullptr};
};

/**
 * @brief one outbound request, staged in pooled buffers.
 *
 * Built by a chain of small calls, like the server's response_t, and for the same
 * reason: there is exactly one useful reaction to "this did not fit", so errors
 * latch and are reported once by failed() instead of at every call.
 *
 * The body flavours mirror response_t exactly, because the question is the same one
 * — who owns these bytes until the kernel has read them:
 *
 *   body()        copies into a pooled buffer. Always safe, and replayable, which
 *                 is what makes an automatic retry possible. The default.
 *   body_static() references bytes of static storage duration. Zero copy.
 *   body_owned()  takes a pooled block, released once the request has been written.
 *
 * The url text is copied into a pooled block too, so the parsed views survive the
 * caller's temporary string — and so a retry or a redirect still has a url to work
 * from after the original argument is long gone.
 */
class client_request_t {
 public:
  client_request_t() = default;

  // Every owning member is a pooled lease that frees itself, so the compiler's
  // move is exactly right and there is nothing for a destructor to do.
  client_request_t(const client_request_t&) = delete;
  client_request_t& operator=(const client_request_t&) = delete;
  client_request_t(client_request_t&&) noexcept = default;
  client_request_t& operator=(client_request_t&&) noexcept = default;

  /**
   * @brief build a request that is not attached to a client.
   *
   * Used by tests and by anyone driving a client_connection_t directly; send() needs
   * a client and is unavailable on one of these.
   */
  CORNET_NODISCARD static expected<client_request_t> make(buffer_pool_t& pool, method_t m,
                                                          std::string_view url,
                                                          uint32_t hdr_bytes = 4u << 10,
                                                          client_t* owner = nullptr);

  // ── headers ──

  client_request_t& header(field_t f, std::string_view value);
  client_request_t& header(std::string_view name, std::string_view value);
  client_request_t& header(field_t f, uint64_t value);

  // ── body, by ownership ──

  client_request_t& body(std::string_view data);
  client_request_t& body_static(std::string_view data);
  client_request_t& body_owned(buffer_lease_t lease, uint32_t len);

  client_request_t& json(std::string_view data) {
    header(field_t::ContentType, "application/json");
    return body(data);
  }
  client_request_t& text(std::string_view data) {
    header(field_t::ContentType, "text/plain; charset=utf-8");
    return body(data);
  }

  // ── per-request overrides; unset means "use the client's option" ──

  /**
   * @brief deadline for the whole exchange, connect included.
   */
  client_request_t& timeout(std::chrono::milliseconds d) {
    timeout_ = d;
    has_timeout_ = true;
    return *this;
  }

  client_request_t& retry(uint8_t times) {
    max_retries_ = times;
    has_retries_ = true;
    return *this;
  }

  /**
   * @brief override the method's own idempotency for retry purposes.
   *
   * A POST that the caller knows is safe to replay (it carries an idempotency key,
   * say) can opt in; a GET with side effects can opt out.
   */
  client_request_t& idempotent(bool on) {
    idempotent_ = on;
    has_idempotent_ = true;
    return *this;
  }

  client_request_t& follow_redirects(uint8_t max) {
    max_redirects_ = max;
    has_redirects_ = true;
    return *this;
  }

  /**
   * @brief ask the peer for a 100-continue before sending the body.
   * Worth it only for a body large enough that a rejection saves real bandwidth.
   */
  client_request_t& expect_continue(bool on = true) {
    expect_continue_ = on;
    return *this;
  }

  // ── inspection ──

  CORNET_NODISCARD method_t method() const { return method_; }
  CORNET_NODISCARD const url_t& url() const { return url_; }

  /**
   * @brief the user header block exactly as it will go on the wire.
   */
  CORNET_NODISCARD std::string_view staged_headers() const { return hdr_.view(); }

  CORNET_NODISCARD body_source_t body_source() const { return source_; }
  CORNET_NODISCARD uint64_t body_length() const { return body_len_; }

  /**
   * @brief the body bytes, wherever they live. Empty for a streamed body.
   */
  CORNET_NODISCARD std::string_view body_view() const;

  CORNET_NODISCARD bool expects_continue() const { return expect_continue_; }

  /**
   * @brief whether the caller already wrote this framing header itself.
   * The head framer consults these so it never emits a second Host or
   * Content-Length; a duplicate is a framing error, not a cosmetic one.
   */
  CORNET_NODISCARD bool saw_host() const { return saw_host_; }
  CORNET_NODISCARD bool saw_content_length() const { return saw_content_length_; }
  CORNET_NODISCARD bool saw_connection() const { return saw_connection_; }
  CORNET_NODISCARD bool saw_transfer_encoding() const { return saw_transfer_encoding_; }
  CORNET_NODISCARD bool saw_user_agent() const { return saw_user_agent_; }
  CORNET_NODISCARD bool saw_accept() const { return saw_accept_; }
  CORNET_NODISCARD bool saw_expect() const { return saw_expect_; }
  CORNET_NODISCARD bool saw_authorization() const { return saw_authorization_; }

  CORNET_NODISCARD error_t error() const;
  CORNET_NODISCARD bool failed() const { return bool(error()); }
  void fail(error_t e) {
    if (!err_) err_ = e;
  }

  /**
   * @brief send through the client this request was created by.
   * @return the response, or the first error that stopped the exchange
   */
  CORNET_NODISCARD coro_t<expected<client_response_t>> send();

 private:
  friend class client_t;
  friend class client_connection_t;

  CORNET_NODISCARD expected<void> init(buffer_pool_t& pool, method_t m, std::string_view url,
                                       uint32_t hdr_bytes);
  /**
   * @brief point this request at another url, copying the text into the pooled
   * block it already owns. Used by redirect following.
   */
  CORNET_NODISCARD expected<void> retarget(std::string_view url);
  void note_framing_header(field_t f);
  CORNET_NODISCARD expected<void> ensure_body_buffer(uint32_t bytes);

  void set_method(method_t m) { method_ = m; }

  /**
   * @brief forget the body. Used when a 301/302/303 turns the request into a GET.
   */
  void drop_body();

  CORNET_NODISCARD std::string_view inline_body() const;
  CORNET_NODISCARD std::string_view static_body() const { return external_; }

  client_t*      owner_{nullptr};
  buffer_pool_t* pool_{nullptr};

  buffer_lease_t url_lease_{};
  uint32_t       url_len_{0};
  url_t          url_{};

  out_buffer_t hdr_{};       // user headers, verbatim
  out_buffer_t body_out_{};  // inline body bytes

  body_source_t source_{body_source_t::None};
  uint64_t      body_len_{0};
  std::string_view external_{};
  buffer_lease_t   owned_{};

  method_t method_{method_t::Get};

  std::chrono::milliseconds timeout_{0};
  uint8_t max_retries_{0};
  uint8_t max_redirects_{0};
  bool    idempotent_{false};
  bool    has_timeout_{false};
  bool    has_retries_{false};
  bool    has_redirects_{false};
  bool    has_idempotent_{false};
  bool    expect_continue_{false};

  // framing headers the caller wrote itself; the connection must not repeat them
  bool saw_host_{false};
  bool saw_content_length_{false};
  bool saw_connection_{false};
  bool saw_transfer_encoding_{false};
  bool saw_user_agent_{false};
  bool saw_accept_{false};
  bool saw_expect_{false};
  bool saw_authorization_{false};

  error_t err_{};
};

} // namespace cornet::http

#endif // CORNET_HTTP_CLIENT_MESSAGE_H
