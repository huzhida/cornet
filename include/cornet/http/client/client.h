#ifndef CORNET_HTTP_CLIENT_CLIENT_H
#define CORNET_HTTP_CLIENT_CLIENT_H

#include <chrono>
#include <cstdint>
#include <string_view>

#include "cornet/http/client/connection.h"
#include "cornet/http/client/message.h"
#include "cornet/http/client/pool.h"

namespace cornet::http {

class client_t;

/**
 * @brief a response whose body is still arriving.
 *
 * Returned by client_t::stream(). It borrows the connection until the body ends, which
 * is why it exists at all: a streamed body's views point into buffers the connection
 * still owns, so something has to keep the two together and hand the connection back
 * exactly once. Destroying a stream before the body is finished discards the
 * connection rather than reusing it — call drain() first to keep it.
 */
class client_stream_t {
 public:
  client_stream_t() = default;
  ~client_stream_t();

  client_stream_t(const client_stream_t&) = delete;
  client_stream_t& operator=(const client_stream_t&) = delete;
  client_stream_t(client_stream_t&& o) noexcept;
  client_stream_t& operator=(client_stream_t&& o) noexcept;

  CORNET_NODISCARD bool valid() const { return conn_ != nullptr; }
  explicit operator bool() const { return conn_ != nullptr; }

  CORNET_NODISCARD status_t status() const;
  CORNET_NODISCARD bool ok() const;
  CORNET_NODISCARD const headers_t& headers() const;
  CORNET_NODISCARD std::string_view header(field_t f) const { return headers().get(f); }

  /**
   * @brief next run of body bytes; an empty view means the body is complete.
   * The view is valid until the following read().
   */
  CORNET_NODISCARD coro_t<expected<std::string_view>> read();

  /**
   * @brief read and discard whatever is left, so the connection can be reused.
   */
  CORNET_NODISCARD coro_t<expected<void>> drain();

  /**
   * @brief hand the head back as a response object and return the connection.
   * The body is empty in it — those bytes went to read().
   */
  CORNET_NODISCARD expected<client_response_t> finish();

 private:
  friend class client_t;
  client_stream_t(client_t* owner, client_connection_t* conn) : owner_(owner), conn_(conn) {}

  void give_back();

  client_t*            owner_{nullptr};
  client_connection_t* conn_{nullptr};
};

/**
 * @brief a request whose body is being streamed out in chunks.
 *
 * Returned by client_t::upload(). Not retryable by construction — the bytes are gone as
 * soon as they are written, so there is nothing to replay.
 */
class client_upload_t {
 public:
  client_upload_t() = default;
  ~client_upload_t();

  client_upload_t(const client_upload_t&) = delete;
  client_upload_t& operator=(const client_upload_t&) = delete;
  client_upload_t(client_upload_t&& o) noexcept;
  client_upload_t& operator=(client_upload_t&& o) noexcept;

  CORNET_NODISCARD bool valid() const { return conn_ != nullptr; }
  explicit operator bool() const { return conn_ != nullptr; }

  /**
   * @brief send one chunk. The data is referenced, not copied, so it must stay put
   * until this returns.
   */
  CORNET_NODISCARD coro_t<expected<void>> write(std::string_view data);

  /**
   * @brief send the terminating chunk and read the response.
   */
  CORNET_NODISCARD coro_t<expected<client_response_t>> finish();

 private:
  friend class client_t;
  client_upload_t(client_t* owner, client_connection_t* conn, method_t method)
    : owner_(owner), conn_(conn), method_(method) {}

  void give_back();

  client_t*            owner_{nullptr};
  client_connection_t* conn_{nullptr};
  method_t             method_{method_t::Post};
};

/**
 * @brief HTTP/1.1 client bound to one context.
 *
 * Owns the connection pool, the dns cache, the timer wheel and the options; a request
 * is a few pooled buffers and one coroutine on top of that.
 *
 *   context_t ctx;
 *   http::client_t cli(ctx);
 *   ctx.spawn([&]() -> coro_t<void> {
 *     auto resp = co_await cli.get("http://127.0.0.1:8080/hello");
 *     if (resp) SPDLOG_INFO("{} {}", resp->status_code(), resp->body());
 *   }());
 *   ctx.run();
 *
 * One client belongs to one context and must not be shared across threads — the same
 * shared-nothing rule the rest of cornet follows. On a runtime_t, give each worker its
 * own client.
 *
 * A non-2xx status is not an error: it arrived, it parsed, and what to do about it is
 * the caller's business. The expected<> channel carries only the failures that
 * prevented an answer — dns, connect, timeout, protocol, buffer limits.
 */
class client_t {
 public:
  explicit client_t(context_t& ctx, client_options_t opt = {});
  ~client_t();

  client_t(const client_t&) = delete;
  client_t& operator=(const client_t&) = delete;

  // ── building requests ──

  /**
   * @brief start a request against this client.
   * A malformed url does not throw and does not need checking here: the returned
   * request carries the error, and send() reports it.
   */
  CORNET_NODISCARD client_request_t request(method_t m, std::string_view url);

  /**
   * @brief send a request built by request().
   */
  CORNET_NODISCARD coro_t<expected<client_response_t>> send(client_request_t& req);

  // ── one-liners ──

  CORNET_NODISCARD coro_t<expected<client_response_t>> get(std::string_view url);
  CORNET_NODISCARD coro_t<expected<client_response_t>> head(std::string_view url);
  CORNET_NODISCARD coro_t<expected<client_response_t>> del(std::string_view url);
  CORNET_NODISCARD coro_t<expected<client_response_t>> post(std::string_view url,
                                                            std::string_view body,
                                                            std::string_view content_type = {});
  CORNET_NODISCARD coro_t<expected<client_response_t>> put(std::string_view url,
                                                           std::string_view body,
                                                           std::string_view content_type = {});

  // ── streaming ──

  /**
   * @brief send a request and take the body run by run instead of all at once.
   */
  CORNET_NODISCARD coro_t<expected<client_stream_t>> stream(method_t m, std::string_view url);
  CORNET_NODISCARD coro_t<expected<client_stream_t>> stream(client_request_t& req);

  /**
   * @brief send a request whose body is written in chunks.
   * The request must not have a staged body; those two are different requests.
   */
  CORNET_NODISCARD coro_t<expected<client_upload_t>> upload(method_t m, std::string_view url);
  CORNET_NODISCARD coro_t<expected<client_upload_t>> upload(client_request_t& req);

  // ── plumbing ──

  CORNET_NODISCARD const client_options_t& options() const { return opt_; }
  CORNET_NODISCARD const client_metrics_t& metrics() const { return metrics_; }
  CORNET_NODISCARD client_pool_t& pool() { return pool_; }
  CORNET_NODISCARD dns_cache_t& dns() { return dns_; }
  CORNET_NODISCARD context_t& context() { return ctx_; }

  /**
   * @brief close every pooled connection and stop the timer wheel.
   * Called by the destructor; safe to call earlier when the client is done.
   */
  void close();

 private:
  friend class client_request_t;
  friend class client_stream_t;
  friend class client_upload_t;

  struct lease_t {
    client_connection_t* conn{nullptr};
    bool                 reused{false};
  };

  void ensure_wheel();
  CORNET_NODISCARD coro_t<expected<lease_t>> borrow(const url_t& url);
  void give_back(client_connection_t* conn, bool reusable);

  /**
   * @brief the total deadline for one send(), shared by every retry and redirect.
   */
  CORNET_NODISCARD std::chrono::milliseconds remaining(uint64_t deadline_ns) const;

  /**
   * @brief rewrite the request to follow a redirect.
   * @return false when this redirect must not be followed, in which case the 3xx is
   *         handed back to the caller unchanged
   */
  CORNET_NODISCARD bool follow(client_request_t& req, const client_response_t& resp);

  context_t&       ctx_;
  client_options_t opt_;
  buffer_pool_t&   bufs_;
  client_metrics_t metrics_{};
  timer_wheel_t    wheel_;
  dns_cache_t      dns_;
  client_pool_t    pool_;
  bool             wheel_started_{false};
};

} // namespace cornet::http

#endif // CORNET_HTTP_CLIENT_CLIENT_H
