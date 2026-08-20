#ifndef CORNET_HTTP_CLIENT_POOL_H
#define CORNET_HTTP_CLIENT_POOL_H

#include <coroutine>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cornet/http/client/connection.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

/**
 * @brief resolved addresses with a ttl, so a request does not pay for a thread-pool
 * round trip when it does not have to.
 *
 * cornet::resolve() offloads getaddrinfo to the executor, which is right — it blocks —
 * but it also means every request would hop to another thread and back before the
 * first SQE. For a client talking to a handful of hosts that hop is pure latency.
 *
 * Keyed by host only, with the port patched into the copy that is handed out: the
 * address of a host does not depend on which port you talk to, and keying on the host
 * alone means the lookup can be done from a string_view without building a key.
 */
class dns_cache_t {
 public:
  dns_cache_t(context_t& ctx, const client_options_t& opt, client_metrics_t& metrics)
    : ctx_(ctx), opt_(opt), metrics_(metrics) {}

  /**
   * @brief resolve, from cache when possible.
   */
  CORNET_NODISCARD coro_t<expected<resolved_address>> resolve(std::string_view host,
                                                             uint16_t port);

  void clear() { entries_.clear(); }
  CORNET_NODISCARD uint32_t size() const { return uint32_t(entries_.size()); }

 private:
  struct entry_t {
    resolved_address addr{};
    uint64_t         expires_ns{0};
  };

  void store(std::string_view host, const resolved_address& addr);

  context_t&              ctx_;
  const client_options_t& opt_;
  client_metrics_t&       metrics_;
  // std::less<> so find() works on a string_view without building a key
  std::map<std::string, entry_t, std::less<>> entries_;
};

/**
 * @brief keep-alive connection pool, one per client, one thread.
 *
 * Connections are grouped by origin (host, port). An idle connection is reused if
 * there is one; otherwise a new one is opened, up to max_conns_per_host, after which
 * callers queue in arrival order until somebody hands one back or the wait times out.
 *
 * There is no health check beyond a non-blocking peek on reuse: an idle keep-alive
 * connection can be closed by the peer at any moment, and no amount of polling closes
 * that race. What closes it is the retry rule in client_t, and the peek just makes the
 * common case cheap.
 *
 * Single-threaded by construction: it belongs to one client on one context, so there
 * are no locks anywhere in here.
 */
class client_pool_t {
 public:
  client_pool_t(context_t& ctx, const client_options_t& opt, buffer_pool_t& bufs,
                timer_wheel_t& wheel, client_metrics_t& metrics, dns_cache_t& dns);
  ~client_pool_t();

  client_pool_t(const client_pool_t&) = delete;
  client_pool_t& operator=(const client_pool_t&) = delete;

  /**
   * @brief borrow a connection for this origin.
   * @param reused set to true when it came from the idle list, which is what decides
   *        whether a failure may be retried
   */
  CORNET_NODISCARD coro_t<expected<client_connection_t*>> acquire(std::string_view host,
                                                                  uint16_t port, scheme_t scheme,
                                                                  bool& reused);

  /**
   * @brief hand a connection back.
   * @param reusable false discards it; pass client_connection_t::reusable()
   */
  void release(client_connection_t* conn, bool reusable);

  /**
   * @brief close every connection and fail every waiter.
   */
  void clear();

  CORNET_NODISCARD uint32_t idle_count() const;
  CORNET_NODISCARD uint32_t busy_count() const;
  CORNET_NODISCARD uint32_t total_count() const { return idle_count() + busy_count(); }

 private:
  using conn_ptr = std::unique_ptr<client_connection_t>;

  struct waiter_t {
    std::coroutine_handle<> handle{};
    client_connection_t*    granted{nullptr};
    bool                    expired{false};
    timer_node_t            timer{};
    client_pool_t*          pool{nullptr};
    void*                   bucket{nullptr};
    // Being parked on a pool slot is user work, and there is no io to prove it: without
    // this token the run loop sees an idle context and returns while requests are still
    // queued, and a graceful drain starts early for the same reason.
    context_t::work_token_t work{};
  };

  struct bucket_t {
    std::string           host;
    uint16_t              port{0};
    std::vector<conn_ptr> idle;
    std::vector<conn_ptr> busy;
    std::vector<waiter_t*> waiters;
    // connections being opened right now. Counted separately because opening is a
    // suspension point: without it two coroutines would both see room and both open,
    // overshooting max_conns_per_host.
    uint32_t opening{0};
  };

  /**
   * @brief suspend until a connection for this bucket frees up, or the wait times out.
   */
  struct wait_awaiter {
    client_pool_t* pool;
    bucket_t*      bucket;
    waiter_t       waiter{};

    // Parked waiters sit in two intrusive lists that outlive the coroutine
    // frame; the destructor is the only thing that can pull one out when the
    // frame dies mid-wait.
    ~wait_awaiter();

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h);
    CORNET_NODISCARD expected<client_connection_t*> await_resume();
  };

  bucket_t& bucket_for(std::string_view host, uint16_t port, scheme_t scheme);
  void      arm_idle(bucket_t& bucket, client_connection_t& conn);
  void      drop_idle(client_connection_t& conn);
  static void on_idle_expired(void* owner);
  static void on_wait_expired(void* owner);
  void      grant_or_park(bucket_t& bucket, conn_ptr conn);
  conn_ptr  detach(std::vector<conn_ptr>& list, client_connection_t* conn);

  context_t&              ctx_;
  const client_options_t& opt_;
  buffer_pool_t&          bufs_;
  timer_wheel_t&          wheel_;
  client_metrics_t&       metrics_;
  dns_cache_t&            dns_;

  // origin(host) -> (port<<1)|https -> bucket; the outer map takes string_view
  // lookups without building a key. Scheme belongs in the key: a plain and a
  // TLS connection to the same port are different worlds and must never be
  // handed out for each other's requests.
  static constexpr uint32_t bucket_key(uint16_t port, scheme_t scheme) {
    return (uint32_t(port) << 1) | (scheme == scheme_t::Https ? 1u : 0u);
  }
  std::map<std::string, std::map<uint32_t, bucket_t>, std::less<>> origins_;
  uint32_t total_{0};
};

} // namespace cornet::http

#endif // CORNET_HTTP_CLIENT_POOL_H
