#ifndef CORNET_HTTP_SERVER_H
#define CORNET_HTTP_SERVER_H

#include <memory>

#include "cornet/concurrency/scope.h"
#include "cornet/http/connection.h"
#include "cornet/http/router.h"
#include "cornet/http/timer_wheel.h"
#include "cornet/net/socket.h"

namespace cornet {
class runtime_t;
}

namespace cornet::http {

/**
 * @brief HTTP/1.1 server bound to one context.
 *
 * Routes are registered on the server (which forwards to its router), the listener
 * is opened with listen(), and serve() is spawned into the context.
 *
 * Shutdown is the server's own business rather than context_t::shutdown()'s. Two
 * things make that necessary. First, a listener's accept is permanently inflight
 * and counts as user work, so the context's drain phase can never observe
 * user_idle() and always waits out its full timeout. Second, the context's
 * cancellation sweep uses IORING_ASYNC_CANCEL_ANY, which reaps writes in flight
 * along with everything else — a response halfway to the client gets truncated.
 * So the sequence here is: stop accepting, close parked connections, let active
 * ones finish their current exchange (answering with Connection: close), and only
 * then let the context wind down.
 */
class server_t {
 public:
  enum class state_t : uint8_t { Running, Draining, Stopped };

  explicit server_t(context_t& ctx, server_options_t opt = {});
  ~server_t();

  server_t(const server_t&) = delete;
  server_t& operator=(const server_t&) = delete;

  // ── route registration, forwarded to the router ──

  template <typename F> route_t& get(std::string_view p, F&& fn) {
    return router_.get(p, std::forward<F>(fn));
  }
  template <typename F> route_t& post(std::string_view p, F&& fn) {
    return router_.post(p, std::forward<F>(fn));
  }
  template <typename F> route_t& put(std::string_view p, F&& fn) {
    return router_.put(p, std::forward<F>(fn));
  }
  template <typename F> route_t& del(std::string_view p, F&& fn) {
    return router_.del(p, std::forward<F>(fn));
  }
  template <typename F> route_t& patch(std::string_view p, F&& fn) {
    return router_.patch(p, std::forward<F>(fn));
  }
  template <typename F> route_t& head(std::string_view p, F&& fn) {
    return router_.head(p, std::forward<F>(fn));
  }
  template <typename F> route_t& options(std::string_view p, F&& fn) {
    return router_.options(p, std::forward<F>(fn));
  }
  template <typename F> route_t& route(method_t m, std::string_view p, F&& fn) {
    return router_.route(m, p, std::forward<F>(fn));
  }
  template <typename F> void fallback(F&& fn) { router_.fallback(std::forward<F>(fn)); }
  template <typename F> void filter(F&& fn) { router_.filter(std::forward<F>(fn)); }

  CORNET_NODISCARD router_t& router() { return router_; }
  CORNET_NODISCARD const router_t& router() const { return router_; }
  CORNET_NODISCARD const server_options_t& options() const { return opt_; }
  CORNET_NODISCARD const connection_metrics_t& metrics() const { return metrics_; }

  /**
   * @brief open the listening socket.
   */
  CORNET_NODISCARD expected<void> listen();
  CORNET_NODISCARD expected<void> listen(std::string_view address, uint16_t port);

  /**
   * @brief accept loop plus the timer wheel tick. Spawn this into the context.
   */
  CORNET_NODISCARD coro_t<void> serve();

  /**
   * @brief begin a graceful drain: stop accepting, finish what is in flight.
   * Safe to call from a signal handler registered with ctx.on_signal().
   */
  void drain();

  /**
   * @brief stop at once, cancelling connections without waiting.
   */
  void stop();

  CORNET_NODISCARD state_t state() const { return state_; }
  CORNET_NODISCARD uint32_t connections() const { return conns_; }

 private:
  CORNET_NODISCARD coro_t<void> accept_loop();
  CORNET_NODISCARD coro_t<void> serve_connection(tcp::socket_t sock);

  context_t&        ctx_;
  server_options_t  opt_;
  router_t          router_;
  timer_wheel_t     wheel_;
  buffer_pool_t&    pool_;
  connection_metrics_t metrics_{};

  // v4 concretely, not the base: cornet::socket_t has no virtual destructor, so
  // owning a derived socket through a base pointer would be undefined behaviour
  std::unique_ptr<tcp::v4::socket_t> listener_;
  // Connections live in a scope so that a drain can wait for every one of them to
  // finish; this is what guarantees no response is cut off mid-write.
  std::unique_ptr<scope_t> scope_;
  // active connections, so a drain can ask each to wind up
  std::vector<connection_t*> active_;

  state_t  state_{state_t::Stopped};
  uint32_t conns_{0};
};

/**
 * @brief run an HTTP server on every worker of a runtime.
 *
 * Each thread opens its own listener with SO_REUSEPORT and gets its own server,
 * so nothing is shared and no lock is needed. Routes are registered per thread by
 * the callback, which keeps the router immutable once serving starts.
 *
 * @param rt the runtime
 * @param opt server options; the port is shared, the listeners are not
 * @param configure called once per worker to register routes
 */
void serve(runtime_t& rt, server_options_t opt,
           const std::function<void(router_t&)>& configure);

} // namespace cornet::http

#endif // CORNET_HTTP_SERVER_H
