#ifndef CORNET_HTTP_ROUTER_H
#define CORNET_HTTP_ROUTER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cornet/coroutine/coro.h"
#include "cornet/http/common.h"
#include "cornet/http/message.h"

namespace cornet::http {

/**
 * @brief a handler that never suspends. Takes the request, writes the response,
 * returns.
 */
using sync_handler_t = std::function<void(request_t&, response_t&)>;

/**
 * @brief a handler that awaits something before answering.
 */
using async_handler_t = std::function<coro_t<void>(request_t&, response_t&)>;

/**
 * @brief a filter that never suspends. Return false to short-circuit: the
 * response written so far is sent and the handler does not run.
 */
using sync_filter_t = std::function<bool(request_t&, response_t&)>;

/**
 * @brief a filter that awaits. Same short-circuit contract.
 */
using async_filter_t = std::function<coro_t<bool>(request_t&, response_t&)>;

/**
 * @brief how the body of a matched route should be delivered.
 */
enum class body_policy_t : uint8_t {
  Auto,       // aggregate when Content-Length is known and small, else stream
  Aggregate,  // always aggregate, up to max_body_bytes
  Stream,     // always stream; the handler runs as soon as headers are parsed
};

/**
 * @brief one route's target.
 *
 * Handlers come in a synchronous and an asynchronous flavour, and the
 * synchronous one is not a convenience wrapper — it is the fast path. A handler
 * that returns a coroutine costs a frame allocation per request even when it
 * never suspends, and most handlers never do: they read memory, format a
 * response, and return. Keeping the two apart lets that majority run with no
 * allocation, no suspension and no trip through the scheduler, and as a side
 * effect removes the `co_return;` that a coroutine-only API forces on every
 * trivial handler.
 */
struct route_t {
  enum class kind_t : uint8_t { Sync, Async };

  kind_t          kind{kind_t::Sync};
  body_policy_t   body{body_policy_t::Auto};
  sync_handler_t  sync_fn{};
  async_handler_t async_fn{};

  // parameter names for this route, in the order the path declares them
  std::vector<std::string> param_names{};

  CORNET_NODISCARD bool valid() const {
    return kind == kind_t::Sync ? bool(sync_fn) : bool(async_fn);
  }
};

/**
 * @brief result of a match attempt.
 */
struct match_t {
  const route_t* route{nullptr};
  // set when the path matched but no route accepts this method
  bool method_mismatch{false};

  explicit operator bool() const { return route != nullptr; }
};

/**
 * @brief a filter in the chain, in either flavour.
 */
struct filter_entry_t {
  enum class kind_t : uint8_t { Sync, Async };
  kind_t         kind{kind_t::Sync};
  sync_filter_t  sync_fn{};
  async_filter_t async_fn{};
};

/**
 * @brief request router: exact paths in a flat hash table, parameterised paths in
 * a radix trie.
 *
 * Everything is allocated while routes are being registered. match() is const and
 * has no side effects, so every worker thread can share one instance with no lock
 * and no copy.
 */
class router_t {
 public:
  router_t();
  ~router_t();

  router_t(const router_t&) = delete;
  router_t& operator=(const router_t&) = delete;
  router_t(router_t&&) noexcept;
  router_t& operator=(router_t&&) noexcept;

  // ── registration ──

  /**
   * @brief register a handler. Accepts either flavour; which one is deduced from
   * the callable's return type, so the caller writes a plain lambda either way.
   */
  template <typename F>
  route_t& route(method_t m, std::string_view path, F&& fn) {
    using R = std::invoke_result_t<F&, request_t&, response_t&>;
    route_t r;
    if constexpr (std::is_same_v<R, void>) {
      r.kind = route_t::kind_t::Sync;
      r.sync_fn = std::forward<F>(fn);
    } else {
      static_assert(std::is_same_v<R, coro_t<void>>,
                    "an http handler returns void (synchronous) or coro_t<void> (asynchronous)");
      r.kind = route_t::kind_t::Async;
      r.async_fn = std::forward<F>(fn);
    }
    return add(m, path, std::move(r));
  }

  template <typename F> route_t& get(std::string_view p, F&& fn) {
    return route(method_t::Get, p, std::forward<F>(fn));
  }
  template <typename F> route_t& post(std::string_view p, F&& fn) {
    return route(method_t::Post, p, std::forward<F>(fn));
  }
  template <typename F> route_t& put(std::string_view p, F&& fn) {
    return route(method_t::Put, p, std::forward<F>(fn));
  }
  template <typename F> route_t& del(std::string_view p, F&& fn) {
    return route(method_t::Delete, p, std::forward<F>(fn));
  }
  template <typename F> route_t& patch(std::string_view p, F&& fn) {
    return route(method_t::Patch, p, std::forward<F>(fn));
  }
  template <typename F> route_t& head(std::string_view p, F&& fn) {
    return route(method_t::Head, p, std::forward<F>(fn));
  }
  template <typename F> route_t& options(std::string_view p, F&& fn) {
    return route(method_t::Options, p, std::forward<F>(fn));
  }

  /**
   * @brief handler for requests that match no route.
   */
  template <typename F>
  void fallback(F&& fn) {
    using R = std::invoke_result_t<F&, request_t&, response_t&>;
    if constexpr (std::is_same_v<R, void>) {
      fallback_.kind = route_t::kind_t::Sync;
      fallback_.sync_fn = std::forward<F>(fn);
    } else {
      fallback_.kind = route_t::kind_t::Async;
      fallback_.async_fn = std::forward<F>(fn);
    }
  }

  /**
   * @brief add a filter, run in registration order before the handler.
   */
  template <typename F>
  void filter(F&& fn) {
    using R = std::invoke_result_t<F&, request_t&, response_t&>;
    filter_entry_t e;
    if constexpr (std::is_same_v<R, bool>) {
      e.kind = filter_entry_t::kind_t::Sync;
      e.sync_fn = std::forward<F>(fn);
    } else {
      static_assert(std::is_same_v<R, coro_t<bool>>,
                    "an http filter returns bool (synchronous) or coro_t<bool> (asynchronous)");
      e.kind = filter_entry_t::kind_t::Async;
      e.async_fn = std::forward<F>(fn);
    }
    filters_.push_back(std::move(e));
  }

  // ── lookup ──

  /**
   * @brief find the route for a request.
   * @param m method
   * @param path path with the query already stripped
   * @param out receives any path parameters
   */
  CORNET_NODISCARD match_t match(method_t m, std::string_view path, param_slots_t& out) const;

  CORNET_NODISCARD const route_t* fallback_route() const {
    return fallback_.valid() ? &fallback_ : nullptr;
  }

  CORNET_NODISCARD const std::vector<filter_entry_t>& filters() const { return filters_; }
  CORNET_NODISCARD bool has_filters() const { return !filters_.empty(); }

  CORNET_NODISCARD uint32_t size() const { return count_; }

 private:
  route_t& add(method_t m, std::string_view path, route_t route);

  struct impl_t;
  std::unique_ptr<impl_t> impl_;
  std::vector<filter_entry_t> filters_;
  route_t  fallback_{};
  uint32_t count_{0};
};

} // namespace cornet::http

#endif // CORNET_HTTP_ROUTER_H
