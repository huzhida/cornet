#ifndef CORNET_SINGLEFLIGHT_H
#define CORNET_SINGLEFLIGHT_H

#include <coroutine>
#include <memory>
#include <unordered_map>
#include <utility>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/coroutine/coro.h"
#include "cornet/scheduling/context.h"

namespace cornet {

/**
 * @brief coalesces duplicate in-flight executions of the same coroutine work.
 *
 * When N coroutines want the same slow thing — a DNS answer, a fetched
 * schema, a refreshed cert, a computed aggregate — the first caller runs it
 * and the rest share its in-flight result instead of stampeding the upstream.
 * This is merging, not caching: the flight is forgotten the moment it lands,
 * so the next run after completion executes again. TTL, invalidation and
 * eviction explicitly do not belong here; layer them on top with a map.
 *
 * Followers that die waiting (frame destroyed) unlink silently; a leader that
 * dies mid-flight fails its followers with EOWNERDEAD — retrying makes the
 * next caller the new leader.
 *
 * Result sharing is by shared_ptr: each caller's copy pins the value's
 * lifetime independently, with no arena coupling.
 */
template <typename K, typename V, typename Hash = std::hash<K>>
class singleflight_t {
 public:
  using key_t = K;
  using value_t = V;
  using result_t = std::shared_ptr<V>;

  explicit singleflight_t(context_t& ctx) : ctx_(ctx) {}

  singleflight_t(const singleflight_t&) = delete;
  singleflight_t& operator=(const singleflight_t&) = delete;

  CORNET_NODISCARD size_t in_flight() const { return in_flight_.size(); }

  /**
   * @brief run fn() for key, sharing one in-flight execution among callers.
   *
   * The first caller for a key becomes the leader and co_awaits fn() (which
   * must return coro_t<expected<std::shared_ptr<V>>> — or ccoro_t, cancelled
   * propagation works through either). Callers arriving while it runs
   * suspend on the flight and wake with a copy of the same result: the same
   * shared_ptr on success, the same error on failure.
   */
  template <typename F>
  CORNET_MAYBE_UNUSED CORNET_NODISCARD coro_t<expected<result_t>> run(const K& key, F&& fn) {
    if (auto it = in_flight_.find(key); it != in_flight_.end()) {
      follower_awaiter follower{*this, it->second};
      co_return co_await follower;
    }

    auto* flight = new flight_t();
    auto [it, inserted] = in_flight_.emplace(key, flight);
    CORNET_ASSERT(inserted, "find() missed but emplace() refused: single-threaded map");

    // A leader killed mid-flight (frame destroyed — exception unwinds through
    // the coroutine machinery the same way) must not strand its followers:
    // the guard converts a missing completion into an EOWNERDEAD broadcast.
    struct leader_guard_t {
      singleflight_t* self;
      flight_t* flight;
      const K* key;
      bool completed{false};
      ~leader_guard_t() {
        if (!completed) {
          flight->err = error_t{EOWNERDEAD, error_domain::System};
          self->leader_finish(flight, *key);
        }
      }
    } guard{this, flight, &key};

    auto res = co_await fn();
    if (res) {
      // copy, not move: the leader's own return value must survive being
      // shared with every follower
      flight->result = *res;
    } else {
      flight->err = res.error();
    }
    guard.completed = true;
    leader_finish(flight, key);
    co_return res;
  }

 private:
  struct flight_t {
    struct waiter_t {
      waiter_t* next{nullptr};
      std::coroutine_handle<> handle{};
      bool linked{false};   // still on the flight's waiter chain
    };
    result_t result{};
    error_t  err{};
    waiter_t* waiters{nullptr};
    // followers holding a reference; the flight dies when the last one
    // releases after completion
    uint32_t waiter_n{0};
    bool done{false};
  };

  // followers park on this; unlink/release-on-destroy handles every exit path
  struct follower_awaiter {
    singleflight_t& self;
    flight_t* flight;
    typename flight_t::waiter_t node{};

    follower_awaiter(singleflight_t& s, flight_t* f) : self(s), flight(f) {}
    ~follower_awaiter() {
      if (node.linked) self.unlink_waiter(flight, &node);
      self.release_waiter(flight);
    }

    follower_awaiter(const follower_awaiter&) = delete;
    follower_awaiter& operator=(const follower_awaiter&) = delete;

    bool await_ready() { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
      node.handle = h;
      node.next = flight->waiters;
      flight->waiters = &node;
      node.linked = true;
      ++flight->waiter_n;
      return true;
    }

    expected<result_t> await_resume() {
      if (flight->err.code != 0) return unexpected(flight->err.code, flight->err.domain);
      return flight->result;
    }
  };

  void leader_finish(flight_t* flight, const K& key) {
    // New runs must see a fresh map before any follower wakes
    in_flight_.erase(key);
    flight->done = true;
    auto* w = flight->waiters;
    flight->waiters = nullptr;
    while (w) {
      auto* nxt = w->next;
      w->linked = false;
      // wake through the ready queue only — never resume in the leader's frame
      ctx_.scheduler().schedule(w->handle);
      w = nxt;
    }
    if (flight->waiter_n == 0) delete flight;
  }

  void unlink_waiter(flight_t* flight, typename flight_t::waiter_t* w) {
    for (auto** p = &flight->waiters; *p; p = &(*p)->next) {
      if (*p == w) {
        *p = w->next;
        w->next = nullptr;
        w->linked = false;
        return;
      }
    }
  }

  // every follower path funnels here exactly once (the awaiter's dtor)
  void release_waiter(flight_t* flight) {
    if (--flight->waiter_n == 0 && flight->done) delete flight;
  }

  context_t& ctx_;
  std::unordered_map<K, flight_t*, Hash> in_flight_;
};

} // namespace cornet

#endif // CORNET_SINGLEFLIGHT_H
