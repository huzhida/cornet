#ifndef CORNET_CONCURRENCY_TIMER_WHEEL_H
#define CORNET_CONCURRENCY_TIMER_WHEEL_H

#include <chrono>
#include <cstdint>
#include <utility>

#include "cornet/coroutine/coro.h"

namespace cornet {

struct context_t;
class timer_wheel_t;

/**
 * @brief a timer, embedded in whatever it belongs to.
 *
 * Intrusive so that arming, re-arming and cancelling are pointer swaps with no
 * allocation — a keep-alive connection re-arms its idle timer on every single
 * request.
 */
struct timer_node_t {
  timer_node_t* prev{nullptr};
  timer_node_t* next{nullptr};

  void* owner{nullptr};
  void (*on_expire)(void* owner){nullptr};

  // full wheel revolutions still to wait through, for deadlines beyond the
  // wheel's span
  uint32_t rounds{0};
  // slot index, or -1 when not armed
  int32_t  slot{-1};

  CORNET_NODISCARD bool armed() const { return slot >= 0; }
};

/**
 * @brief hashed timing wheel, one per user (a context, a server, a pool...).
 *
 * The point of this class is what it replaces. Attaching an io_uring link_timeout
 * to every op is elegant — it needs no timer thread and costs no extra syscall
 * — but it doubles the SQEs and CQEs the ring handles per timed operation, and
 * under mass concurrency the submission queue is the scarce resource. Coarse
 * deadlines (idle timeouts, umbrella coroutine deadlines: anything measured in
 * ticks, not microseconds) are what a wheel is shaped for: arming is O(1), and
 * the whole wheel keeps at most one timeout SQE in flight no matter how many
 * timers are armed — none at all while nothing is armed (the run() coroutine
 * parks and is kicked awake by the next arm()).
 *
 * Expiry is quantized to the tick: a deadline of D fires within [D, D+tick).
 * Users needing sub-tick precision should use an io_uring link_timeout instead.
 *
 * Single-threaded: no locks. The owning loop and all arm/cancel/stop calls must
 * run on the same thread.
 */
class timer_wheel_t {
 public:
  static constexpr uint32_t kSlots = 512;

  timer_wheel_t(context_t& ctx, std::chrono::milliseconds tick);
  ~timer_wheel_t();

  timer_wheel_t(const timer_wheel_t&) = delete;
  timer_wheel_t& operator=(const timer_wheel_t&) = delete;

  /**
   * @brief arm (or re-arm) a node to fire after `delay`.
   * Re-arming an armed node moves it; there is no need to cancel first.
   * Kicks a parked run() coroutine back into ticking.
   */
  void arm(timer_node_t& node, std::chrono::milliseconds delay);

  /**
   * @brief remove a node from the wheel. Safe on an unarmed node.
   */
  void cancel(timer_node_t& node);

  /**
   * @brief the ticking coroutine. Spawn once per context.
   * Ticks only while timers are armed; parks otherwise (see the wheel comment).
   * Marked as framework io, so it never keeps the context from draining.
   */
  CORNET_NODISCARD coro_t<void> run();

  /**
   * @brief stop ticking and wake a parked run() coroutine so it can exit.
   * MUST be called (and the run() coroutine allowed to finish) before the
   * wheel is destroyed — a parked runner still references the wheel.
   */
  void stop() {
    running_ = false;
    kick();
  }

  CORNET_NODISCARD bool running() const { return running_; }
  CORNET_NODISCARD uint64_t ticks() const { return ticks_; }
  CORNET_NODISCARD uint32_t armed_count() const { return armed_; }

 private:
  /**
   * @brief parks the run() coroutine while nothing is armed. arm()/stop()
   * resume it by spawning the handle stashed here.
   */
  struct park_awaiter {
    timer_wheel_t& wheel_;
    bool await_ready() const noexcept { return false; }
    CORNET_MAYBE_UNUSED bool await_suspend(std::coroutine_handle<> h) noexcept {
      wheel_.parked_runner_ = h;
      return true;  // stay parked
    }
    void await_resume() noexcept {}
  };

  /**
   * @brief wake a parked run() coroutine (no-op while it is ticking).
   * Defined out of line: it needs the complete context_t.
   */
  void kick();

  void link(timer_node_t& node, uint32_t slot);
  void unlink(timer_node_t& node);
  void advance();

  context_t& ctx_;
  std::chrono::milliseconds tick_;
  timer_node_t* slots_[kSlots]{};
  uint32_t cursor_{0};
  uint32_t armed_{0};
  uint64_t ticks_{0};
  bool     running_{true};
  std::coroutine_handle<> parked_runner_{nullptr};
};

} // namespace cornet

#endif // CORNET_CONCURRENCY_TIMER_WHEEL_H
