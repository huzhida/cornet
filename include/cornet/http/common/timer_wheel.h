#ifndef CORNET_HTTP_COMMON_TIMER_WHEEL_H
#define CORNET_HTTP_COMMON_TIMER_WHEEL_H

#include <chrono>
#include <cstdint>

#include "cornet/coroutine/coro.h"

namespace cornet {
struct context_t;
}

namespace cornet::http {

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
 * @brief hashed timing wheel, one per context.
 *
 * The point of this class is what it replaces. Attaching an io_uring link_timeout
 * to every recv is elegant — it needs no timer thread and costs no extra syscall
 * — but it doubles the SQEs and CQEs the ring handles per request, and with ten
 * thousand connections the submission queue is the scarce resource. Idle,
 * header and body deadlines are all measured in seconds and tolerate coarse
 * expiry, so a wheel is the right shape: arming is O(1), and the whole context
 * keeps exactly one timeout SQE in flight no matter how many connections exist.
 *
 * Single-threaded per context: no locks.
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
   */
  void arm(timer_node_t& node, std::chrono::milliseconds delay);

  /**
   * @brief remove a node from the wheel. Safe on an unarmed node.
   */
  void cancel(timer_node_t& node);

  /**
   * @brief the ticking coroutine. Spawn once per context.
   * Marked as framework io, so it never keeps the context from draining.
   */
  CORNET_NODISCARD coro_t<void> run();

  /**
   * @brief stop ticking. The run() coroutine exits after the current tick.
   */
  void stop() { running_ = false; }

  CORNET_NODISCARD bool running() const { return running_; }
  CORNET_NODISCARD uint64_t ticks() const { return ticks_; }
  CORNET_NODISCARD uint32_t armed_count() const { return armed_; }

 private:
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
};

} // namespace cornet::http

#endif // CORNET_HTTP_COMMON_TIMER_WHEEL_H
