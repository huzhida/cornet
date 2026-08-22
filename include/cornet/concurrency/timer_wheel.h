#ifndef CORNET_CONCURRENCY_TIMER_WHEEL_H
#define CORNET_CONCURRENCY_TIMER_WHEEL_H

#include <chrono>
#include <cstdint>
#include <memory>
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
 * timers are armed — none at all while nothing is armed (the runner exits and the
 * next arm() spawns a fresh one).
 *
 * Expiry is quantized to the tick: a deadline of D fires within [D, D+tick).
 * Users needing sub-tick precision should use an io_uring link_timeout instead.
 *
 * Single-threaded: no locks. The owning loop and all arm/cancel/stop calls must
 * run on the same thread.
 *
 * One wheel per (context, tick): ask context_t::wheel_for() rather than building
 * your own, so a context with a server, three clients and a websocket session
 * still runs one coarse wheel and one fine one instead of five.
 *
 * Nothing idle is kept around at either level. With no timer armed the runner
 * returns instead of parking, so its frame goes back to the pool and the wheel
 * holds no io; and once the last owner drops its shared_ptr (the context's
 * registry only keeps a weak one), the wheel itself goes too — a client that
 * came and went leaves no 4KB of slots behind.
 *
 * Always heap-owned, via make(): the runner co-owns the wheel through a
 * shared_ptr, so the wheel cannot be freed under a runner that is asleep on its
 * tick. That is not a hypothetical — a wheel embedded in a coroutine frame (an
 * http client_t living in one, say) dies the moment that frame does, while the
 * runner still has a timeout in flight on the ring and will read the wheel again
 * when it completes. Owners hold a shared_ptr, users keep taking a reference.
 */
class timer_wheel_t : public std::enable_shared_from_this<timer_wheel_t> {
  /**
   * @brief passkey, so the only route to a wheel is make() (see the class note
   * on why a stack- or frame-embedded wheel is unsafe).
   */
  struct private_tag_t {
    explicit private_tag_t() = default;
  };

 public:
  static constexpr uint32_t kSlots = 512;
  // fallback for a nonsensical tick; also the key context_t files such a wheel under
  static constexpr std::chrono::milliseconds kDefaultTick{500};

  timer_wheel_t(private_tag_t, context_t& ctx, std::chrono::milliseconds tick);
  ~timer_wheel_t();

  /**
   * @brief create a standalone wheel. Prefer context_t::wheel_for(), which shares
   * one wheel per tick across the whole context. The runner starts itself on the
   * first arm(), so a wheel nobody arms costs nothing but its slots.
   */
  static std::shared_ptr<timer_wheel_t> make(context_t& ctx, std::chrono::milliseconds tick) {
    return std::make_shared<timer_wheel_t>(private_tag_t{}, ctx, tick);
  }

  timer_wheel_t(const timer_wheel_t&) = delete;
  timer_wheel_t& operator=(const timer_wheel_t&) = delete;

  /**
   * @brief arm (or re-arm) a node to fire after `delay`.
   * Re-arming an armed node moves it; there is no need to cancel first.
   * Spawns the runner whenever the wheel was idle, which is also why this must run
   * on the context's own thread.
   */
  void arm(timer_node_t& node, std::chrono::milliseconds delay);

  /**
   * @brief remove a node from the wheel. Safe on an unarmed node.
   */
  void cancel(timer_node_t& node);

  /**
   * @brief stop ticking for good. The runner notices within one tick and exits.
   *
   * Owners do not have to call this to avoid a leak — an idle wheel winds itself
   * down. It exists so the context can force a still-ticking wheel down while its
   * loop is winding up, and so a re-arm during teardown cannot start a fresh
   * runner. A stopped wheel stays stopped.
   */
  void stop() { running_ = false; }

  CORNET_NODISCARD bool running() const { return running_; }
  CORNET_NODISCARD uint64_t ticks() const { return ticks_; }
  CORNET_NODISCARD uint32_t armed_count() const { return armed_; }

 private:
  /**
   * @brief the ticking coroutine, spawned by an arm() on an idle wheel.
   * Runs only while timers are armed and returns as soon as none are, rather than
   * parking: a parked coroutine still costs its frame, and it used to be the thing
   * that stranded a wheel nobody wanted any more. Marked as framework io, so it
   * never keeps the context from draining.
   *
   * Holds a reference for as long as it runs, which is what makes dropping the
   * wheel while it ticks safe.
   */
  CORNET_NODISCARD static coro_t<void> run(std::shared_ptr<timer_wheel_t> self);

  /**
   * @brief spawn the runner unless one is already ticking.
   * Defined out of line: it needs the complete context_t.
   */
  void ensure_runner();

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
  // whether a runner coroutine is alive; false again as soon as it returns
  bool     runner_live_{false};
};

} // namespace cornet

#endif // CORNET_CONCURRENCY_TIMER_WHEEL_H
