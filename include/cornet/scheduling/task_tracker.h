#ifndef CORNET_TASK_TRACKER_H
#define CORNET_TASK_TRACKER_H

#include <spdlog/spdlog.h>

#include <cstdint>
namespace cornet {

struct context_t; // forward declaration

/**
 * @brief single source of truth for all in-flight work across uring, executor, and scheduler.
 *
 * All modules that create or complete work go through this tracker.
 * Since every context is single-threaded (thread-per-core), all tracker accesses
 * happen on the owner thread — no atomics needed.
 *
 * Two independent axes are tracked, because the two idle questions need
 * different answers:
 *
 *   - Ownership: does any *user* work remain? Framework-internal io (watcher
 *     reads, the drain timer, cancellation ops) is excluded, so user_idle()
 *     becomes true as soon as the application is done. This drives the
 *     Running -> Canceling transition.
 *   - Liveness: is any SQE still in flight at all? This includes framework io
 *     and fire-and-forget ops, and is the real run-loop exit condition —
 *     leaving the loop with inflight io would tear the ring down underneath it.
 *
 * Both predicates are plain all-zero checks. Nothing is compared across
 * categories, so adding a watcher or changing how many ops one keeps armed
 * cannot skew the result.
 *
 * Failure mode: io ownership defaults to "user" (see utask_t::user_work), so a
 * missed classification keeps the context alive rather than draining it early.
 * A missed inc/dec likewise drifts the counter upward and idle() stays false —
 * the system conservatively stays alive (fail-safe).
 */
class task_tracker_t {
  // owning context. Bound at construction and never rebound, so every module
  // reached through the tracker can assume it is valid.
  context_t& ctx_;
  // ready-queue depth. Kind-agnostic on purpose: a queued handle is resumed on
  // the next sched() cycle, so counting a framework handle here only delays
  // idle detection by one cycle, and errs toward staying alive.
  uint32_t ready_{0};
  // work offloaded to the executor thread pool. Always user work — only
  // ctx.async() reaches the executor.
  uint32_t cpu_{0};
  // io ops owned by user code (subset of io_inflight_)
  uint32_t user_io_{0};
  // every SQE submitted to the kernel, including framework io and
  // fire-and-forget ops that have no utask to resolve them
  uint32_t io_inflight_{0};

public:
  explicit task_tracker_t(context_t& ctx) : ctx_(ctx) {}

  // === IO liveness (via uring, bulk at submit/completion) ===
  // Called when SQEs are submitted to the kernel.
  inline void io_submit(uint32_t n) {
    io_inflight_ += n;
  }
  // Called when CQEs are processed (completed or peeked).
  inline void io_complete(uint32_t n) {
    io_inflight_ -= n;
  }

  // === IO ownership (via utask_t, per-op at prepare/complete) ===
  // Only ops with user_work set reach these.
  inline void user_io_add(uint32_t n = 1) {
    user_io_ += n;
  }
  inline void user_io_remove(uint32_t n = 1) {
    user_io_ -= n;
  }

  // === CPU work (via executor thread pool) ===
  inline void executor_add(uint32_t n = 1) {
    cpu_ += n;
  }
  inline void executor_remove(uint32_t n = 1) {
    cpu_ -= n;
  }

  // === Coroutine work (via scheduler ready queue) ===
  inline void coroutine_add(uint32_t n = 1) {
    ready_ += n;
  }
  inline void coroutine_remove(uint32_t n = 1) {
    ready_ -= n;
  }

  // === Query ===
  // True when no user work remains. Framework io may still be armed.
  // Used as the drain trigger for graceful shutdown.
  inline bool user_idle() const { return (ready_ + cpu_ + user_io_) == 0; }

  // True when literally nothing remains, including framework and
  // fire-and-forget io — used as the run-loop exit condition.
  inline bool idle() const { return user_idle() && io_inflight_ == 0; }

  // Debug helpers
  inline uint32_t inflight_io() const { return io_inflight_; }
  inline uint32_t inflight_user_io() const { return user_io_; }

  inline context_t& context() const { return ctx_; }
};

} // namespace cornet

#endif // CORNET_TASK_TRACKER_H
