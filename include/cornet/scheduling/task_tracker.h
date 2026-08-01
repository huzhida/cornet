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
 * Idle checks are a single load: can_idle() means no user work remains,
 * has_any_work() means even persistent watchers remain.
 *
 * Failure mode: if any module forgets to inc/dec, the counter drifts upward
 * and can_idle() stays false — the system conservatively stays alive (fail-safe).
 */
class task_tracker_t {
  context_t* ctx_ = nullptr;
  uint32_t user_task_{0};
  uint32_t io_task_{0};
  uint32_t cpu_task_{0};
  uint32_t persistent_task_{0};

public:
  void bind(context_t* ctx) { ctx_ = ctx; }

  // === IO work (via uring) ===
  // Called when SQEs are submitted to the kernel.
  void io_submit(uint32_t n) {
    io_task_ += n;
  }
  // Called when CQEs are processed (completed or peeked).
  void io_complete(uint32_t n) {
    io_task_ -= n;
  }
  // Persistent watcher lifecycle (e.g., signalfd, eventfd watch loops).
  void io_persistent_add(uint32_t n = 1) {
    persistent_task_ += n;
  }
  void io_persistent_remove(uint32_t n = 1) {
    persistent_task_ -= n;
  }

  // === CPU work (via executor thread pool) ===
  void cpu_add(uint32_t n = 1) {
    cpu_task_ += n;
  }
  void cpu_complete(uint32_t n = 1) {
    cpu_task_ -= n;
  }

  // === Coroutine work (via scheduler ready queue) ===
  void coroutine_add(uint32_t n = 1) {
    user_task_ += n;
  }
  void coroutine_remove(uint32_t n = 1) {
    user_task_ -= n;
  }

  // === Query ===
  // True when no user work remains (persistent watchers may still be running).
  // Used as the drain trigger for graceful shutdown.
  bool user_idle() const { return (user_task_ + io_task_ + cpu_task_) <= persistent_task_; }

  // True when literally nothing remains — used as the run-loop exit condition.
  bool idle() const { return (user_task_ + io_task_ + cpu_task_) == 0 && persistent_task_ == 0; }

  // Debug helpers
  uint32_t inflight_io() const { return io_task_; }
  uint32_t inflight_persistent() const { return persistent_task_; }

  context_t* context() const { return ctx_; }
};

} // namespace cornet

#endif // CORNET_TASK_TRACKER_H
