#ifndef CORNET_SCHEDULER_H
#define CORNET_SCHEDULER_H

#include <chrono>
#include <vector>
#include <coroutine>
#include <array>
#include <memory>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <moodycamel/concurrentqueue.h>

#include "cornet/base/defines.h"
#include "cornet/utils/config.h"
#include "cornet/scheduling/task_tracker.h"
#include "cornet/scheduling/executor.h"

namespace cornet {

struct config_t;
struct context_t;
struct atask_t;

/**
 * @brief IO-aware cooperative task scheduler.
 *
 * Manages the ready queue, I/O submission/waiting, async task harvesting,
 * and adaptive batch sizing based on CPU/I/O pressure.
 *
 * Scheduling is driven by a fixed cpu_batch limit and adaptive io_wait;
 * configuration is read from config_t at construction time.
 */
struct scheduler_t {
  /**
   * @brief cache-friendly ring buffer queue for coroutine handles.
   * Contiguous memory layout for optimal cache performance on the hot scheduling path.
   * Power-of-2 capacity for branchless index wrapping via bitmask.
   */
  struct ring_queue_t {
    ring_queue_t() : buf_(1024), mask_(1023), head_(0), tail_(0) {}

    void push(std::coroutine_handle<> h) {
      if (size() == buf_.size()) [[unlikely]]
        grow();
      buf_[tail_ & mask_] = h;
      tail_++;
    }

    std::coroutine_handle<> front() const { return buf_[head_ & mask_]; }

    void pop() { head_++; }

    CORNET_NODISCARD bool empty() const { return head_ == tail_; }
    CORNET_NODISCARD size_t size() const { return tail_ - head_; }

  private:
    void grow() {
      size_t old_cap = buf_.size();
      size_t new_cap = old_cap * 2;
      std::vector<std::coroutine_handle<>> new_buf(new_cap);
      for (size_t i = head_; i != tail_; i++) {
        new_buf[i & (new_cap - 1)] = buf_[i & mask_];
      }
      buf_ = std::move(new_buf);
      mask_ = new_cap - 1;
    }

    std::vector<std::coroutine_handle<>> buf_;
    size_t mask_;
    size_t head_;
    size_t tail_;
  };
  using queue_t = ring_queue_t;

  /**
   * @brief scoped timer 
   */
  struct scoped_timer_t {
    uint64_t& elapsed_ns_;
    std::chrono::steady_clock::time_point start;

    scoped_timer_t(uint64_t& elapsed_ns, bool enabled = true)
    : elapsed_ns_(elapsed_ns), start(enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}) {}

    ~scoped_timer_t() {
      if (start != std::chrono::steady_clock::time_point{}) {
        elapsed_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
      }
    }
  };

  /**
   * @brief statistics collected during a single sched() cycle.
   * Used by adapt() to tune cpu_batch and io_wait based on
   * I/O completion pressure and CPU saturation.
   */
  struct sched_stats {
    uint32_t inflight{0};
    uint32_t cqes_ready{0};
    uint32_t tasks_resumed{0};

    uint64_t task_runtime_ns{};
    uint64_t loop_runtime_ns{};

    inline void reset() {
      *this = {};
    }
  };

  scheduler_t(task_tracker_t& tracker, config_t* config);

   ~scheduler_t();

  /**
   * @brief schedule interface: harvest remote/async tasks, resume ready tasks up to cpu_batch,
   * submit I/O, wait for completions if queue is empty.
   * Adaptive batch sizing via adapt() based on CPU/I/O pressure.
   * @param ctx owner context
   */
  void sched(context_t& ctx);

  /**
   * @brief push coroutine handle to ready queue.
   * @param h coroutine handle
   */
  inline void schedule(std::coroutine_handle<> h) {
    ready_tasks_.push(h);
    tracker_.coroutine_add();
  }

  /**
   * @brief push coroutine handle to the remote (cross-thread) queue.
   *
   * Contract: spawn_remote() is for keep-alive contexts. The handle is counted
   * at harvest time, on the owner thread, which keeps the tracker strictly
   * single-threaded — no atomics on the hot path. That is safe because with
   * keep_alive set, user_idle()/idle() can never turn true, so an enqueued
   * handle can never be abandoned by the idle exit; the loop only leaves on an
   * explicit stop()/shutdown(), and spawning new work into a closing context
   * is a usage error either way.
   * @param h coroutine handle
   */
  inline void schedule_remote(std::coroutine_handle<> h) {
    remote_tasks_.enqueue(h);
  }

  /**
   * @brief whether scheduler idle
   * @return true for idle / false for busy
   */
  CORNET_NODISCARD inline bool idle() const {
    return ready_tasks_.empty();
  }

private:
  // work tracker (owned by context_t, set after construction)
  task_tracker_t& tracker_;
  // ready to resume queue
  queue_t ready_tasks_;
  // MPSC queue for cross-thread task submission
  moodycamel::ConcurrentQueue<std::coroutine_handle<>> remote_tasks_;
  // thread-pool executor for offload heavy cpu task
  executor_t executor_;
  // async tasks buffer
  std::array<std::shared_ptr<atask_t>, 32> async_tasks;
  // config pointer for reading scheduler tuning parameters at construction
  config_t* config_ = nullptr;
  // --------- schedule policy --------
  // schedule stats
  sched_stats stats;
  // schedule cycles count
  size_t cycles{0};
  // scheduler cpu batch size. Single source of truth for the default: the
  // value_or() fallback in scheduler.cc reads cpu_batch_ directly, and the
  // number in docs/configuration.md is meant to match this initializer.
  size_t cpu_batch_{64};
  // scheduler io wait budget
  std::chrono::nanoseconds io_wait_{std::chrono::milliseconds(1)};
  // io saturation
  double io_saturation_{0.0};
  // io saturation fast
  double io_sat_fast_{0.0};
  // cpu pressure fast
  double cpu_pressure_fast_{0.0};
  // adapt() cadence: 1 in kAdaptInterval cycles. EWMA alphas were tuned at
  // every-cycle cadence, so skipping 7/8 calls stretches the time constant
  // ~8x; that is fine (cycles run at 10K+/sec under load) and saves the 4
  // steady_clock reads + EWMA divides on the skipped cycles.
  static constexpr uint32_t kAdaptInterval = 1;
  uint32_t adapt_phase_{0};
  // min batch
  static constexpr size_t min_batch_ = 32;
  // max batch
  static constexpr size_t max_batch_ = 2048;
  // min_wait
  static constexpr std::chrono::nanoseconds min_wait_ = std::chrono::nanoseconds(50000); // 50us
  // max wait
  static constexpr std::chrono::nanoseconds max_wait_ = std::chrono::milliseconds(1);

  // resume one task from the ready queue
  void resume_task();
  // resume ready tasks up to cpu_batch_, measuring time into stats
  void resume_ready(context_t& ctx, bool need_adapt);
  // collect completed executor tasks into ready queue, returns how many moved
  size_t harvest_async();
  // drain the cross-thread queue into the ready queue, returns how many moved
  uint32_t harvest_remote();

  void adapt();

  friend struct context_t;
};

} // cornet

#endif //CORNET_SCHEDULER_H
