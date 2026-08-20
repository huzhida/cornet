#include "cornet/base/metrics.h"
#include "cornet/scheduling/scheduler.h"

#include <fmt/ranges.h>
#include <chrono>

#include "cornet/scheduling/context.h"
#include "cornet/coroutine/atask.h"
#include "cornet/utils/config.h"

namespace cornet {

scheduler_t::scheduler_t(task_tracker_t& tracker, config_t* config)
  : tracker_(tracker),
config_(config), executor_(tracker_, config) {
  if(config) {
    auto conf = (*config)["cornet"]["context"]["scheduler"];
    cpu_batch_ = conf["cpu_batch"].value_or(64);
    io_wait_ = parse_time_str(conf["io_wait"].value_or("1ms"));
  }
}

scheduler_t::~scheduler_t() {
  executor_.terminate();
}

void scheduler_t::resume_ready(context_t& ctx) {
  if (ready_tasks_.empty()) return;
  auto t0 = std::chrono::steady_clock::now();
  while (!ready_tasks_.empty() && stats.tasks_resumed < cpu_batch_) {
    resume_task();
    stats.tasks_resumed++;
    CORNET_METRICS_ADD(ctx.metrics().tasks_resumed);
  }
  stats.task_runtime_ns +=
    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
}

void scheduler_t::resume_task() {
  auto task = ready_tasks_.front();
  ready_tasks_.pop();
  tracker_.coroutine_remove();
  if (task && !task.done()) task.resume();
}

size_t scheduler_t::harvest_async() {
  size_t n{0}, completed{0};
  do {
    n = executor_.get_completed(async_tasks);
    for (size_t idx = 0; idx < n; ++idx) {
      // A null handle means the awaiting frame was destroyed while the pool
      // ran the job; the shared bookkeeping kept the result write safe, and
      // there is simply nobody left to resume.
      if (async_tasks[idx]->handle) {
        this->schedule(async_tasks[idx]->handle);
      }
      async_tasks[idx].reset();
    }
    completed += n;
  } while(n > 0);

  return completed;
}

uint32_t scheduler_t::harvest_remote() {
  std::coroutine_handle<> h;
  uint32_t n = 0;
  while (remote_tasks_.try_dequeue(h)) {
    // Counted here, on the owner thread: schedule_remote() is caller-thread
    // code and must not touch the tracker's plain counters (see its comment).
    schedule(h);
    ++n;
  }
  return n;
}

void scheduler_t::sched(context_t& ctx) {
  CORNET_METRICS_SCOPE_TIMER(ctx.metrics().sched_latency);
  CORNET_METRICS_ADD(ctx.metrics().sched_cycles);
  auto& uring = ctx.io_uring();
  uint32_t cqes = 0;
  stats.reset();
  auto start = std::chrono::steady_clock::now();
  // harvest remote queue handles
  harvest_remote();
  // harvest async completed handles
  harvest_async();
  resume_ready(ctx);
  // Reap completions from earlier submissions before touching the SQ:
  // peeking costs no syscall, and anything reaped here resumes in this
  // same cycle rather than the next one.
  if (uring.running_task_nr() > 0) {
    cqes = uring.peek_cqes(ctx);
  }
  resume_ready(ctx);
  if (!ready_tasks_.empty()) {
    // busy: flush this cycle's SQEs and keep rolling; their completions are
    // reaped on a following cycle
    uring.submit();
  } else {
    // park token + re-check on both producer queues: Dekker handshake with
    // wakeup(), never blocks with remote work pending
    context_t::park_scope park(ctx);
    if (harvest_remote() == 0 && harvest_async() == 0) {
      // One io_uring_enter both flushes pending SQEs and waits: an
      // independent submit() in front of the wait would be a second syscall
      // buying nothing on this edge path.
      cqes += uring.submit_and_wait_cqes(ctx, 1, io_wait_);
    } else {
      uring.submit();
    }
  }
  // Completions reaped above, and remote/async work admitted while parking,
  // also run in this same cycle.
  resume_ready(ctx);

  stats.cqes_ready = cqes;
  stats.inflight = uring.running_task_nr();
  stats.loop_runtime_ns +=
    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
  adapt();
}

void scheduler_t::adapt() {
  /*
   * 1. IO completion pressure
   *
   * cqes_ready:
   *   完成事件数量
   *
   * inflight:
   *   当前挂起IO
   *
   * cqes_ready / (inflight + cqes_ready):
   *   接近 1 时，说明挂起 IO 很少而完成事件集中，IO 吞吐高
   *   接近 0 时，说明大量 IO 仍在挂起，完成事件少
   */
  double instant_io_sat = 0.0;

  uint32_t total = stats.inflight + stats.cqes_ready;

  if (total > 0) {
    instant_io_sat = static_cast<double>(stats.cqes_ready) / static_cast<double>(total);
  }

  /*
   * 2. CPU pressure
   *
   * 防止task执行时间过长霸占event loop
   */
  double instant_cpu_pressure = 0.0;

  if (stats.loop_runtime_ns > 0) {
    instant_cpu_pressure = static_cast<double>(stats.task_runtime_ns) / static_cast<double>(stats.loop_runtime_ns);

    if (instant_cpu_pressure > 1.0)
      instant_cpu_pressure = 1.0;
  }

  /*
   * 3. EWMA smoothing
   */
  constexpr double io_alpha = 0.3;
  constexpr double cpu_alpha = 0.2;

  io_sat_fast_ = io_sat_fast_ * (1.0 - io_alpha) + instant_io_sat * io_alpha;

  cpu_pressure_fast_ = cpu_pressure_fast_ * (1.0 - cpu_alpha) + instant_cpu_pressure * cpu_alpha;

  double io_pressure = io_sat_fast_;

  double cpu_pressure = cpu_pressure_fast_;

  /*
   * 4. batch controller
   *
   * 优先级:
   *
   * CPU过高
   *      ↓
   * 减少batch释放CPU给IO
   *
   * IO压力高
   *      ↓
   * 减少batch避免task霸占
   *
   * 空闲
   *      ↓
   * 增大batch降低调度成本
   */

  int batch_delta = 0;

  if (cpu_pressure > 0.80) {
    batch_delta = -static_cast<int>(cpu_batch_ * 0.15);
  } else if (io_pressure > 0.65) {
    batch_delta = -static_cast<int>(cpu_batch_ * 0.08);
  } else if (cpu_pressure < 0.30 && io_pressure < 0.20 && stats.tasks_resumed >= cpu_batch_ / 2) {
    batch_delta = static_cast<int>(cpu_batch_ * 0.10);
  }

  /*
   * ready queue correction
   */
  size_t ready_cnt = ready_tasks_.size();

  if (ready_cnt > cpu_batch_ * 4) {
    batch_delta += 16;
  } else if (ready_cnt < cpu_batch_ / 4 && cpu_pressure < 0.3) {
    batch_delta -= 8;
  }

  int new_batch = static_cast<int>(cpu_batch_) + batch_delta;

  cpu_batch_ = std::clamp(static_cast<size_t>(std::max(0, new_batch)), min_batch_, max_batch_);

  /*
   * 5. IO wait controller
   *
   * idle:
   *      增大wait降低syscall
   *
   * busy:
   *      快速缩短wait降低latency
   */

  // "Idle" must mean *nothing is happening at all*: with io_uring ops in
  // flight, completions arrive on their own and cut any wait short — backing
  // off then only manufactures latency (submit_and_wait wakes on the first
  // completion either way; the timeout is a cap, never the expected value).
  // Ground truth experiment: sparse-but-active workloads (e.g. TLS record
  // pacing) were being throttled to adaptive-max wait per exchange.
  bool idle = stats.tasks_resumed == 0 && stats.cqes_ready == 0 && ready_cnt == 0 &&
              stats.inflight == 0;

  if (idle) {
    /*
     * exponential backoff
     *
     * 10us
     * 15us
     * 22us
     * ...
     */
    io_wait_ = std::min(max_wait_, std::chrono::duration_cast<std::chrono::nanoseconds>(io_wait_ + io_wait_ / 2));
  } else {
    /*
     * busy状态快速下降
     */
    if (cpu_pressure > 0.8 || io_pressure > 0.6) {
      io_wait_ = std::max(min_wait_, io_wait_ / 2);
    } else {
      io_wait_ = std::max(min_wait_, io_wait_ - io_wait_ / 5);
    }
  }
}

} // cornet
