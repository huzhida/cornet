#include "core/scheduler.h"
#include "core/context.h"
#include "core/atask.h"
#include "utils/metrics.h"
#include <fmt/ranges.h>

namespace cornet {

std::unordered_map<scheduler_type_t, std::pair<std::string, std::unique_ptr<scheduler_t>(*)()>> scheduler_t::registry;

std::unique_ptr<scheduler_t> scheduler_t::scheduler(scheduler_type_t scheduler_type) {
  auto iter = registry.find(scheduler_type);
  if (iter == registry.end()) {
    std::vector<std::string> registered_schedulers;
    registered_schedulers.reserve(registry.size());
    for (const auto& kv : registry) {
      registered_schedulers.emplace_back(kv.second.first);
    }
    CORNET_FATAL("scheduler '{}' not exist, available scheduler: [{}]",
                 cornet::scheduler_name(scheduler_type),
                 fmt::join(registered_schedulers.begin(), registered_schedulers.end(), ","));
  }
  return iter->second.second();
}

void scheduler_t::transfer_to(scheduler_t &scheduler) {
  scheduler.ready_tasks = std::move(ready_tasks);
}
void scheduler_t::resume_one_task() {
  auto task = ready_tasks.front();
  ready_tasks.pop();
  if (task && !task.done()) task.resume();
}

void scheduler_t::process_async_tasks(context_t& ctx) {
  auto& executor = ctx.async_executor();
  if (!executor) return;
  auto completed = executor->get_completed(async_tasks);
  for (size_t idx = 0; idx < completed; ++idx) {
    this->ready_tasks.push(async_tasks[idx]->handle);
  }
}

uint32_t scheduler_t::flush_io(context_t& ctx, std::chrono::nanoseconds wait_timeout) {
  auto& uring = ctx.io_uring();
  uring.submit();

  // collect executor completions first, may fill ready_tasks
  process_async_tasks(ctx);

  // drain cross-thread task submissions
  ctx.drain_remote_queue();

  uint32_t cqes = 0;
  if (!uring.user_idle()) {
    // user IO tasks inflight, try to harvest completions
    cqes = uring.peek_cqes(utask_t::process_utask, ctx, uring.running_task_nr());
    if (cqes == 0 && ready_tasks.empty()) {
      cqes = uring.wait_cqes(utask_t::process_utask, ctx, 1, wait_timeout);
    }
  } else if (ready_tasks.empty()) {
    // no user IO, no CPU tasks. bounded wait to avoid hot-loop
    cqes = uring.wait_cqes(utask_t::process_utask, ctx, 1, wait_timeout);
  }

  return cqes;
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::TimeSlice, time_slice_scheduler_t);
time_slice_scheduler_t::time_slice_scheduler_t() {
  auto conf = config_t::get()["cornet"]["context"]["scheduler"];

  cpu_budget = config_t::to_nanoseconds(conf["cpu_budget"].value_or("10ms"));
  io_budget = config_t::to_nanoseconds(conf["io_budget"].value_or("1ms"));
}
void time_slice_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(&ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;
  auto start = std::chrono::steady_clock::now();

  while (!ready_tasks.empty() && !cpu_timeout(start)) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }

  flush_io(ctx, io_budget);
}

bool time_slice_scheduler_t::cpu_timeout(std::chrono::steady_clock::time_point& start) const {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now() - start
    ) > cpu_budget;
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::RoundRobin, round_robin_scheduler_t);
void round_robin_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(&ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;

  while (!ready_tasks.empty()) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }

  flush_io(ctx);
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::Batch, batch_scheduler_t);
batch_scheduler_t::batch_scheduler_t() {
  if (auto batch_num = config_t::get()["cornet"]["context"]["scheduler"]["batch"]) {
    batch_nr = batch_num.value_or(32);
  }
}
void batch_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(&ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;
  size_t processed = 0;

  while (!ready_tasks.empty() && ++processed < batch_nr) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }

  flush_io(ctx);
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::Adaptive, adaptive_scheduler_t);
void adaptive_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(&ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;

  size_t resumed = 0;
  while (!ready_tasks.empty() && resumed < cpu_batch_) {
    resume_one_task();
    resumed++;
    ctx.metrics().tasks_resumed++;
  }

  size_t inflight = ctx.io_uring().running_task_nr();
  uint32_t cqes_ready = flush_io(ctx, io_wait_);
  adapt(resumed, cqes_ready, inflight);
}

void adaptive_scheduler_t::adapt(size_t resumed, uint32_t cqes_ready, size_t inflight) {
  // update I/O saturation (exponential moving average)
  double sat = inflight > 0 ? double(cqes_ready) / double(inflight) : 0.0;
  io_saturation_ = io_saturation_ * 0.9 + sat * 0.1;

  // adjust cpu_batch based on I/O saturation
  if (io_saturation_ > 0.5 && cpu_batch_ > 8) {
    // I/O completions piling up, reduce CPU batch to process them faster
    cpu_batch_ = std::max(size_t(8), cpu_batch_ * 3 / 4);
  } else if (io_saturation_ < 0.1 && resumed >= cpu_batch_) {
    // I/O idle and CPU saturated, allow more CPU work per cycle
    cpu_batch_ = std::min(size_t(1024), cpu_batch_ + cpu_batch_ / 4 + 1);
  }

  // adjust wait timeout based on load
  if (resumed == 0 && cqes_ready == 0) {
    // nothing happening, increase wait to save CPU
    io_wait_ = std::min(std::chrono::nanoseconds(10000000), io_wait_ * 2);
  } else if (cqes_ready > 0 || resumed > 0) {
    // active workload, keep wait tight
    io_wait_ = std::max(std::chrono::nanoseconds(50000), io_wait_ * 3 / 4);
  }
}

} // cornet
