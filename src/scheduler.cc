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

CORNET_REGISTER_SCHEDULER(scheduler_type_t::TimeSlice, time_slice_scheduler_t);
time_slice_scheduler_t::time_slice_scheduler_t() {
  auto conf = config_t::get()["cornet"]["context"]["scheduler"];

  cpu_budget = config_t::to_nanoseconds(conf["cpu_budget"].value_or("10ms"));
  io_budget = config_t::to_nanoseconds(conf["io_budget"].value_or("1ms"));
}
void time_slice_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;
  auto start = std::chrono::steady_clock::now();
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty() && !cpu_timeout(start)) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }
  uring.submit();

  uring.wait_cqes(utask_t::process_utask, ctx, 1, io_budget);

  process_async_tasks(ctx);
}

bool time_slice_scheduler_t::cpu_timeout(std::chrono::steady_clock::time_point& start) const {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now() - start
    ) > cpu_budget;
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::RoundRobin, round_robin_scheduler_t);
void round_robin_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty()) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }
  uring.submit();

  uint32_t nr = uring.running_task_nr();
  if (nr > 0) {
    uring.peek_cqes(utask_t::process_utask, ctx, nr);
  }

  process_async_tasks(ctx);
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::Batch, batch_scheduler_t);
batch_scheduler_t::batch_scheduler_t() {
  if (auto batch_num = config_t::get()["cornet"]["context"]["scheduler"]["batch"]) {
    batch_nr = batch_num.value_or(32);
  }
}
void batch_scheduler_t::sched(context_t& ctx) {
  scoped_timer_t timer(ctx.metrics().sched_latency);
  ctx.metrics().sched_cycles++;
  size_t processed = 0;
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty() && ++processed < batch_nr) {
    resume_one_task();
    ctx.metrics().tasks_resumed++;
  }
  uring.submit();

  if (uring.running_task_nr() > 0) {
    uring.peek_cqes(utask_t::process_utask, ctx, batch_nr);
  }

  process_async_tasks(ctx);
}

} // cornet
