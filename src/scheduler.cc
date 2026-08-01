#include "cornet/base/metrics.h"
#include "cornet/scheduling/scheduler.h"

#include <fmt/ranges.h>

#include "cornet/scheduling/context.h"
#include "cornet/coroutine/atask.h"
#include "cornet/utils/config.h"

namespace cornet {

std::unordered_map<scheduler_type_t, std::pair<std::string, scheduler_t::policy_factory_t>> scheduler_t::registry;

void scheduler_t::resume_one_task() {
  auto task = ready_tasks.front();
  ready_tasks.pop();
  tracker_.coroutine_remove();
  if (task && !task.done()) task.resume();
}

void scheduler_t::process_async_tasks(context_t& ctx) {
  auto completed = ctx.executor().get_completed(async_tasks);
  for (size_t idx = 0; idx < completed; ++idx) {
    this->schedule(async_tasks[idx]->handle);
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

  if (uring.running_task_nr() > 0) {
    cqes = uring.peek_cqes(ctx);
  }

  if (cqes == 0 && ready_tasks.empty()) {
    uring.wait_cqes(ctx, 1, wait_timeout);
  }

  return cqes;
}

void scheduler_t::sched(context_t& ctx) {
  CORNET_METRICS_SCOPE_TIMER(ctx.metrics().sched_latency);
  CORNET_METRICS_ADD(ctx.metrics().sched_cycles);
  auto& uring = ctx.io_uring();
  uint32_t cqes = 0;

  sched_stats stats;
  stats.start = std::chrono::steady_clock::now();

  while (!ready_tasks.empty() && !policy_->should_stop_cpu(stats)) {
    resume_one_task();
    stats.tasks_resumed++;
    CORNET_METRICS_ADD(ctx.metrics().tasks_resumed);
  }

  uring.submit();

  // collect executor completions first, may fill ready_tasks
  process_async_tasks(ctx);

  // drain cross-thread task submissions
  ctx.drain_remote_queue();

  if (uring.running_task_nr() > 0) {
    cqes = uring.peek_cqes(ctx);
  }

  if (cqes == 0 && ready_tasks.empty()) {
    uring.wait_cqes(ctx, 1, policy_->get_io_wait());
  }

  policy_->on_sched_done(stats, cqes, uring.running_task_nr());
}

// ---- Policy factory functions and registrations ----

static std::unique_ptr<schedule_policy_t> make_round_robin(config_t*) {
  return std::make_unique<round_robin_policy_t>();
}
CORNET_REGISTER_SCHEDULER(scheduler_type_t::RoundRobin, make_round_robin);

static std::unique_ptr<schedule_policy_t> make_batch(config_t* config) {
  auto p = std::make_unique<batch_policy_t>();
  if (config) {
    if (auto batch_num = (*config)["cornet"]["context"]["scheduler"]["batch"]) {
      p->max_batch = batch_num.value_or(32);
    }
  }
  return p;
}
CORNET_REGISTER_SCHEDULER(scheduler_type_t::Batch, make_batch);

static std::unique_ptr<schedule_policy_t> make_time_slice(config_t* config) {
  auto p = std::make_unique<time_slice_policy_t>();
  if (config) {
    auto conf = (*config)["cornet"]["context"]["scheduler"];
    p->cpu_budget = parse_time_str(conf["cpu_budget"].value_or("10ms"));
    p->io_budget = parse_time_str(conf["io_budget"].value_or("1ms"));
  }
  return p;
}
CORNET_REGISTER_SCHEDULER(scheduler_type_t::TimeSlice, make_time_slice);

static std::unique_ptr<schedule_policy_t> make_adaptive(config_t* config) {
  auto p = std::make_unique<adaptive_policy_t>();
  if (config) {
    auto conf = (*config)["cornet"]["context"]["scheduler"];
    if (auto batch = conf["cpu_batch"]) {
      p->cpu_batch_ = batch.value_or(64);
    }
    if (auto wait = conf["io_wait"]) {
      p->io_wait_ = parse_time_str(wait.value_or("1ms"));
    }
  }
  return p;
}
CORNET_REGISTER_SCHEDULER(scheduler_type_t::Adaptive, make_adaptive);

} // cornet
