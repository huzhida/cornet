#include "core/scheduler.h"
#include "core/context.h"
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
    SPDLOG_ERROR("scheduler '{}' not exist, available scheduler: [{}]",
                 cornet::scheduler_name(scheduler_type),
                 fmt::join(registered_schedulers.begin(), registered_schedulers.end(), ","));
  }
  return iter->second.second();
}
void scheduler_t::transfer_to(scheduler_t &scheduler) {
  scheduler.ready_tasks = std::move(ready_tasks);
}
void scheduler_t::process_ready_task() {
  auto handle = ready_tasks.front();
  ready_tasks.pop();
  if (!handle.done())
    handle.resume();
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::TimeSlice, time_slice_scheduler_t);
time_slice_scheduler_t::time_slice_scheduler_t() {
  auto conf = config_t::get()["cornet"]["context"]["scheduler"];

  cpu_budget = config_t::to_nanoseconds(conf["cpu_budget"].value_or("10ms"));
  io_budget = config_t::to_nanoseconds(conf["io_budget"].value_or("1ms"));
}
void time_slice_scheduler_t::sched(context_t& ctx) {
  auto start = std::chrono::steady_clock::now();
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty() && !cpu_timeout(start) && !uring.full()) {
    process_ready_task();
  }

  uring.submit();

  uring.wait_cqes(context_t::process_utask, uring.running_task_nr()
                                       , 0, ready_tasks.empty() ? io_budget.count() : 0);
}

bool time_slice_scheduler_t::cpu_timeout(std::chrono::steady_clock::time_point& start) const {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now() - start
    ) > cpu_budget;
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::RoundRobin, round_robin_scheduler_t);
void round_robin_scheduler_t::sched(context_t& ctx) {
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty() && !uring.full()) {
    process_ready_task();
  }

  uring.submit();

  uring.peek_cqes(context_t::process_utask, uring.running_task_nr());

  if (ready_tasks.empty() && !uring.idle()) {
    uring.wait_cqes(context_t::process_utask, uring.running_task_nr(), 0 , 1000000);
  }
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::Batch, batch_scheduler_t);
batch_scheduler_t::batch_scheduler_t() {
  if (auto batch_num = config_t::get()["cornet"]["context"]["scheduler"]["batch"]) {
    batch_nr = batch_num.value_or(32);
  }
}
void batch_scheduler_t::sched(context_t& ctx) {
  size_t processed = 0;
  auto& uring = ctx.io_uring();
  while (!ready_tasks.empty() && !uring.full()) {
    process_ready_task();
    if (++processed >= batch_nr) {
      break;
    }
  }
  uring.submit();

  uring.peek_cqes(context_t::process_utask, batch_nr);

  if (ready_tasks.empty() && !uring.idle()) {
    uring.wait_cqes(context_t::process_utask, uring.running_task_nr(), 0, 100000);
  }
}

} // cornet