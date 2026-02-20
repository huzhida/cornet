#include "core/scheduler.h"
#include "core/context.h"

namespace cornet {

std::unordered_map<scheduler_type_t, std::unique_ptr<scheduler_t>(*)()> scheduler_t::registry;

std::unique_ptr<scheduler_t> scheduler_t::scheduler(scheduler_type_t scheduler_type) {
  auto iter = registry.find(scheduler_type);
  if (iter == registry.end()) {
    std::vector<std::string> registered_schedulers;
    registered_schedulers.reserve(registry.size());
    for (const auto& kv : registry) {
      registered_schedulers.emplace_back(cornet::scheduler_type(kv.first));
    }
    SPDLOG_ERROR("scheduler '{}' not exist, available scheduler: [{}]",
                 cornet::scheduler_type(scheduler_type),
                 fmt::join(registered_schedulers.begin(), registered_schedulers.end(), ","));
  }
  return iter->second();
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::TimeSlice, time_slice_scheduler_t);
uint32_t time_slice_scheduler_t::sched(context_t& ctx) {
  constexpr int IOBudget = 1000;
  constexpr int CPUBudget = 10;

  auto start = std::chrono::steady_clock::now();
  static uint32_t loop = 0;
  while (!pending_tasks.empty() && (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)).count() < CPUBudget) {
    auto handle = pending_tasks.front();
    pending_tasks.pop();
    if (!handle.done())
      handle.resume();
    if (handle.done()) {
      active_tasks.erase(handle.address());
    }
         }
  ctx.io_uring().wait_and_process_cqes(context_t::process_utask, ctx.io_uring().running_task_nr()
                                       , 0, pending_tasks.empty() ? IOBudget : 0);
  return 0;
}

CORNET_REGISTER_SCHEDULER(scheduler_type_t::RoundRobin, round_robin_scheduler_t);

uint32_t round_robin_scheduler_t::sched(context_t& ctx) {
  while (!pending_tasks.empty()) {
    auto handle = pending_tasks.front();
    pending_tasks.pop();
    if (!handle.done())
      handle.resume();
    if (handle.done()) {
      active_tasks.erase(handle.address());
    }
  }

  ctx.io_uring().wait_and_process_cqes(context_t::process_utask, ctx.io_uring().running_task_nr()
                                       , 0, 0);
  return 0;
}

} // cornet