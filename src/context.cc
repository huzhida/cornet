#include "core/context.h"

namespace cornet {

#define CORNET_REGISTER_SCHEDULER(name, cls) \
  struct register_##cls { \
    register_##cls() {                 \
      cornet::context_t::scheduler_t::register_scheduler(name, cls::create);\
    }\
  } register_##cls##_instance;

std::mutex context_t::contexts_mutex;
std::unordered_map<std::thread::id, context_t*> context_t::contexts;
std::unordered_map<std::string, std::unique_ptr<context_t::scheduler_t>(*)()> context_t::scheduler_t::registry;

void context_t::run() {
  terminated = false;
  while(!terminated) {
    scheduler->sched(*this);
    if (scheduler->idle() && uring.task_nr == 0) {
      return;
    }
  }

}
void context_t::run_until(bool (*predicate)()) {
   do {
    scheduler->sched(*this);
  } while(predicate());
}
coro_t<void> context_t::stop() {
  co_await cancel();
  terminated = true;
}
uring_t &context_t::io_uring() {
  return uring;
}
context_t::context_t() {
  scheduler = scheduler_t::scheduler(scheduler_t::SCHEDULER_TYPE_ROUND_ROBIN);
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts[std::this_thread::get_id()] = this;
}
context_t::~context_t() {
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts.erase(std::this_thread::get_id());
}

std::unique_ptr<context_t::scheduler_t> context_t::scheduler_t::scheduler(const std::string &scheduler_type) {
  auto iter = registry.find(scheduler_type);
  if (iter == registry.end()) {
    std::vector<std::string> available_schedulers;
    for(const auto& k : registry) {
      available_schedulers.emplace_back(k.first);
    }
    SPDLOG_ERROR("scheduler '{}' not exist, available scheduler: [{}]",
                 scheduler_type, fmt::join(available_schedulers, ","));
  }
  return (iter->second)();
}

CORNET_REGISTER_SCHEDULER(context_t::scheduler_t::SCHEDULER_TYPE_TIME_SLICE, time_slice_scheduler_t)
uint32_t time_slice_scheduler_t::sched(context_t& ctx) {
  return 0;
}
CORNET_REGISTER_SCHEDULER(context_t::scheduler_t::SCHEDULER_TYPE_ROUND_ROBIN, round_robin_scheduler_t)
uint32_t round_robin_scheduler_t::sched(context_t &ctx) {
  while(!pending_tasks.empty()) {
    auto handle = pending_tasks.front();
    pending_tasks.pop();
    if (!handle.done()) handle.resume();
    if (handle.done()) {
      active_tasks.erase(handle.address());
    }
  }

  ctx.io_uring().wait_and_process_cqes([](cqe_t cqe) {
    if (cqe->user_data) {
      reinterpret_cast<uring_task_t*>(cqe->user_data)->complete(cqe);
    } else {
      context_t::context().cancel_task->complete(cqe);
    }

  }, 1, 0, 0);
  return 0;
}
context_t::cancel_awaiter::cancel_awaiter(context_t& ctx, void *user_data, int flags) : uring_task_t(ctx) {
  ctx.cancel_task =  this;
  ctx.io_uring().new_sqe().prep_cancel(user_data, flags);
}

} // cornet