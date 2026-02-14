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

context_t::context_t() {
  scheduler = scheduler_t::scheduler(scheduler_t::SCHEDULER_TYPE_ROUND_ROBIN);
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts[std::this_thread::get_id()] = this;
}
context_t::~context_t() {
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts.erase(std::this_thread::get_id());
}
void context_t::run() {
  if (owner != std::this_thread::get_id()) {
    SPDLOG_ERROR("never run context in other thread");
    return;
  }

  state.store(state_t::Running, std::memory_order_release);
  auto current_state = state_t::Running;
  while((current_state = state.load(std::memory_order_acquire)) != state_t::Terminated) {

    if (current_state == state_t::Canceling) {
      sched(cancel_io_tasks());
      state.store(state_t::Terminating);
    }

    scheduler->sched(*this);

    if (scheduler->idle() && uring.idle()) {
      state.store(state_t::Terminated);
    }

  }

}
uring_t &context_t::io_uring() {
  return uring;
}
void context_t::set_cancel_task(utask_t* task) {
  cancel_task = task;
}
utask_t *context_t::get_cancel_task() const {
  return cancel_task;
}
void context_t::stop(bool cancel) {
  if (cancel) {
    state.store(state_t::Canceling);
  } else {
    state.store(state_t::Terminated);
  }
}
std::thread::id context_t::owner_thread() const {
  return owner;
}
int context_t::process_utask(cqe_t cqe) {
  if (cqe->user_data) {
    reinterpret_cast<utask_t*>(cqe->user_data)->complete(cqe);
    return 0;
  }

  auto* cancel = context().get_cancel_task();
  if (!cancel) {
    SPDLOG_ERROR("context cancel_task task is nullptr, but cqe user data is nullptr");
    return -1;
  }
  cancel->complete(cqe);
  return 0;
}

std::unique_ptr<context_t::scheduler_t> context_t::scheduler_t::scheduler(const std::string &scheduler_type) {
  auto iter = registry.find(scheduler_type);
  if (iter == registry.end()) {
    std::vector<std::string> available_schedulers;
    available_schedulers.reserve(registry.size());
for(const auto& k : registry) {
      available_schedulers.emplace_back(k.first);
    }
    SPDLOG_ERROR("scheduler '{}' not exist, available scheduler: [{}]",
                 scheduler_type, fmt::join(available_schedulers, ","));
  }
  return iter->second();
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

  ctx.io_uring().wait_and_process_cqes(context_t::process_utask, ctx.io_uring().running_task_nr()
    , 0, 0);
  return 0;
}
context_t::cancel_awaiter::cancel_awaiter(context_t& ctx, void *user_data, int flags) : utask_t(ctx) {
  ctx.set_cancel_task(this);
  ctx.io_uring().new_sqe().prep_cancel(user_data, flags);
}

} // cornet