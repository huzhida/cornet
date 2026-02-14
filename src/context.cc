#include "core/context.h"

namespace cornet {

#define CORNET_REGISTER_SCHEDULER(name, cls) \
  struct register_##cls { \
    register_##cls() {                 \
      cornet::context_t::scheduler_t::register_scheduler(name, cls::create);\
    }\
  } register_##cls_instance;

void context_t::run() {
  need_stop = false;
  while(!need_stop) {
  }
}
void context_t::run_until(bool (*predicate)()) {
  while(predicate()) {
    uring.wait_and_process_cqes([](cqe_t cqe) {
      auto* task = reinterpret_cast<uring_task_t*>(cqe->user_data);
      task->complete(task, cqe->res);
    })
  }
}
void context_t::stop() {
  need_stop = true;
}
uring_t &context_t::io_uring() {
  return uring;
}
context_t::context_t() {
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
}

CORNET_REGISTER_SCHEDULER(context_t::scheduler_t::SCHEDULER_TYPE_TIME_SLICE, time_slice_scheduler_t)
uint32_t time_slice_scheduler_t::sched(context_t& ctx) {
  return 0;
}
} // cornet