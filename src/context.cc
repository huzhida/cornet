#include "core/context.h"

namespace cornet {

std::mutex context_t::contexts_mutex;
std::unordered_map<std::thread::id, context_t*> context_t::contexts;

context_t::context_t(): uring(config::get()["cornet"]["context"]["uring"]["capacity"].value_or(32)) {
  if (auto scheduler_name = config::get()["cornet"]["context"]["scheduler"]["name"]) {
    scheduler_type = scheduler_t::to_scheduler_type(scheduler_name.as_string()->value_or(""));
  }
  scheduler = scheduler_t::scheduler(scheduler_type);
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
  while ((current_state = state.load(std::memory_order_acquire)) != state_t::Terminated) {

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

uring_t& context_t::io_uring() {
  return uring;
}

void context_t::set_cancel_task(utask_t* task) {
  cancel_task = task;
}

void context_t::set_scheduler_type(scheduler_type_t type) {
  if (type == scheduler_type) return;
  scheduler_type = type;
  auto s = scheduler_t::scheduler(scheduler_type);
  scheduler->transfer_to(*s);
  scheduler = std::move(s);
}

utask_t* context_t::get_cancel_task() const {
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

context_t::cancel_awaiter::cancel_awaiter(context_t& ctx, void* user_data, int flags)
  : utask_t(ctx) {
  ctx.set_cancel_task(this);
  ctx.io_uring().new_sqe().prep_cancel(user_data, flags);
}

} // cornet