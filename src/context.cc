#include "core/context.h"

namespace cornet {

std::mutex context_t::contexts_mutex;
std::unordered_map<std::thread::id, context_t*> context_t::contexts;

context_t::context_t()
: uring(config_t::get()["cornet"]["context"]["uring"]["capacity"].value_or(32)) {
  if (auto scheduler_name = config_t::get()["cornet"]["context"]["scheduler"]["name"]) {
    scheduler_type = scheduler_t::to_scheduler_type(scheduler_name.as_string()->value_or(""));
  }
  scheduler = scheduler_t::scheduler(scheduler_type);
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts[std::this_thread::get_id()] = this;

}

context_t::~context_t() {
  if (executor) executor->terminate();
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts.erase(std::this_thread::get_id());
}

void context_t::run() {
  if (owner != std::this_thread::get_id()) {
    SPDLOG_ERROR("never run context in other thread");
    return;
  }

  switch_to(state_t::Running);
  state_t current_state;
  while ((current_state = state.load(std::memory_order_acquire)) != state_t::Terminated) {

    if (current_state == state_t::Canceling) {
      sched(cancel_io_tasks());
      switch_to(state_t::Terminating);
    }

    scheduler->sched(*this);

    if (idle()) {
      switch_to(state_t::Terminated);
    }else if (scheduler->idle() && !uring.idle()) {
      uring.wait_cqes(utask_t::process_utask, *this, 1, std::chrono::seconds(1));
    }

  }

}

void context_t::set_scheduler_type(scheduler_type_t type) {
  if (type == scheduler_type) return;
  scheduler_type = type;
  auto s = scheduler_t::scheduler(scheduler_type);
  scheduler->transfer_to(*s);
  scheduler = std::move(s);
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

context_t::cancel_awaiter::cancel_awaiter(context_t& ctx, void* user_data, int flags)
  : utask_t(ctx) {
  sqe.prep_cancel(user_data, flags).with_data(this);
}

} // cornet