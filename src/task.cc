#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"

namespace cornet {

utask_t::utask_t(context_t &ctx) {
  sqe = ctx.io_uring().new_sqe();
}

utask_t::utask_t(utask_t&& other) noexcept {
  this->sqe = other.sqe;
  other.sqe.sqe = nullptr;

  this->callback = other.callback;
  this->completed = other.completed;
  this->value = other.value;
  this->handle = other.handle;
  this->sqe.with_data(this);
}

void utask_t::await_suspend(std::coroutine_handle<> handle) {
  this->handle = handle;
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  if (!cqe->user_data) return 1;
  reinterpret_cast<utask_t*>(cqe->user_data)->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  value = cqe->res;
  completed = true;

  if (callback && handle) {
    auto a = (action*)handle.address();
    a->callback(ctx, a->data);
    return;
  }

  if (handle) {
    ctx.sched(this);
  }
}

}