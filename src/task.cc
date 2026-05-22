#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"

namespace cornet {

utask_t::utask_t(utask_t&& other) noexcept {
  this->callback = other.callback;
  this->completed = other.completed;
  this->value = other.value;
  this->handle = other.handle;
  this->user_data = other.user_data;
  this->ctx = other.ctx;
  other.ctx = nullptr;
}

void utask_t::await_suspend(std::coroutine_handle<> h) {
  this->handle = h;
  auto& uring = ctx->io_uring();
  auto sqe = uring.get_sqe();
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, this);
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  if (!cqe->user_data) return 1;
  reinterpret_cast<utask_t*>(cqe->user_data)->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  value = cqe->res;
  completed = true;

  if (callback) {
    callback(ctx, user_data);
  }

  if (handle) {
    ctx.sched(this);
  }
}

}
