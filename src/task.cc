#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"
#include "core/io_slot.h"

namespace cornet {

utask_t::~utask_t() {
  if (slot_data != 0 && ctx) {
    ctx->io_slots().free(slot_data);
  }
}

utask_t::utask_t(utask_t&& other) noexcept {
  this->prepare_fn = other.prepare_fn;
  this->callback = other.callback;
  this->completed = other.completed;
  this->value = other.value;
  this->handle = other.handle;
  this->user_data = other.user_data;
  this->ctx = other.ctx;
  this->slot_data = other.slot_data;
  other.ctx = nullptr;
  other.slot_data = 0;
}

void utask_t::await_suspend(std::coroutine_handle<> h) {
  this->handle = h;
  auto& uring = ctx->io_uring();
  auto& slots = ctx->io_slots();
  slot_data = slots.alloc(this);
  auto sqe = uring.get_sqe();
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(slot_data));
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  uint64_t data = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
  if (data == 0) return 1;
  auto* task = ctx.io_slots().lookup(data);
  if (!task) return 1;
  task->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  ctx.io_slots().free(slot_data);
  slot_data = 0;

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
