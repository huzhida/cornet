#include "cornet/base/task.h"
#include "cornet/scheduling/context.h"
#include "cornet/io_uring/utask.h"
#include "cornet/io_uring/io_slot.h"

namespace cornet {

utask_t::~utask_t() {
  if (slot_data_ != 0 && ctx) {
    ctx->io_slots().free(slot_data_);
    CORNET_METRICS_ADD(ctx->metrics().slot_frees);
  }
}

utask_t::utask_t(utask_t&& other) noexcept {
  this->prepare_fn = other.prepare_fn;
  this->completed = other.completed;
  this->value = other.value;
  this->handle = other.handle;
  this->ctx = other.ctx;
  this->slot_data_ = other.slot_data_;
  other.ctx = nullptr;
  other.slot_data_ = 0;
}

void utask_t::prepare_into(io_uring_sqe* sqe) {
  slot_data_ = ctx->io_slots().alloc(this);
  CORNET_METRICS_ADD(ctx->metrics().slot_allocs);
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(slot_data_));
}

void utask_t::await_suspend(std::coroutine_handle<> h) {
  this->handle = h;
  auto sqe = ctx->io_uring().get_sqe();
  prepare_into(sqe);
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  uint64_t data = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
  if (data == 0) return 1;
  auto* task = ctx.io_slots().lookup(data);
  if (!task) {
    CORNET_METRICS_ADD(ctx.metrics().slot_stale_cqes);
    return 1;
  }
  task->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  ctx.io_slots().free(slot_data_);
  CORNET_METRICS_ADD(ctx.metrics().slot_frees);
  slot_data_ = 0;

  value = cqe->res;
  completed = true;

  if (value < 0) {
    CORNET_METRICS_ADD(ctx.metrics().tasks_failed);
  } else {
    CORNET_METRICS_ADD(ctx.metrics().tasks_completed);
  }

  if (handle) {
    ctx.spawn(this);
  }
}

}
