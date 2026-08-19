#include "cornet/base/task.h"
#include "cornet/scheduling/context.h"
#include "cornet/io_uring/utask.h"
#include "cornet/io_uring/io_slot.h"

namespace cornet {

utask_t::~utask_t() {
  // op abandoned before its CQE resolved: release the count so it cannot pin
  // user_idle() at false
  if (user_io_counted && ctx) {
    ctx->tracker_.user_io_remove();
    user_io_counted = false;
  }
  if (slot_data_ != 0 && ctx) {
    ctx->io_slots().free(slot_data_);
    slot_data_ = 0;
    CORNET_METRICS_ADD(ctx->metrics().slot_frees);
  }
}

utask_t::utask_t(utask_t&& other) noexcept {
  this->prepare_fn = other.prepare_fn;
  this->completed = other.completed;
  this->user_work = other.user_work;
  this->user_io_counted = other.user_io_counted;
  other.user_io_counted = false;
  this->value = other.value;
  this->handle = other.handle;
  this->ctx = other.ctx;
  other.ctx = nullptr;
  this->slot_data_ = other.slot_data_;
  other.slot_data_ = 0;
}

void utask_t::prepare_into(io_uring_sqe* sqe) {
  // Always the slot-token encoding: a stale CQE is detected by generation, and
  // user_data semantics do not actually depend on kernel version — the ring
  // round-trips 64 opaque bits on every kernel.
  slot_data_ = ctx->io_slots().alloc(this);
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(slot_data_));
  // single choke point for "op armed": also reached by linked-submission
  // awaiters, so the count always pairs with complete()
  if (user_work && !user_io_counted) {
    ctx->tracker_.user_io_add();
    user_io_counted = true;
  }
  CORNET_METRICS_ADD(ctx->metrics().slot_allocs);
}

bool utask_t::await_suspend(std::coroutine_handle<> h) {
  this->handle = h;
  auto sqe = ctx->io_uring().get_sqe();
  if (!sqe) {
    // Nothing reached the kernel, so no CQE will ever arrive for this op:
    // resume immediately with the error instead of suspending on a wakeup that
    // cannot come.
    fail(sqe.error().code);
    return false;
  }
  prepare_into(*sqe);
  return true;
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  uint64_t data = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
  if (data == 0) return 1;
  // decode slot table token, detect stale CQEs via generation
  auto* task = ctx.io_slots().lookup(data);
  if (!task) {
    CORNET_METRICS_ADD(ctx.metrics().slot_stale_cqes);
    return 1;
  }
  task->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  if (user_io_counted) {
    ctx.tracker_.user_io_remove();
    user_io_counted = false;
  }
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

  if (handle && !handle.done()) {
    ctx.spawn(this);
  }
}

}
