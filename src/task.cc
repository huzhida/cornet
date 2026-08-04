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
#if !CORNET_LINUX_VERSION_GE_5_19
  if (slot_data_ != 0 && ctx) {
    ctx->io_slots().free(slot_data_);
    slot_data_ = 0;
    CORNET_METRICS_ADD(ctx->metrics().slot_frees);
  }
#endif
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
#if !CORNET_LINUX_VERSION_GE_5_19
  this->slot_data_ = other.slot_data_;
  other.slot_data_ = 0;
#endif
}

void utask_t::prepare_into(io_uring_sqe* sqe) {
#if CORNET_LINUX_VERSION_GE_5_19
  // 5.19+ uses raw this pointer as user_data; slot table skipped
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(this));
#else
  slot_data_ = ctx->io_slots().alloc(this);
  prepare_fn(this, sqe);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(slot_data_));
#endif
  // single choke point for "op armed": also reached by linked-submission
  // awaiters, so the count always pairs with complete()
  if (user_work && !user_io_counted) {
    ctx->tracker_.user_io_add();
    user_io_counted = true;
  }
  CORNET_METRICS_ADD(ctx->metrics().slot_allocs);
}

void utask_t::await_suspend(std::coroutine_handle<> h) {
  this->handle = h;
  auto sqe = ctx->io_uring().get_sqe();
  prepare_into(sqe);
}

int utask_t::process_utask(context_t& ctx, cqe_t cqe) {
  uint64_t data = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
  if (data == 0) return 1;
#if CORNET_LINUX_VERSION_GE_5_19
  // 5.19+ uses raw this pointer as user_data
  auto* task = static_cast<utask_t*>(reinterpret_cast<void*>(data));
#else
  // older kernels: decode slot table token, detect stale CQEs via generation
  auto* task = ctx.io_slots().lookup(data);
  if (!task) {
    CORNET_METRICS_ADD(ctx.metrics().slot_stale_cqes);
    return 1;
  }
#endif
  task->complete(ctx, cqe);
  return 0;
}

void utask_t::complete(context_t& ctx, cqe_t cqe) {
  if (user_io_counted) {
    ctx.tracker_.user_io_remove();
    user_io_counted = false;
  }
#if !CORNET_LINUX_VERSION_GE_5_19
  ctx.io_slots().free(slot_data_);
  CORNET_METRICS_ADD(ctx.metrics().slot_frees);
  slot_data_ = 0;
#endif

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
