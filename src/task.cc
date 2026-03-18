#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"

namespace cornet {

CORNET_MAYBE_UNUSED void utask_t::await_suspend(std::coroutine_handle<> handle) {
  this->handle = handle;
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