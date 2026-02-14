#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"

namespace cornet{

CORNET_MAYBE_UNUSED void utask_t::await_suspend(std::coroutine_handle<> handle) {
  this->handle = handle;
  ctx.io_uring().submit();
}
void utask_t::complete(cqe_t cqe) {
  value = cqe->res;
  ctx.sched(this);
}

}

