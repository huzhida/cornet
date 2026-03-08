#include "core/task.h"
#include "core/context.h"
#include "core/utask.h"

namespace cornet {

task_t::task_t(task_priority_t priority)
  : metadata{.priority = priority} {}

CORNET_MAYBE_UNUSED void utask_t::await_suspend(std::coroutine_handle<> handle) {
  this->handle = handle;
}

void utask_t::complete(cqe_t cqe) {
  value = cqe->res;
  ctx.sched(this);
}

}