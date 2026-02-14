#include "core/task.h"
#include "core/context.h"

namespace cornet{

void uring_task_t::await_suspend(std::coroutine_handle<> handle) {
  this->handle = handle;
  ctx.io_uring().submit();
}

}

