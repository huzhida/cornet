#ifndef CORNET_URING_TASK_H
#define CORNET_URING_TASK_H

#include "task.h"
#include "uring.h"

namespace cornet {

struct context_t;
struct uring_task_t : task_t {
  using callback_t = void (*) (task_t*, cqe_t);
  explicit uring_task_t(context_t& ctx, callback_t complete) : ctx(ctx), complete(complete) {}
  CORNET_MAYBE_UNUSED inline bool await_ready() {
    return false;
  }
  CORNET_MAYBE_UNUSED void await_suspend(std::coroutine_handle<> handle);

  context_t& ctx;
  callback_t complete;
};

}

#endif //CORNET_URING_TASK_H
