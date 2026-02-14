#ifndef CORNET_TASK_H
#define CORNET_TASK_H

#include <coroutine>
#include "utils.h"

namespace cornet {

struct task_t {
  using callback_t = void (*) (task_t*, int);
  explicit task_t(callback_t complete): complete(complete) {}
  callback_t complete;
  std::coroutine_handle<> handle;
};

struct context_t;
struct uring_task_t : task_t {
  explicit uring_task_t(context_t& ctx, callback_t complete) : ctx(ctx), task_t(complete) {}
  CORNET_MAYBE_UNUSED inline bool await_ready() {
    return false;
  }
  CORNET_MAYBE_UNUSED void await_suspend(std::coroutine_handle<> handle);

  context_t& ctx;
};

}

#endif //CORNET_TASK_H
