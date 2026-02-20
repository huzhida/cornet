#ifndef CORNET_URING_TASK_H
#define CORNET_URING_TASK_H

#include "task.h"
#include "uring.h"

namespace cornet {

struct context_t;

struct utask_t : task_t {
  explicit utask_t(context_t& ctx)
    : ctx(ctx) {}

  CORNET_MAYBE_UNUSED inline bool await_ready() {
    return false;
  }

  CORNET_MAYBE_UNUSED void await_suspend(std::coroutine_handle<> handle);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED inline int await_resume() const {
    return value;
  }

  void complete(cqe_t cqe);

  int value{0};
  context_t& ctx;
};


}

#endif //CORNET_URING_TASK_H