#ifndef CORNET_TASK_H
#define CORNET_TASK_H

#include <coroutine>

namespace cornet {

struct task_t {
  using callback_t = void (*) (task_t*, int);
  explicit task_t(callback_t complete): complete(complete) {}
  callback_t complete;
  std::coroutine_handle<> handle;
};

}

#endif //CORNET_TASK_H
