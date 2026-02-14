#ifndef CORNET_TASK_H
#define CORNET_TASK_H

#include <coroutine>
#include "utils.h"

namespace cornet {

struct task_t {
  explicit task_t(int priority = 0): priority(priority) {}
  int priority;
  std::coroutine_handle<> handle;
};

}

#endif //CORNET_TASK_H
