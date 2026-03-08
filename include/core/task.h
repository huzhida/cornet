#ifndef CORNET_TASK_H
#define CORNET_TASK_H

#include <coroutine>
#include "utils/utils.h"

namespace cornet {

enum class task_priority_t {
  IO, Low, Normal, High
};

struct task_t {
  explicit task_t(task_priority_t priority = task_priority_t::IO);

  union {
    task_priority_t priority;
  } metadata{};

  std::coroutine_handle<> handle;
};

}

#endif //CORNET_TASK_H