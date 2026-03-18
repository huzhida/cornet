#ifndef CORNET_TASK_H
#define CORNET_TASK_H

#include <coroutine>
#include "utils/utils.h"

namespace cornet {

/**
 * @brief coroutine wrapper, minimum schedule unit
 */
struct task_t {
  // coroutine handle
  std::coroutine_handle<> handle{nullptr};
};

}

#endif //CORNET_TASK_H