#ifndef CORNET_ATASK_H
#define CORNET_ATASK_H

#include "cornet/base/task.h"
#include <exception>
#include <variant>

namespace cornet {

/**
 * @brief base async task for thread pool execution.
 * Contains a function pointer that worker threads invoke.
 */
struct atask_t : task_t {
  // function to execute on worker thread
  void (*fn) (atask_t*){};
  // exception captured during execution (if any)
  std::exception_ptr exception{nullptr};
};

/**
 * @brief typed async task that stores a callable and its result.
 * Executes the callable on a worker thread, stores the return value.
 * @tparam F callable type
 * @tparam R return type of callable
 */
template<typename F, typename R = std::invoke_result_t<F>>
struct async_task_t : atask_t {
  // the callable to execute
  F func_;
  // storage for the callable's return value
  std::conditional_t<std::is_void_v<R>, std::monostate, R> result_{};

  /**
   * @brief construct from a callable
   * @param f callable to execute on worker thread
   */
  explicit async_task_t(F&& f) : func_(std::forward<F>(f)) {
    this->fn = [](atask_t* self) {
      auto* t = static_cast<async_task_t*>(self);
      try {
        if constexpr (std::is_void_v<R>) {
          t->func_();
          return;
        } else {
          t->result_ = t->func_();
        }
      } catch (...) {
        t->exception = std::current_exception();
      }
    };
  }
};

}

#endif //CORNET_ATASK_H
