#ifndef CORNET_ATASK_H
#define CORNET_ATASK_H

#include "base/task.h"
#include <coroutine>
#include <exception>

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
struct typed_atask_t : atask_t {
  // the callable to execute
  F func_;
  // storage for the callable's return value
  R result_{};

  /**
   * @brief construct from a callable
   * @param f callable to execute on worker thread
   */
  explicit typed_atask_t(F&& f) : func_(std::forward<F>(f)) {
    this->fn = [](atask_t* self) {
      auto* t = static_cast<typed_atask_t*>(self);
      try {
        t->result_ = t->func_();
      } catch (...) {
        t->exception = std::current_exception();
      }
    };
  }
};

/**
 * @brief typed async task specialization for void-returning callables.
 * @tparam F callable type
 */
template<typename F>
struct typed_atask_t<F, void> : atask_t {
  // the callable to execute
  F func_;

  /**
   * @brief construct from a void-returning callable
   * @param f callable to execute on worker thread
   */
  explicit typed_atask_t(F&& f) : func_(std::forward<F>(f)) {
    this->fn = [](atask_t* self) {
      auto* t = static_cast<typed_atask_t*>(self);
      try {
        t->func_();
      } catch (...) {
        t->exception = std::current_exception();
      }
    };
  }
};

}

#endif //CORNET_ATASK_H
