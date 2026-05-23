#ifndef CORNET_ATASK_H
#define CORNET_ATASK_H

#include "task.h"
#include <coroutine>

namespace cornet {

struct atask_t : task_t {
  void (*fn) (atask_t*){};
};

/**
 * @brief typed async task that stores a callable and its result.
 * Executes the callable on a worker thread, then resumes the coroutine on the owner thread.
 * @tparam F callable type
 * @tparam R return type of callable
 */
template<typename F, typename R = std::invoke_result_t<F>>
struct typed_atask_t : atask_t {
  F func_;
  R result_{};

  explicit typed_atask_t(F&& f) : func_(std::forward<F>(f)) {
    this->fn = [](atask_t* self) {
      auto* t = static_cast<typed_atask_t*>(self);
      t->result_ = t->func_();
    };
  }
};

template<typename F>
struct typed_atask_t<F, void> : atask_t {
  F func_;

  explicit typed_atask_t(F&& f) : func_(std::forward<F>(f)) {
    this->fn = [](atask_t* self) {
      auto* t = static_cast<typed_atask_t*>(self);
      t->func_();
    };
  }
};

}

#endif //CORNET_ATASK_H
