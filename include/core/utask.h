#ifndef CORNET_URING_TASK_H
#define CORNET_URING_TASK_H

#include "task.h"
#include "uring.h"

namespace cornet {

struct context_t;

/**
 * @brief io_uring specific task/awaiter base struct.
 * This struct implements the C++ coroutine Awaiter interface,
 * allowing io_uring operations to be used with 'co_await'.
 * Subclasses store operation parameters and set prepare_fn to fill the SQE at submission time.
 */
struct utask_t : task_t {
  using callback_t = void(*)(context_t&, void*);
  using prepare_fn_t = void(*)(utask_t*, io_uring_sqe*);

  utask_t() = default;
  utask_t(const utask_t&) = delete;
  utask_t& operator=(const utask_t&) = delete;
  utask_t& operator=(utask_t&&) = delete;
  utask_t(utask_t&&) noexcept;

  /**
   * @brief checks if the result is already available.
   * @return whether the operation has completed.
   */
  CORNET_MAYBE_UNUSED inline bool await_ready() {
    return completed;
  }

  /**
   * @brief called by the compiler when the coroutine suspends.
   * Allocates an SQE, calls prepare_fn to fill it, and sets user_data.
   * @param h the handle of the suspended coroutine, to be resumed when CQE arrives.
   */
  CORNET_MAYBE_UNUSED void await_suspend(std::coroutine_handle<> h);

  /**
   * @brief called by the compiler when the coroutine resumes.
   * @return expected<int>: value on success, error on failure.
   */
  CORNET_NODISCARD CORNET_MAYBE_UNUSED inline expected<int> await_resume() const {
    if (value < 0) {
      return unexpected(-value);
    }
    return value;
  }
    return value;
  }

  /**
   * @brief completes the task by processing the CQE and storing the result.
   * @param ctx context reference
   * @param cqe the completion queue entry from io_uring.
   */
  void complete(context_t& ctx, cqe_t cqe);

  /**
   * @brief io_uring task processor, dispatches CQE to the corresponding utask_t.
   * @param ctx context reference
   * @param cqe complete queue entry on io_uring.
   * @return 0 for success / 1 for no user_data
   */
  static int process_utask(context_t& ctx, cqe_t cqe);

  // function pointer that fills the io_uring_sqe with operation-specific data
  prepare_fn_t prepare_fn{nullptr};
  // completed flag
  bool completed{false};
  // the return value of the async system call
  int value{0};
  // callback, will be called on complete
  callback_t callback{nullptr};
  // callback user data
  void* user_data{nullptr};
  // owner context
  context_t* ctx{nullptr};
};

} // namespace cornet

#endif //CORNET_URING_TASK_H
