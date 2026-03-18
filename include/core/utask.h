#ifndef CORNET_URING_TASK_H
#define CORNET_URING_TASK_H

#include "task.h"
#include "uring.h"

namespace cornet {

struct context_t;

struct action {
  using callback_t = void(*)(context_t&, void*);
  void* data;
  callback_t callback;
};

/**
 * @brief io_uring specific task/awaiter base struct.
 * * This struct implements the C++ coroutine Awaiter interface,
 * allowing io_uring operations to be used with 'co_await'.
 */
struct utask_t : task_t {
  /**
   * @param ctx the owner context (io_uring manager)
   */
  explicit utask_t(sqe_t sqe)
      : sqe(sqe) {}

  /**
   * @brief checks if the result is already available.
   * @return always false to ensure the coroutine suspends and waits for CQE.
   */
  CORNET_MAYBE_UNUSED inline bool await_ready() {
    return completed;
  }

  /**
   * @brief called by the compiler when the coroutine suspends.
   * @param handle the handle of the suspended coroutine, to be resumed when CQE arrives.
   */
  CORNET_MAYBE_UNUSED void await_suspend(std::coroutine_handle<> handle);

  /**
   * @brief called by the compiler when the coroutine resumes.
   * @return the result of the io_uring operation (e.g., bytes read or error code).
   */
  CORNET_NODISCARD CORNET_MAYBE_UNUSED inline int await_resume() const {
    return value;
  }

  /**
   * @brief completes the task by processing the CQE and storing the result.
   * @param ctx context reference
   * @param cqe the completion queue entry from io_uring.
   */
  void complete(context_t& ctx,cqe_t cqe);

  /**
   * @brief completed flag
   */
  bool completed{false};

  /**
   * @brief callback flag, when flag = true, it's represent handle is a void (*callback)
   */
  bool callback{false};

  /**
   * @brief the return value of the async system call.
   */
  int value{0};

  /**
   * @brief io_uring_sqe wrapper for this task
   */
  sqe_t sqe{nullptr};
};

} // namespace cornet

#endif //CORNET_URING_TASK_H