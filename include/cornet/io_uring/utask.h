#ifndef CORNET_URING_TASK_H
#define CORNET_URING_TASK_H

#include <concepts>
#include <type_traits>

#include "cornet/base/task.h"
#include "cornet/io_uring/uring.h"
#include "cornet/base/expected.h"

namespace cornet {

struct context_t;

/**
 * @brief io_uring specific task/awaiter base struct.
 * This struct implements the C++ coroutine Awaiter interface,
 * allowing io_uring operations to be used with 'co_await'.
 * Subclasses store operation parameters and set prepare_fn to fill the SQE at submission time.
 */
struct utask_t : task_t {
  using prepare_fn_t = void(*)(utask_t*, io_uring_sqe*);

  utask_t() = default;
  ~utask_t();
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
   * @return 0 if CQE was dispatched to a task, 1 if user_data was null (no task to dispatch)
   */
  static int process_utask(context_t& ctx, cqe_t cqe);

  /**
   * @brief allocate a slot and fill the given SQE via prepare_fn, then set its user_data.
   * Used both by await_suspend (single SQE) and by linked-submission awaiters
   * (e.g. with_timeout) that need to fill an externally-provided SQE.
   * @param sqe the SQE to fill.
   */
  void prepare_into(io_uring_sqe* sqe);

  /**
   * @brief reclassify this op as framework-internal.
   * Framework io (watcher reads, the shutdown drain timer, cancellation) must
   * not count as user work, otherwise the context can never reach user_idle()
   * and graceful shutdown never starts draining. Must be called before the op
   * is awaited. See as_system() for the intended spelling.
   */
  void mark_system() { user_work = false; }

  /**
   * @brief whether this op counts as user work.
   * @return true for application io, false for framework-internal io
   */
  CORNET_NODISCARD bool is_user_work() const { return user_work; }

#if !CORNET_LINUX_VERSION_GE_5_19
  /**
   * @brief the io_uring user_data token for this task (encoded slot index + generation).
   * Used by cancellation to target the inflight operation. 0 means not submitted.
   */
  CORNET_NODISCARD inline uint64_t io_token() const { return slot_data_; }
#endif

  /**
   * @brief the raw result of the async syscall (negative errno on failure).
   * Used by wrapping awaiters (e.g. with_timeout) to inspect the underlying
   * outcome before reinterpreting it.
   */
  CORNET_NODISCARD inline int io_result() const { return value; }

protected:
  // function pointer that fills the io_uring_sqe with operation-specific data
  prepare_fn_t prepare_fn{nullptr};
  // completed flag (set by subclass on early failure, or by complete())
  bool completed{false};
  // whether this op counts as user work for drain detection. Defaults to true
  // so that forgetting to classify a framework op keeps the context alive
  // (fail-safe) instead of draining it out from under running work.
  bool user_work{true};
  // whether this op has been counted into the tracker's user io total
  bool user_io_counted{false};
  // the return value of the async system call
  int value{0};
  // owner context
  context_t* ctx{nullptr};

#if !CORNET_LINUX_VERSION_GE_5_19
private:
  // encoded slot data (index + generation) for safe lifetime tracking
  uint64_t slot_data_{0};
#endif
};

/**
 * @brief mark an io operation as framework-internal before awaiting it.
 *
 * Usage: co_await as_system(async_read(ctx, fd, buf, len));
 *
 * Framework io must not count as user work, or user_idle() can never become
 * true and graceful shutdown never starts draining. The default is "user", so
 * omitting this call keeps the context alive (fail-safe) — only deliberately
 * writing as_system() can move an op out of the user account.
 * @tparam A utask_t-derived awaiter type
 * @param op the operation to reclassify
 * @return the same operation, moved
 */
template<typename A>
  requires std::derived_from<std::decay_t<A>, utask_t>
std::decay_t<A> as_system(A&& op) {
  op.mark_system();
  return std::forward<A>(op);
}

} // namespace cornet

#endif //CORNET_URING_TASK_H
