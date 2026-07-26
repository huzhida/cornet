#ifndef CORNET_CONTEXT_CANCELLATION_H
#define CORNET_CONTEXT_CANCELLATION_H

#include "cornet/coroutine/coro.h"
#include "cornet/base/expected.h"

namespace cornet {

class context_t;
struct cancel_awaiter;

/**
 * @brief Handles cancellation of all pending io_uring operations.
 * Uses IORING_ASYNC_CANCEL_ANY on 5.19+ kernels, falls back to
 * per-slot cancellation on older kernels.
 *
 * This is the only place where io_uring cancellation API is directly used,
 * making it possible to replace with a different implementation for
 * non-io_uring backends.
 */
class context_cancellation_t {
 public:
  /**
   * @brief Initialize with a context_t reference.
   * Must be called before using cancel_pending_io().
   */
  void init(context_t& ctx);

  /**
   * @brief Cancel all pending io_uring operations.
   * @return coro_t<expected<int>>: coroutine that yields canceled task count on success, error on failure
   */
  CORNET_NODISCARD coro_t<expected<int>> cancel_pending_io();

 private:
  context_t* ctx_{nullptr};
};

} // namespace cornet

#endif // CORNET_CONTEXT_CANCELLATION_H
