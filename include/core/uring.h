#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
#include <queue>
#include "utils/utils.h"

namespace cornet {

/**
 * @brief io_uring completion queue entry type alias
 */
using cqe_t = io_uring_cqe*;

struct context_t;

/**
 * @brief io_uring context wrapper.
 * Manages the io_uring instance, SQE allocation, submission, and CQE processing.
 */
class uring_t {
 public:
  /**
   * @brief initialize io_uring instance
   * @param entries_nr number of entries in the ring
   * @param flags io_uring_setup flags
   */
  explicit uring_t(uint32_t entries_nr = 32, uint32_t flags = 0);

  ~uring_t();

  uring_t(const uring_t&) = delete;

  uring_t(uring_t&& r) noexcept;

  uring_t& operator=(const uring_t&) = delete;

  uring_t& operator=(uring_t&& r) noexcept;

  /**
   * @brief get an SQE from the ring. If the ring is full, submits pending SQEs first.
   * @return pointer to an available io_uring_sqe
   */
  io_uring_sqe* get_sqe();

  /**
   * @brief submit all prepared SQEs to kernel
   * @return < 0 submit failed, > 0 submitted count
   */
  int submit();

  /**
   * @brief wait for CQEs and process them (with timeout)
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param timeout timeout period
   * @param mask signal mask
   * @return number of processed CQEs
   */
  template<typename Rep, typename Period>
  uint32_t wait_cqes(int (*process_fn)(context_t &, cqe_t), context_t &ctx, uint32_t wait_nr,
                     std::chrono::duration<Rep, Period> timeout, sigset_t *mask = nullptr) {
    cqe_t cqe;
    __kernel_timespec ts = to_kernel_timespec(timeout);
    int ret = io_uring_wait_cqes(uring.get(), &cqe, wait_nr, &ts, mask);
    if (ret == -ETIME) {
      SPDLOG_DEBUG("Uring wait_and_process_cqes timeout.");
      return 0;
    }
    return process_cqes(process_fn, ctx, cqe);
  }

  /**
   * @brief wait for CQEs and process them (blocking, no timeout)
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param mask signal mask
   * @return number of processed CQEs
   */
  uint32_t wait_cqes(int (*process_fn)(context_t &, cqe_t), context_t &ctx, uint32_t wait_nr = 1, sigset_t *mask = nullptr);

  /**
   * @brief peek available CQEs without blocking
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param peek_nr maximum number of CQEs to peek
   * @return number of processed CQEs
   */
  uint32_t peek_cqes(int (*process_fn)(context_t&, cqe_t), context_t& ctx, uint32_t peek_nr = 1);

  /**
   * @brief get raw io_uring pointer for low-level operations
   * @return raw io_uring pointer
   */
  inline io_uring* raw() { return uring.get(); }

  /**
   * @brief check if there are no pending tasks
   * @return true if task count is zero
   */
  inline bool idle() const {
    return task_nr == 0;
  }

  /**
   * @brief get number of currently inflight tasks
   * @return task count
   */
  inline size_t running_task_nr() const {
    return task_nr;
  }

  /**
   * @brief register fixed buffers for performance optimization
   * @param buffers pointer to iovec array
   * @param buffer_nr number of buffers
   * @return register ok?
   */
  CORNET_MAYBE_UNUSED bool register_buffers(iovec* buffers, size_t buffer_nr);

  /**
   * @brief register fixed files for performance optimization
   * @param files pointer to fd array
   * @param file_nr number of files
   * @return register ok?
   */
  CORNET_MAYBE_UNUSED bool register_files(int* files, size_t file_nr);

 private:
  // submitted task count
  uint32_t task_nr{0};
  // io_uring handle
  std::unique_ptr<io_uring> uring;
  // registered buffers
  std::unique_ptr<iovec[]> registered_buffers{};
  // registered buffer count
  size_t registered_buffer_nr{0};
  // registered file descriptors
  std::unique_ptr<int[]> registered_files{};

  uint32_t process_cqes(int (*process_fn)(context_t&, cqe_t), context_t& ctx, cqe_t cqe);
};

} // cornet

#endif //CORNET_URING_H
