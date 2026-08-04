#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>

#include <memory>

#include "cornet/base/defines.h"
#include "cornet/base/metrics.h"
#include "cornet/scheduling/task_tracker.h"
#include "cornet/utils/config.h"

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
   * @brief initialize io_uring instance.
   * Reads capacity and flags from config.
   * @param tracker work tracker
   * @param config configuration pointer (may be nullptr)
   */
   uring_t(task_tracker_t& tracker, config_t* config);

   ~uring_t();

   uring_t(const uring_t&) = delete;

   uring_t(uring_t&& r) = delete;

   uring_t& operator=(const uring_t&) = delete;

   uring_t& operator=(uring_t&& r) = delete;

   /**
    * @brief get an SQE from the ring. If the ring is full, submits pending SQEs first.
    * @return pointer to an available io_uring_sqe
    */
   io_uring_sqe* get_sqe();

   /**
    * @brief atomically get multiple SQEs from the ring.
    * If SQ space is insufficient, submits pending SQEs first, then acquires all at once.
    * Guarantees all returned SQEs are in the same submission batch (safe for IOSQE_IO_LINK).
    * @param out pointer to array that receives the SQE pointers
    * @param n number of SQEs to acquire
    */
   void get_sqes(io_uring_sqe** out, size_t n);

   /**
    * @brief submit all prepared SQEs to kernel
    * @return < 0 submit failed, > 0 submitted count
    */
   int submit();

   /**
    * @brief wait for CQEs and process them (with timeout)
    * @param ctx context reference
    * @param wait_nr minimum number of CQEs to wait for
    * @param timeout timeout period
    * @param mask signal mask
    * @return number of processed CQEs
    */
   template <typename Rep, typename Period>
   uint32_t wait_cqes(context_t& ctx, uint32_t wait_nr, std::chrono::duration<Rep, Period> timeout,
                      sigset_t* mask = nullptr) {
     CORNET_METRICS_ADD(metrics_->wait_calls);
     cqe_t cqe;
     __kernel_timespec ts = to_kernel_timespec(timeout);
     int ret = io_uring_wait_cqes(uring.get(), &cqe, wait_nr, &ts, mask);
     if (ret == -ETIME) {
       CORNET_METRICS_ADD(metrics_->wait_timeouts);
       return 0;
     }
     uint32_t n = process_cqes(ctx, cqe);
     CORNET_METRICS_ADD(metrics_->wait_cqes_processed);
     return n;
  }

  /**
   * @brief wait for CQEs and process them (blocking, no timeout)
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param mask signal mask
   * @return number of processed CQEs
   */
  uint32_t wait_cqes(context_t &ctx, uint32_t wait_nr = 1, sigset_t *mask = nullptr);

  /**
   * @brief peek available CQEs without blocking
   * @param ctx context reference
   * @return number of processed CQEs
   */
  uint32_t peek_cqes( context_t& ctx);

  /**
   * @brief get raw io_uring pointer for low-level operations
   * @return raw io_uring pointer
   */
  inline io_uring* raw() { return uring.get(); }

  /**
   * @brief get number of currently inflight tasks
   * @return task count
   */
  inline size_t running_task_nr() const {
    return tracker_.inflight_io();
  }

 private:
  // work tracker (owned by context_t, bound during construction)
  task_tracker_t& tracker_;
  // saved config pointer for use in policy factory
  config_t* config_ = nullptr;
  // io_uring handle
  std::unique_ptr<io_uring> uring;
  #ifdef CORNET_METRICS
  // metrics pointer (set by context after construction)
  context_metrics_t* metrics_{nullptr};
  #endif

  uint32_t process_cqes(context_t& ctx, cqe_t cqe);

  friend struct context_t;
};

} // cornet

#endif //CORNET_URING_H
