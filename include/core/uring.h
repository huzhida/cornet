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

/**
 * @brief wrapper for io_uring_sqe, providing a fluent interface for request preparation.
 */
struct sqe_t {
  /**
   * @brief set user data for the sqe
   * @param user_data pointer to user-defined data (usually awaiter or task)
   */
  CORNET_MAYBE_UNUSED inline void with_data(void* user_data) const {
    if(!sqe) return;
    io_uring_sqe_set_data(sqe, user_data);
  }

  /**
   * @brief set flags for the sqe (e.g., IOSQE_IO_LINK, IOSQE_ASYNC)
   * @param flags bitmask of io_uring_sqe_flags
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& with_flags(uint32_t flags) {
    if(!sqe) return *this;
    io_uring_sqe_set_flags(sqe, sqe->flags | flags);
    return *this;
  }

  /**
   * @brief prepare a standard read operation
   * @param fd file descriptor
   * @param buf buffer to read into
   * @param nbytes number of bytes to read
   * @param offset file offset
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_read(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    if(!sqe) return *this;
    io_uring_prep_read(sqe, fd, buf, nbytes, offset);
    return *this;
  }

  /**
   * @brief prepare a vectored read operation
   * @param fd file descriptor
   * @param iovecs pointer to iovec array
   * @param nr_vecs number of iovecs
   * @param offset file offset
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_readv(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    if(!sqe) return *this;
    io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }

  /**
   * @brief prepare a standard write operation
   * @param fd file descriptor
   * @param buf buffer to write from
   * @param nbytes number of bytes to write
   * @param offset file offset
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_write(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    if(!sqe) return *this;
    io_uring_prep_write(sqe, fd, buf, nbytes, offset);
    return *this;
  }

  /**
   * @brief prepare a vectored write operation
   * @param fd file descriptor
   * @param iovecs pointer to iovec array
   * @param nr_vecs number of iovecs
   * @param offset file offset
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_writev(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    if(!sqe) return *this;
    io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }

  /**
   * @brief prepare a send operation
   * @param sockfd socket file descriptor
   * @param buf data buffer
   * @param len data length
   * @param flags MSG_* flags
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_send(int sockfd, void* buf, size_t len, int flags) {
    if(!sqe) return *this;
    io_uring_prep_send(sqe, sockfd, buf, len, flags);
    return *this;
  }

  /**
   * @brief prepare a recv operation
   * @param sockfd socket file descriptor
   * @param buf buffer to store received data
   * @param len buffer length
   * @param flags MSG_* flags
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_recv(int sockfd, void* buf, size_t len, int flags) {
    if(!sqe) return *this;
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_sendmsg(int sockfd, struct msghdr* msg, int flags) {
    if(!sqe) return *this;
    io_uring_prep_sendmsg(sqe, sockfd, msg, flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_recvmsg(int sockfd, struct msghdr* msg, int flags) {
    if(!sqe) return *this;
    io_uring_prep_recvmsg(sqe, sockfd, msg, flags);
    return *this;
  }

  /**
   * @brief prepare an accept operation
   * @param sockfd listening socket
   * @param addr sockaddr pointer to store client address
   * @param addrlen pointer to sockaddr length
   * @param flags SOCK_NONBLOCK / SOCK_CLOEXEC
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_accept(int sockfd, sockaddr* addr, socklen_t* addrlen, int flags) {
    if(!sqe) return *this;
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    return *this;
  }

  /**
   * @brief prepare a connect operation
   * @param sockfd socket file descriptor
   * @param addr destination address
   * @param addrlen address length
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_connect(int sockfd, sockaddr* addr, socklen_t addrlen) {
    if(!sqe) return *this;
    io_uring_prep_connect(sqe, sockfd, addr, addrlen);
    return *this;
  }

  /**
   * @brief prepare a close operation
   * @param fd file descriptor to close
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_close(int fd) {
    if(!sqe) return *this;
    io_uring_prep_close(sqe, fd);
    return *this;
  }

  /**
   * @brief prepare a cancellation operation
   * @param user_data the user_data of the request to cancel
   * @param flags IORING_ASYNC_CANCEL_* flags
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_cancel(void* user_data, int flags) {
    if(!sqe) return *this;
    io_uring_prep_cancel(sqe, user_data, flags);
    return *this;
  }

  /**
   * @brief prepare a link_timout operation
   * @tparam Rep storage unit
   * @tparam Period ratio
   * @param timeout timeout period
   * @param count =0 represent only care about timeout,  >0 represent wakeup when `count` number cqe comes
   * @param flags such as IORING_TIMEOUT_ABS / IORING_TIMEOUT_BOOTTIME / IORING_TIMEOUT_REALTIME
   * @return self reference
   */
  template<typename Rep, typename Period>
  CORNET_MAYBE_UNUSED inline sqe_t& prep_timeout(std::chrono::duration<Rep,Period> timeout, int count, int flags) {
    if(!sqe) return *this;
    ts = to_kernel_timespec(timeout);
    io_uring_prep_timeout(sqe, &ts, count, flags);
    return *this;
  }

  /**
   * @brief prepare a link_timout operation
   * @tparam Rep storage unit
   * @tparam Period ratio
   * @param timeout timeout period
   * @param flags such as IORING_TIMEOUT_ABS / IORING_TIMEOUT_BOOTTIME / IORING_TIMEOUT_REALTIME
   * @return self reference
   */
  template<typename Rep, typename Period>
  CORNET_MAYBE_UNUSED inline sqe_t& prep_link_timeout(std::chrono::duration<Rep,Period> timeout, int flags) {
    if(!sqe) return *this;
    ts = to_kernel_timespec(timeout);
    io_uring_prep_link_timeout(sqe, &ts, flags);
    return *this;
  }

  /**
   * @brief used for empty sqe op
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_nop() {
    if(!sqe) return *this;
    io_uring_prep_nop(sqe);
    return *this;
  }

  io_uring_sqe* sqe;
  __kernel_timespec ts{};
};

struct context_t;

/**
 * @brief io_uring context wrapper
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
   * @brief submit all prepared SQEs to kernel
   * @return < 0 submit failed, > 0 submitted count
   */
  int submit();

  /**
   * @brief post process overflow SQEs and flush when need
   * @return submitted sqe count in postprocess
   */
  int postprocess();

  /**
   * @brief wait for CQEs and process them
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param timeout timeout period
   * @param mask signal mask
   * @return number of processed CQEs
   */
  template<typename Rep, typename Period>
  uint32_t wait_cqes(int (*process_fn)(context_t &, cqe_t),context_t &ctx,uint32_t wait_nr,
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
   * @brief wait for CQEs and process them (infinity wait)
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param mask signal mask
   * @return number of processed CQEs
   */
  uint32_t wait_cqes(int (*process_fn)(context_t &, cqe_t),context_t &ctx,uint32_t wait_nr = 1, sigset_t *mask=nullptr);

  /**
   * @brief peek available CQEs without blocking
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param peek_nr maximum number of CQEs to peek
   * @return number of processed CQEs
   */
  uint32_t peek_cqes(int (*process_fn)(context_t&, cqe_t), context_t& ctx, uint32_t peek_nr = 1);

  /**
   * @brief space left in io uring sq
   * @return remain sqe count
   */
  inline uint32_t space_left() const {
    return io_uring_sq_space_left(uring.get());
  }

  /**
   * @brief get sq size
   * @return sq size
   */
  inline size_t sq_size() const {
    return uring->sq.ring_sz;
  }

  /**
   * @brief check if there are no pending tasks
   * @return true if task count is zero
   */
  inline bool idle() const {
    return task_nr == 0;
  }

  /**
   * @brief whether sqe overflow
   * @return true if overflow / ...
   */
  inline bool overflow() const {
    return !sm.empty();
  }

  /**
   * @brief whether io uring sq about full
   * @return true if about full / ...
   */
  inline bool about_full() const {
    return remain_sqe_nr == 0;
  }

  /**
   * @brief if overflow and flush called, will submit overflow tasks immediately afterwards submit once.
   */
  inline void flush() {
    if (overflow()) need_flush = true;
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

  /**
   * @brief get a new SQE from the ring
   * @return sqe_t wrapper
   */
  inline sqe_t new_sqe() {
    auto sqe = io_uring_get_sqe(uring.get());
    if (!sqe) {
      return {sm.acquire_overflow_sqe()};
    }
    --remain_sqe_nr;
    return {sqe};
  }

 private:

  struct sqe_manager {
    static constexpr auto OVERFLOW_CAPACITY = 16;
    io_uring_sqe pool[OVERFLOW_CAPACITY];
    int head = 0;
    int tail = 0;

    inline bool empty() const {
      return head == tail;
    }
    inline bool full() const {
      return ((tail + 1) & (OVERFLOW_CAPACITY - 1)) == head;
    }
    sqe_t acquire_overflow_sqe() {
      if (full()) {
        CORNET_FATAL("reaching the maximum tolerance limit for overflow, try increase OVERFLOW_CAPACITY");
      }
      auto sqe = &pool[tail];
      tail = (tail + 1) & (OVERFLOW_CAPACITY - 1);
      return {sqe};
    }

    io_uring_sqe* flush_overflow_sqe() {
      if (empty()) {
        CORNET_FATAL("should never flush empty overflow pool");
      }
      auto sqe = &pool[head];
      head = (head + 1) & (OVERFLOW_CAPACITY - 1);
      return sqe;
    }
  };

  // submitted task count
  uint32_t task_nr{0};
  // remain sqe count
  uint32_t remain_sqe_nr{0};
  // io_uring handle
  std::unique_ptr<io_uring> uring;
  // sqe overflow manager
  sqe_manager sm{};
  // need submit overflow tasks immediately afterwards submit once
  bool need_flush{false};
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