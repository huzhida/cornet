#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
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
    io_uring_sqe_set_data(sqe, user_data);
  }

  /**
   * @brief set flags for the sqe (e.g., IOSQE_IO_LINK, IOSQE_ASYNC)
   * @param flags bitmask of io_uring_sqe_flags
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& with_flags(uint32_t flags) {
    sqe->flags |= flags;
    io_uring_sqe_set_flags(sqe, sqe->flags);
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
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
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
    io_uring_prep_connect(sqe, sockfd, addr, addrlen);
    return *this;
  }

  /**
   * @brief prepare a close operation
   * @param fd file descriptor to close
   * @return self reference
   */
  CORNET_MAYBE_UNUSED inline sqe_t& prep_close(int fd) {
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
    io_uring_prep_cancel(sqe, user_data, flags);
    return *this;
  }

  io_uring_sqe* sqe;
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
   * @return submit ok?
   */
  bool submit();

  /**
   * @brief wait for CQEs and process them
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param wait_nr minimum number of CQEs to wait for
   * @param timeout_s timeout seconds
   * @param timeout_ns timeout nanoseconds
   * @param mask signal mask
   * @return number of processed CQEs
   */
  uint32_t wait_cqes(int (*process_fn)(context_t&, cqe_t), context_t& ctx, uint32_t wait_nr = 1,
                     int timeout_s = -1, int timeout_ns = -1, sigset_t* mask = nullptr);

  /**
   * @brief peek available CQEs without blocking
   * @param process_fn callback function for each CQE
   * @param ctx context reference
   * @param peek_nr maximum number of CQEs to peek
   * @param mask signal mask
   * @return number of processed CQEs
   */
  uint32_t peek_cqes(int (*process_fn)(context_t&, cqe_t), context_t& ctx,
                     uint32_t peek_nr = 1, sigset_t* mask = nullptr);

  /**
   * @brief check if the SQE ring is full
   * @return true if no more SQEs can be allocated before submit
   */
  inline bool full() const {
    return remain_sqe_nr == 0;
  }

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

  /**
   * @brief get a new SQE from the ring
   * @return sqe_t wrapper
   */
  inline sqe_t new_sqe() {
    auto sqe = io_uring_get_sqe(uring.get());
    if (!sqe) {
      if (!submit() || (sqe = io_uring_get_sqe(uring.get()))) {
        SPDLOG_ERROR("io_uring sqe exhausted or try submit failed.");
        return {nullptr};
      }
    }
    --remain_sqe_nr;
    return {sqe};
  }

 private:
  // submitted task count
  uint32_t task_nr{0};
  // remain sqe count
  uint32_t remain_sqe_nr{0};
  // io_uring handle
  std::unique_ptr<io_uring> uring;
  // registered buffers
  std::unique_ptr<iovec[]> registered_buffers{};
  // registered file descriptors
  std::unique_ptr<int[]> registered_files{};
};

} // cornet

#endif //CORNET_URING_H