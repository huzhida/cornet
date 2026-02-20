#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
#include "utils.h"

namespace cornet {

using cqe_t = io_uring_cqe*;

struct sqe_t {
  CORNET_MAYBE_UNUSED inline void with_data(void* user_data) const {
    io_uring_sqe_set_data(sqe, user_data);
  }

  CORNET_MAYBE_UNUSED inline sqe_t& with_flags(uint32_t flags) {
    sqe->flags |= flags;
    io_uring_sqe_set_flags(sqe, sqe->flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_read(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    io_uring_prep_read(sqe, fd, buf, nbytes, offset);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_readv(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_write(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    io_uring_prep_write(sqe, fd, buf, nbytes, offset);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_writev(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_send(int sockfd, void* buf, size_t len, int flags) {
    io_uring_prep_send(sqe, sockfd, buf, len, flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_recv(int sockfd, void* buf, size_t len, int flags) {
    io_uring_prep_recv(sqe, sockfd, buf, len, flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_accept(int sockfd, sockaddr* addr, socklen_t* addrlen, int flags) {
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_connect(int sockfd, sockaddr* addr, socklen_t addrlen) {
    io_uring_prep_connect(sqe, sockfd, addr, addrlen);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_close(int fd) {
    io_uring_prep_close(sqe, fd);
    return *this;
  }

  CORNET_MAYBE_UNUSED inline sqe_t& prep_cancel(void* user_data, int flags) {
    io_uring_prep_cancel(sqe, user_data, flags);
    return *this;
  }

  io_uring_sqe* sqe;
};

class uring_t {
public:
  explicit uring_t(uint32_t entries_nr = 32, uint32_t flags = 0);

  ~uring_t();

  uring_t(const uring_t&) = delete;

  uring_t(uring_t&& r) noexcept;

  uring_t& operator=(const uring_t&) = delete;

  uring_t& operator=(uring_t&& r) noexcept;

  inline bool submit() {
    int submit_nr = io_uring_submit(uring.get());
    if (submit_nr < 0) {
      SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
      return false;
    }
    task_nr += submit_nr;
    return true;
  }

  uint32_t wait_and_process_cqes(int (*process_fn)(cqe_t), uint32_t wait_nr = 1, int timeout_s = -1,
                                 int timeout_ns = -1,
                                 sigset_t* mask = nullptr);

  inline bool idle() const {
    return task_nr == 0;
  }

  inline size_t running_task_nr() const {
    return task_nr;
  }

  CORNET_MAYBE_UNUSED bool register_buffers(iovec* buffers, size_t buffer_nr);

  CORNET_MAYBE_UNUSED bool register_files(int* files, size_t file_nr);

  inline sqe_t new_sqe() {
    return {io_uring_get_sqe(uring.get())};
  }

private:
  uint32_t task_nr{0};
  std::unique_ptr<io_uring> uring;
  std::unique_ptr<iovec[]> registered_buffers{};
  std::unique_ptr<int[]> registered_files{};
};

} // cornet

#endif //CORNET_URING_H