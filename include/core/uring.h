#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
#include "utils.h"

namespace cornet {

using cqe_t = io_uring_cqe*;
struct sqe_t {
  sqe_t& with_data(void* user_data) {
    io_uring_sqe_set_data(sqe, user_data);
    return *this;
  }
  sqe_t& with_flags(uint32_t flags) {
    sqe->flags |= flags;
    io_uring_sqe_set_flags(sqe, sqe->flags);
    return *this;
  }
  void prep_read(int fd, void* buf, uint32_t nbytes, uint64_t offset) const {
    io_uring_prep_read(sqe, fd, buf ,nbytes, offset);
  }
  void prep_readv(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) const {
    io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
  }
  void prep_write(int fd, void* buf, uint32_t nbytes, uint64_t offset) const {
    io_uring_prep_write(sqe, fd, buf ,nbytes, offset);
  }
  void prep_writev(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) const {
    io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
  }
  void prep_send(int sockfd, void* buf, size_t len, int flags) const {
    io_uring_prep_send(sqe, sockfd, buf , len, flags);
  }
  void prep_recv(int sockfd, void* buf, size_t len, int flags) const {
    io_uring_prep_recv(sqe, sockfd, buf , len, flags);
  }
  void prep_accept(int sockfd, sockaddr* addr, socklen_t* addrlen, int flags) const {
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
  }
  void prep_connect(int sockfd, sockaddr* addr, socklen_t addrlen) const {
    io_uring_prep_connect(sqe, sockfd, addr, addrlen);
  }
  void prep_close(int fd) const {
    io_uring_prep_close(sqe, fd);
  }

  io_uring_sqe* sqe;
};

class uring_t {
 public:
  explicit uring_t(uint32_t entries_nr = 32, uint32_t flags = 0) {
    CORNET_UNIX_CHECK(io_uring_queue_init(entries_nr, &uring, flags));
  }
  inline bool submit() {
    CORNET_UNIX_CHECK(io_uring_submit(&uring), return false;);
    return true;
  }
  uint32_t wait_and_process_cqes(
      void (*process_fn)(cqe_t) ,
      int wait_nr = 1,
      int timeout_s = 0,
      int timeout_ns = 0,
      sigset_t* mask  = nullptr
  ) {
    cqe_t cqe;
    if (timeout_s > 0 || timeout_ns > 0) {
      __kernel_timespec ts{timeout_s, timeout_ns};
      int ret = io_uring_wait_cqes(&uring, &cqe,wait_nr, &ts, mask);
      if (ret == -ETIME) {
        SPDLOG_INFO("Uring wait_and_process_cqes timeout.");
        return 0;
      }
    } else {
      CORNET_UNIX_CHECK(io_uring_wait_cqes(&uring, &cqe,wait_nr, nullptr, mask), return 0;);
    }
    uint32_t head, count;
    io_uring_for_each_cqe(&uring, head, cqe) {
      process_fn(cqe);
      ++count;
      io_uring_cqe_seen(&uring, cqe);
    }
    return count;
  }
  inline bool register_buffers(iovec* buffers, size_t buffer_nr) {
    for (size_t index = 0; index < buffer_nr; ++index) {
      iovec& buffer = buffers[index];
      CORNET_UNIX_CHECK(posix_memalign(&buffer.iov_base, 4 * 1024, buffer.iov_len), return false;);
    }
    CORNET_UNIX_CHECK(io_uring_register_buffers(&uring, buffers, buffer_nr), return false;);
    this->registered_buffers = std::make_unique<iovec[]>(buffer_nr);
    for (size_t index = 0; index < buffer_nr; ++index) {
      this->registered_buffers[index] = buffers[index];
    }
    return true;
  }
  inline bool register_files(int* files, size_t file_nr) {
    CORNET_UNIX_CHECK(io_uring_register_files(&uring, files, file_nr), return false;);
    this->registered_files = std::make_unique<int[]>(file_nr);
    for (size_t index = 0; index < file_nr; ++index) {
      this->registered_files[index] = files[index];
    }
    return true;
  }
  inline sqe_t new_sqe() {
    return {io_uring_get_sqe(&uring)};
  }
 private:
  io_uring uring{};
  std::unique_ptr<iovec[]> registered_buffers{};
  std::unique_ptr<int[]> registered_files{};
};

} // cornet

#endif //CORNET_URING_H
