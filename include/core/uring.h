#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
#include "utils.h"

namespace cornet {

using cqe_t = io_uring_cqe*;
struct sqe_t {
  void with_data(void* user_data) {
    io_uring_sqe_set_data(sqe, user_data);
  }
  sqe_t& with_flags(uint32_t flags) {
    sqe->flags |= flags;
    io_uring_sqe_set_flags(sqe, sqe->flags);
    return *this;
  }
  sqe_t& prep_read(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    io_uring_prep_read(sqe, fd, buf ,nbytes, offset);
    return *this;
  }
  sqe_t& prep_readv(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }
  sqe_t& prep_write(int fd, void* buf, uint32_t nbytes, uint64_t offset) {
    io_uring_prep_write(sqe, fd, buf ,nbytes, offset);
    return *this;
  }
  sqe_t& prep_writev(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) {
    io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
    return *this;
  }
  sqe_t& prep_send(int sockfd, void* buf, size_t len, int flags) {
    io_uring_prep_send(sqe, sockfd, buf , len, flags);
    return *this;
  }
  sqe_t& prep_recv(int sockfd, void* buf, size_t len, int flags) {
    io_uring_prep_recv(sqe, sockfd, buf , len, flags);
    return *this;
  }
  sqe_t& prep_accept(int sockfd, sockaddr* addr, socklen_t* addrlen, int flags) {
    io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    return *this;
  }
  sqe_t& prep_connect(int sockfd, sockaddr* addr, socklen_t addrlen) {
    io_uring_prep_connect(sqe, sockfd, addr, addrlen);
    return *this;
  }
  sqe_t& prep_close(int fd) {
    io_uring_prep_close(sqe, fd);
    return *this;
  }

  io_uring_sqe* sqe;
};

class uring_t {
 public:
  explicit uring_t(uint32_t entries_nr = 32, uint32_t flags = 0) : uring(std::make_unique<io_uring>()) {
    if (io_uring_queue_init(entries_nr, uring.get(), flags) < 0) {
      SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(errno));
    }
  }
  ~uring_t() {
    io_uring_queue_exit(uring.get());
  }
  uring_t(const uring_t&) = delete;
  uring_t(uring_t&& r) noexcept {
    if (this != &r) {
      this->uring = std::move(r.uring);
      this->registered_buffers = std::move(r.registered_buffers);
      this->registered_files = std::move(r.registered_files);
      r.uring = nullptr;
      r.registered_buffers = nullptr;
      r.registered_files = nullptr;
    }
  }
  uring_t& operator=(const uring_t&) = delete;
  uring_t& operator=(uring_t&& r) noexcept {
    if (this != &r) {
      this->uring = std::move(r.uring);
      this->registered_buffers = std::move(r.registered_buffers);
      this->registered_files = std::move(r.registered_files);
      r.uring = nullptr;
      r.registered_buffers = nullptr;
      r.registered_files = nullptr;
    }
    return *this;
  }
  inline bool submit() {
    if(io_uring_submit(uring.get()) < 0) {
      SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
      return false;
    }
    return true;
  }
  uint32_t wait_and_process_cqes(
      void (*process_fn)(cqe_t) ,
      int wait_nr = 1,
      int timeout_s = -1,
      int timeout_ns = -1,
      sigset_t* mask  = nullptr
  ) {
    cqe_t cqe;
    if (timeout_s == 0 && timeout_ns == 0) {
      uint32_t ret = io_uring_peek_batch_cqe(uring.get(), &cqe, wait_nr);
      if (ret == 0) {
        SPDLOG_DEBUG("Uring peek batch cqe return empty");
        return 0;
      }
    } else if (timeout_s > 0 || timeout_ns > 0) {
      __kernel_timespec ts{timeout_s, timeout_ns};
      int ret = io_uring_wait_cqes(uring.get(), &cqe,wait_nr, &ts, mask);
      if (ret == -ETIME) {
        SPDLOG_DEBUG("Uring wait_and_process_cqes timeout.");
        return 0;
      }
    } else {
      if(io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
        SPDLOG_ERROR("failed to wait io_uring cqes with error: {}", strerror(errno));
        return 0;
      }
    }
    uint32_t head, count;
    io_uring_for_each_cqe(uring.get(), head, cqe) {
      process_fn(cqe);
      ++count;
      io_uring_cqe_seen(uring.get(), cqe);
    }
    return count;
  }
  inline bool register_buffers(iovec* buffers, size_t buffer_nr) {
    for (size_t index = 0; index < buffer_nr; ++index) {
      iovec& buffer = buffers[index];
      if (posix_memalign(&buffer.iov_base, 4 * 1024, buffer.iov_len) < 0) {
        SPDLOG_ERROR("failed to posix mem align with error: {}", strerror(errno));
        return false;
      }
    }
    if (io_uring_register_buffers(uring.get(), buffers, buffer_nr) < 0) {
      SPDLOG_ERROR("failed to register buffer on io_uring with error: {}", strerror(errno));
      return false;
    }
    this->registered_buffers = std::make_unique<iovec[]>(buffer_nr);
    for (size_t index = 0; index < buffer_nr; ++index) {
      this->registered_buffers[index] = buffers[index];
    }
    return true;
  }
  inline bool register_files(int* files, size_t file_nr) {
    if (io_uring_register_files(uring.get(), files, file_nr) < 0) {
      SPDLOG_ERROR("failed to register files on io_uring with error: {}", strerror(errno));
    }
    this->registered_files = std::make_unique<int[]>(file_nr);
    for (size_t index = 0; index < file_nr; ++index) {
      this->registered_files[index] = files[index];
    }
    return true;
  }
  inline sqe_t new_sqe() {
    return {io_uring_get_sqe(uring.get())};
  }
 private:
  std::unique_ptr<io_uring> uring;
  std::unique_ptr<iovec[]> registered_buffers{};
  std::unique_ptr<int[]> registered_files{};
};

} // cornet

#endif //CORNET_URING_H
