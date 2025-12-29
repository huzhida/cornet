#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>
#include "utils.h"

namespace cornet {

class uring {
 public:
  using CQE = io_uring_cqe;
  struct SQE {
    explicit SQE(io_uring_sqe* sqe): sqe(sqe) {}
    SQE& with_data(void* user_data) {
      io_uring_sqe_set_data(sqe, user_data);
      return *this;
    }
    SQE& with_flags(uint32_t flags) {
      io_uring_sqe_set_flags(sqe, flags);
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
   private:
    io_uring_sqe* sqe;
  };

  explicit uring(uint32_t entries = 32, uint32_t flags = 0) {
    CORNET_UNIX_CHECK(io_uring_queue_init(entries, &_ring, flags))
  }
  ~uring() {
    io_uring_queue_exit(&_ring);
  }
  SQE get_sqe() {
    return SQE{io_uring_get_sqe(&_ring)};
  }
  std::vector<CQE*> wait_cqes(int wait_nr = 1, int timeout_s = 0, int timeout_ns = 0, sigset_t* mask = nullptr) {
    io_uring_cqe* cqe;
    __kernel_timespec ts{timeout_s, timeout_ns};
    CORNET_UNIX_CHECK(io_uring_wait_cqes(&_ring, &cqe,wait_nr, &ts, mask), return {});

    std::vector<CQE*> cqes;
    uint32_t head;
    io_uring_for_each_cqe(&_ring, head, cqe) {
      cqes.emplace_back(cqe);
    }
    return cqes;
  }
  void submit() {
    CORNET_UNIX_CHECK(io_uring_submit(&_ring));
  }
 private:
  io_uring _ring{};
};

} // cornet

#endif //CORNET_URING_H
