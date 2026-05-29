#include "core/awaiters.h"
#include "core/context.h"

namespace cornet {

close_awaiter::close_awaiter(int fd) : fd_(fd) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<close_awaiter*>(self);
    io_uring_prep_close(sqe, t->fd_);
  };
}

read_awaiter::read_awaiter(int fd, void* buf, size_t nbytes, uint64_t offset)
  : fd_(fd), buf_(buf), nbytes_(nbytes), offset_(offset) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<read_awaiter*>(self);
    io_uring_prep_read(sqe, t->fd_, t->buf_, t->nbytes_, t->offset_);
  };
}

write_awaiter::write_awaiter(int fd, const void* buf, size_t nbytes, uint64_t offset)
  : fd_(fd), buf_(buf), nbytes_(nbytes), offset_(offset) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<write_awaiter*>(self);
    io_uring_prep_write(sqe, t->fd_, t->buf_, t->nbytes_, t->offset_);
  };
}

nop_awaiter::nop_awaiter() {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    io_uring_prep_nop(sqe);
  };
}

void async_close(int fd) {
  context_t::current().io_detach([fd](io_uring_sqe* sqe) {
    io_uring_prep_close(sqe, fd);
  });
}

} // cornet
