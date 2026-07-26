#include "cornet/io_uring/awaiters.h"

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include "cornet/scheduling/context.h"

namespace cornet {

close_awaiter::close_awaiter(context_t& ctx, int fd) : fd_(fd) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<close_awaiter*>(self);
    io_uring_prep_close(sqe, t->fd_);
  };
}

read_awaiter::read_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, uint64_t offset)
  : fd_(fd), buf_(buf), nbytes_(nbytes), offset_(offset) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<read_awaiter*>(self);
    io_uring_prep_read(sqe, t->fd_, t->buf_, t->nbytes_, t->offset_);
  };
}

write_awaiter::write_awaiter(context_t& ctx, int fd, const void* buf, size_t nbytes, uint64_t offset)
  : fd_(fd), buf_(buf), nbytes_(nbytes), offset_(offset) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<write_awaiter*>(self);
    io_uring_prep_write(sqe, t->fd_, t->buf_, t->nbytes_, t->offset_);
  };
}

nop_awaiter::nop_awaiter(context_t& ctx) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    io_uring_prep_nop(sqe);
  };
}

void async_close(context_t& ctx, int fd) {
  if (ctx.is_shutting_down()) {
    ::close(fd);
  } else {
    ctx.io_detach([fd](io_uring_sqe* sqe) {
      io_uring_prep_close(sqe, fd);
    });
  }
}

shutdown_awaiter::shutdown_awaiter(context_t& ctx, int fd, int how) : fd_(fd), how_(how) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<shutdown_awaiter*>(self);
    io_uring_prep_shutdown(sqe, t->fd_, t->how_);
  };
}

openat_awaiter::openat_awaiter(context_t& ctx, int dirfd, const char* path, int flags, mode_t mode)
  : dirfd_(dirfd), path_(path), flags_(flags), mode_(mode) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<openat_awaiter*>(self);
    io_uring_prep_openat(sqe, t->dirfd_, t->path_, t->flags_, t->mode_);
  };
}

splice_awaiter::splice_awaiter(context_t& ctx, int fd_in, int64_t off_in, int fd_out, int64_t off_out,
                               unsigned int nbytes, unsigned int flags)
  : fd_in_(fd_in), off_in_(off_in), fd_out_(fd_out), off_out_(off_out),
    nbytes_(nbytes), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<splice_awaiter*>(self);
    io_uring_prep_splice(sqe, t->fd_in_, t->off_in_, t->fd_out_, t->off_out_,
                         t->nbytes_, t->flags_);
  };
}

poll_add_awaiter::poll_add_awaiter(context_t& ctx, int fd, unsigned int poll_mask) : fd_(fd), poll_mask_(poll_mask) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<poll_add_awaiter*>(self);
    io_uring_prep_poll_add(sqe, t->fd_, t->poll_mask_);
  };
}

statx_awaiter::statx_awaiter(context_t& ctx, int dirfd, const char* path, int flags, unsigned int mask, struct statx* stx)
  : dirfd_(dirfd), path_(path), flags_(flags), mask_(mask), stx_(stx) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<statx_awaiter*>(self);
    io_uring_prep_statx(sqe, t->dirfd_, t->path_, t->flags_, t->mask_, t->stx_);
  };
}

unlinkat_awaiter::unlinkat_awaiter(context_t& ctx, int dirfd, const char* path, int flags)
  : dirfd_(dirfd), path_(path), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<unlinkat_awaiter*>(self);
    io_uring_prep_unlinkat(sqe, t->dirfd_, t->path_, t->flags_);
  };
}

renameat_awaiter::renameat_awaiter(context_t& ctx, int old_dirfd, const char* old_path, int new_dirfd, const char* new_path, unsigned int flags)
  : old_dirfd_(old_dirfd), old_path_(old_path), new_dirfd_(new_dirfd), new_path_(new_path), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<renameat_awaiter*>(self);
    io_uring_prep_renameat(sqe, t->old_dirfd_, t->old_path_, t->new_dirfd_, t->new_path_, t->flags_);
  };
}

mkdirat_awaiter::mkdirat_awaiter(context_t& ctx, int dirfd, const char* path, mode_t mode)
  : dirfd_(dirfd), path_(path), mode_(mode) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<mkdirat_awaiter*>(self);
    io_uring_prep_mkdirat(sqe, t->dirfd_, t->path_, t->mode_);
  };
}

fsync_awaiter::fsync_awaiter(context_t& ctx, int fd, unsigned int flags) : fd_(fd), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<fsync_awaiter*>(self);
    io_uring_prep_fsync(sqe, t->fd_, t->flags_);
  };
}

coro_t<expected<size_t>> splice_forward(context_t& ctx, int fd_in, int fd_out, size_t chunk_size) {
  int pipefd[2];
  if (::pipe(pipefd) < 0) {
    co_return unexpected(errno);
  }
  size_t total = 0;
  for (;;) {
    auto n = co_await splice_awaiter(ctx, fd_in, -1, pipefd[1], -1, chunk_size, SPLICE_F_MOVE);
    if (!n) {
      ::close(pipefd[0]);
      ::close(pipefd[1]);
      co_return unexpected(n.error());
    }
    if (*n == 0) break;
    int remaining = *n;
    while (remaining > 0) {
      auto w = co_await splice_awaiter(ctx, pipefd[0], -1, fd_out, -1, remaining, SPLICE_F_MOVE);
      if (!w) {
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        co_return unexpected(w.error());
      }
      remaining -= *w;
      total += *w;
    }
  }
  ::close(pipefd[0]);
  ::close(pipefd[1]);
  co_return total;
}

} // cornet
