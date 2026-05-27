#ifndef CORNET_AWAITERS_H
#define CORNET_AWAITERS_H

#include "utask.h"
#include "coro.h"

namespace cornet {

/**
 * @brief close awaiter for io_uring_prep_close.
 * General-purpose async fd close, usable for any file descriptor.
 * Usage: co_await close_awaiter(fd);
 */
struct close_awaiter : utask_t {
  explicit close_awaiter(int fd);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int fd_;
};

/**
 * @brief read awaiter for io_uring_prep_read.
 * General-purpose async read from any file descriptor.
 * Usage: auto n = co_await read_awaiter(fd, buf, len, offset);
 */
struct read_awaiter : utask_t {
  read_awaiter(int fd, void* buf, size_t nbytes, uint64_t offset = 0);
 private:
  int fd_;
  void* buf_;
  size_t nbytes_;
  uint64_t offset_;
};

/**
 * @brief write awaiter for io_uring_prep_write.
 * General-purpose async write to any file descriptor.
 * Usage: auto n = co_await write_awaiter(fd, buf, len, offset);
 */
struct write_awaiter : utask_t {
  write_awaiter(int fd, const void* buf, size_t nbytes, uint64_t offset = 0);
 private:
  int fd_;
  const void* buf_;
  size_t nbytes_;
  uint64_t offset_;
};

/**
 * @brief nop awaiter for io_uring_prep_nop.
 * Submits a no-op to io_uring, useful for wakeup or synchronization.
 * Usage: co_await nop_awaiter();
 */
struct nop_awaiter : utask_t {
  nop_awaiter();

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
};

/**
 * @brief async close a file descriptor via io_uring.
 * Convenience coroutine wrapping close_awaiter, suitable for spawn().
 * Usage: ctx.spawn(async_close(fd));
 */
inline coro_t<void> async_close(int fd) {
  co_await close_awaiter(fd);
}

} // namespace cornet

#endif //CORNET_AWAITERS_H
