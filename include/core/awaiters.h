#ifndef CORNET_AWAITERS_H
#define CORNET_AWAITERS_H

#include "utask.h"
#include "coro.h"
#include <sys/types.h>
#include <sys/stat.h>

namespace cornet {

/**
 * @brief close awaiter for io_uring_prep_close.
 * General-purpose async fd close, usable for any file descriptor.
 * Usage: co_await close_awaiter(fd);
 */
struct close_awaiter : utask_t {
  explicit close_awaiter(int fd);

  bool await_ready() {
    if (fd_ == -1) {
      value = 0;
      return true;
    }
    return completed;
  }

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
 * @brief shutdown awaiter for io_uring_prep_shutdown.
 * Half-close a socket (SHUT_RD, SHUT_WR, or SHUT_RDWR).
 * Usage: co_await shutdown_awaiter(fd, SHUT_WR);
 */
struct shutdown_awaiter : utask_t {
  shutdown_awaiter(int fd, int how);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int fd_;
  int how_;
};

/**
 * @brief openat awaiter for io_uring_prep_openat.
 * Async open a file relative to a directory fd.
 * Usage: auto fd = co_await openat_awaiter(dirfd, path, flags, mode);
 */
struct openat_awaiter : utask_t {
  openat_awaiter(int dirfd, const char* path, int flags, mode_t mode = 0);
 private:
  int dirfd_;
  const char* path_;
  int flags_;
  mode_t mode_;
};

/**
 * @brief splice awaiter for io_uring_prep_splice.
 * Zero-copy data transfer between two fds.
 * Usage: auto n = co_await splice_awaiter(fd_in, off_in, fd_out, off_out, nbytes, flags);
 */
struct splice_awaiter : utask_t {
  splice_awaiter(int fd_in, int64_t off_in, int fd_out, int64_t off_out, unsigned int nbytes, unsigned int flags);
 private:
  int fd_in_;
  int64_t off_in_;
  int fd_out_;
  int64_t off_out_;
  unsigned int nbytes_;
  unsigned int flags_;
};

/**
 * @brief poll_add awaiter for io_uring_prep_poll_add.
 * Wait for events on a file descriptor (POLLIN, POLLOUT, etc.).
 * Usage: auto mask = co_await poll_add_awaiter(fd, POLLIN);
 */
struct poll_add_awaiter : utask_t {
  poll_add_awaiter(int fd, unsigned int poll_mask);
 private:
  int fd_;
  unsigned int poll_mask_;
};

/**
 * @brief statx awaiter for io_uring_prep_statx.
 * Async stat for file metadata.
 * Usage: co_await statx_awaiter(AT_FDCWD, path, 0, STATX_ALL, &stx);
 */
struct statx_awaiter : utask_t {
  statx_awaiter(int dirfd, const char* path, int flags, unsigned int mask, struct statx* stx);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int dirfd_;
  const char* path_;
  int flags_;
  unsigned int mask_;
  struct statx* stx_;
};

/**
 * @brief unlinkat awaiter for io_uring_prep_unlinkat.
 * Async delete file or directory.
 * Usage: co_await unlinkat_awaiter(AT_FDCWD, path, 0);
 */
struct unlinkat_awaiter : utask_t {
  unlinkat_awaiter(int dirfd, const char* path, int flags);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int dirfd_;
  const char* path_;
  int flags_;
};

/**
 * @brief renameat awaiter for io_uring_prep_renameat.
 * Async rename/move file.
 * Usage: co_await renameat_awaiter(AT_FDCWD, old_path, AT_FDCWD, new_path, 0);
 */
struct renameat_awaiter : utask_t {
  renameat_awaiter(int old_dirfd, const char* old_path, int new_dirfd, const char* new_path, unsigned int flags);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int old_dirfd_;
  const char* old_path_;
  int new_dirfd_;
  const char* new_path_;
  unsigned int flags_;
};

/**
 * @brief mkdirat awaiter for io_uring_prep_mkdirat.
 * Async create directory.
 * Usage: co_await mkdirat_awaiter(AT_FDCWD, path, 0755);
 */
struct mkdirat_awaiter : utask_t {
  mkdirat_awaiter(int dirfd, const char* path, mode_t mode);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int dirfd_;
  const char* path_;
  mode_t mode_;
};

/**
 * @brief fsync awaiter for io_uring_prep_fsync.
 * Async flush file data to disk.
 * Usage: co_await fsync_awaiter(fd, IORING_FSYNC_DATASYNC);
 */
struct fsync_awaiter : utask_t {
  fsync_awaiter(int fd, unsigned int flags = 0);

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value < 0) return unexpected(-value);
    return {};
  }
 private:
  int fd_;
  unsigned int flags_;
};

/**
 * @brief zero-copy forward all data from fd_in to fd_out via splice.
 * Automatically creates and manages an internal pipe.
 * @param fd_in source fd (file or socket)
 * @param fd_out destination fd (file or socket)
 * @param chunk_size bytes per splice call (default 64KB)
 * @return total bytes transferred, or error
 */
coro_t<expected<size_t>> splice_forward(int fd_in, int fd_out, size_t chunk_size = 65536);

/**
 * @brief fire-and-forget close via io_uring (no coroutine overhead).
 * Usage: async_close(fd);
 */
void async_close(int fd);

// ─── Convenience wrappers (async_* free functions) ───

/**
 * @brief async shutdown socket (half-close).
 * Usage: co_await async_shutdown(fd, SHUT_WR);
 */
inline shutdown_awaiter async_shutdown(int fd, int how) {
  return shutdown_awaiter{fd, how};
}

/**
 * @brief async open file relative to current directory.
 * Usage: auto fd = co_await async_open("/path", O_RDONLY);
 */
inline openat_awaiter async_open(const char* path, int flags, mode_t mode = 0) {
  return openat_awaiter{AT_FDCWD, path, flags, mode};
}

/**
 * @brief async open file relative to a directory fd.
 * Usage: auto fd = co_await async_openat(dirfd, "file", O_RDONLY);
 */
inline openat_awaiter async_openat(int dirfd, const char* path, int flags, mode_t mode = 0) {
  return openat_awaiter{dirfd, path, flags, mode};
}

/**
 * @brief async zero-copy data transfer between two fds (at least one must be a pipe).
 * Usage: auto n = co_await async_splice(fd_in, -1, fd_out, -1, 4096);
 */
inline splice_awaiter async_splice(int fd_in, int64_t off_in, int fd_out, int64_t off_out,
                                   unsigned int nbytes, unsigned int flags = 0) {
  return splice_awaiter{fd_in, off_in, fd_out, off_out, nbytes, flags};
}

/**
 * @brief async wait for events on a file descriptor.
 * Usage: auto mask = co_await async_poll(fd, POLLIN);
 */
inline poll_add_awaiter async_poll(int fd, unsigned int poll_mask) {
  return poll_add_awaiter{fd, poll_mask};
}

/**
 * @brief async get file metadata (stat).
 * Usage: struct statx stx{}; co_await async_statx("/path", STATX_ALL, &stx);
 */
inline statx_awaiter async_statx(const char* path, unsigned int mask, struct statx* stx, int flags = 0) {
  return statx_awaiter{AT_FDCWD, path, flags, mask, stx};
}

/**
 * @brief async delete file or directory.
 * Usage: co_await async_unlink("/tmp/file.txt");
 */
inline unlinkat_awaiter async_unlink(const char* path, int flags = 0) {
  return unlinkat_awaiter{AT_FDCWD, path, flags};
}

/**
 * @brief async rename/move file.
 * Usage: co_await async_rename("/tmp/old.txt", "/tmp/new.txt");
 */
inline renameat_awaiter async_rename(const char* old_path, const char* new_path, unsigned int flags = 0) {
  return renameat_awaiter{AT_FDCWD, old_path, AT_FDCWD, new_path, flags};
}

/**
 * @brief async create directory.
 * Usage: co_await async_mkdir("/tmp/newdir");
 */
inline mkdirat_awaiter async_mkdir(const char* path, mode_t mode = 0755) {
  return mkdirat_awaiter{AT_FDCWD, path, mode};
}

/**
 * @brief async flush file data to disk.
 * Usage: co_await async_fsync(fd);
 */
inline fsync_awaiter async_fsync(int fd, unsigned int flags = 0) {
  return fsync_awaiter{fd, flags};
}

/**
 * @brief async read from any file descriptor.
 * Usage: auto n = co_await async_read(fd, buf, len);
 */
inline read_awaiter async_read(int fd, void* buf, size_t nbytes, uint64_t offset = 0) {
  return read_awaiter{fd, buf, nbytes, offset};
}

/**
 * @brief async write to any file descriptor.
 * Usage: auto n = co_await async_write(fd, buf, len);
 */
inline write_awaiter async_write(int fd, const void* buf, size_t nbytes, uint64_t offset = 0) {
  return write_awaiter{fd, buf, nbytes, offset};
}

} // namespace cornet

#endif //CORNET_AWAITERS_H
