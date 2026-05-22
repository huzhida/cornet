#include "core/uring.h"

namespace cornet {

uring_t::uring_t(uint32_t entries_nr, uint32_t flags)
  : uring(std::make_unique<io_uring>()) {
  if (io_uring_queue_init(entries_nr, uring.get(), flags) < 0) {
    CORNET_FATAL("failed to init io_uring queue with error: {}", strerror(errno));
  }
}

uring_t::~uring_t() {
  if (registered_buffers) {
    for (size_t i = 0; i < registered_buffer_nr; ++i) {
      free(registered_buffers[i].iov_base);
    }
  }
  if (uring) io_uring_queue_exit(uring.get());
}

uring_t::uring_t(uring_t&& r) noexcept {
  if (this != &r) {
    this->uring = std::move(r.uring);
    this->task_nr = r.task_nr;
    this->registered_buffers = std::move(r.registered_buffers);
    this->registered_buffer_nr = r.registered_buffer_nr;
    this->registered_files = std::move(r.registered_files);
    r.uring = nullptr;
    r.task_nr = 0;
    r.registered_buffers = nullptr;
    r.registered_buffer_nr = 0;
    r.registered_files = nullptr;
  }
}

uring_t& uring_t::operator=(uring_t&& r) noexcept {
  if (this != &r) {
    if (registered_buffers) {
      for (size_t i = 0; i < registered_buffer_nr; ++i) {
        free(registered_buffers[i].iov_base);
      }
    }
    if (uring) io_uring_queue_exit(uring.get());
    this->uring = std::move(r.uring);
    this->task_nr = r.task_nr;
    this->registered_buffers = std::move(r.registered_buffers);
    this->registered_buffer_nr = r.registered_buffer_nr;
    this->registered_files = std::move(r.registered_files);
    r.uring = nullptr;
    r.task_nr = 0;
    r.registered_buffers = nullptr;
    r.registered_buffer_nr = 0;
    r.registered_files = nullptr;
  }
  return *this;
}

io_uring_sqe* uring_t::get_sqe() {
  auto sqe = io_uring_get_sqe(uring.get());
  if (!sqe) {
    int ret = io_uring_submit(uring.get());
    if (ret > 0) task_nr += ret;
    sqe = io_uring_get_sqe(uring.get());
    if (!sqe) {
      CORNET_FATAL("failed to get sqe even after submit");
    }
  }
  return sqe;
}

int uring_t::submit() {
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
    return submit_nr;
  }
  task_nr += submit_nr;
  return submit_nr;
}

uint32_t uring_t::process_cqes(int (*process_fn)(context_t &, cqe_t), context_t &ctx, cqe_t cqe) {
  uint32_t count{0}, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    process_fn(ctx, cqe);
    ++count;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
  return count;
}

uint32_t uring_t::wait_cqes(int (*process_fn)(context_t &, cqe_t), context_t &ctx, uint32_t wait_nr, sigset_t *mask) {
  cqe_t cqe;
  if (io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
    SPDLOG_ERROR("failed to wait io_uring cqes with error: {}", strerror(errno));
    return 0;
  }
  return process_cqes(process_fn, ctx, cqe);
}

uint32_t uring_t::peek_cqes(int(* process_fn)(context_t&, cqe_t), context_t& ctx, uint32_t peek_nr) {
  cqe_t cqe;
  uint32_t count = 0, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    process_fn(ctx, cqe);
    if (++count >= peek_nr) break;
  }
  if (count == 0) {
    SPDLOG_DEBUG("Uring peek batch cqe return empty");
    return 0;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
  return count;
}

CORNET_MAYBE_UNUSED bool uring_t::register_buffers(iovec* buffers, size_t buffer_nr) {
  for (size_t index = 0; index < buffer_nr; ++index) {
    iovec& buffer = buffers[index];
    if (posix_memalign(&buffer.iov_base, 4 * 1024, buffer.iov_len) != 0) {
      SPDLOG_ERROR("failed to allocate aligned buffer with error: {}", strerror(errno));
      for (size_t i = 0; i < index; ++i) {
        free(buffers[i].iov_base);
        buffers[i].iov_base = nullptr;
      }
      return false;
    }
  }
  int x = io_uring_register_buffers(uring.get(), buffers, buffer_nr);
  if (x < 0) {
    SPDLOG_ERROR("failed to register buffer on io_uring with error: {}", strerror(-x));
    for (size_t i = 0; i < buffer_nr; ++i) {
      free(buffers[i].iov_base);
      buffers[i].iov_base = nullptr;
    }
    return false;
  }
  this->registered_buffer_nr = buffer_nr;
  this->registered_buffers = std::make_unique<iovec[]>(buffer_nr);
  for (size_t index = 0; index < buffer_nr; ++index) {
    this->registered_buffers[index] = buffers[index];
  }
  return true;
}

CORNET_MAYBE_UNUSED bool uring_t::register_files(int* files, size_t file_nr) {
  if (io_uring_register_files(uring.get(), files, file_nr) < 0) {
    SPDLOG_ERROR("failed to register files on io_uring with error: {}", strerror(errno));
    return false;
  }
  this->registered_files = std::make_unique<int[]>(file_nr);
  for (size_t index = 0; index < file_nr; ++index) {
    this->registered_files[index] = files[index];
  }
  return true;
}

} // cornet
