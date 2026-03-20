#include "core/uring.h"
#include <sys/eventfd.h>

namespace cornet {

uring_t::uring_t(uint32_t entries_nr, uint32_t flags)
  : uring(std::make_unique<io_uring>()) {
  if (io_uring_queue_init(entries_nr, uring.get(), flags) < 0) {
    SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(errno));
    throw std::runtime_error("io_uring_queue_init failed");
  }
}


uring_t::~uring_t() {
  io_uring_queue_exit(uring.get());
}

uring_t::uring_t(uring_t&& r) noexcept {
  if (this != &r) {
    this->uring = std::move(r.uring);
    this->registered_buffers = std::move(r.registered_buffers);
    this->registered_files = std::move(r.registered_files);
    r.uring = nullptr;
    r.registered_buffers = nullptr;
    r.registered_files = nullptr;
  }
}

uring_t& uring_t::operator=(uring_t&& r) noexcept {
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

int uring_t::submit() {
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
    return submit_nr;
  }
  task_nr += submit_nr;
  remain_sqe_nr += submit_nr;

  if (overflow()) {
    SPDLOG_WARN("io_uring sqe overflow, it will influence performance, try to increase io_uring entry count.");
    while(!sm.empty()) {
      auto sqe = io_uring_get_sqe(uring.get());
      if (!sqe) {
        SPDLOG_ERROR("get sqe failed even submitted, maybe io_uring capacity overflow...");
        exit(1);
      }
      *sqe = *sm.flush_overflow_sqe();
    }

    if (need_flush) {
      int overflow_submit_nr = io_uring_submit(uring.get());
      if (overflow_submit_nr < 0) {
        SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
        return overflow_submit_nr;
      }
      task_nr += overflow_submit_nr;
      submit_nr += overflow_submit_nr;
      need_flush = false;
    }
  }

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

uint32_t uring_t::peek_cqes(int(* process_fn)(context_t&, cqe_t), context_t& ctx, uint32_t peek_nr, sigset_t* mask) {
  std::vector<cqe_t> cqes(peek_nr);
  uint32_t ret = io_uring_peek_batch_cqe(uring.get(), cqes.data(), peek_nr);
  if (ret == 0) {
    SPDLOG_DEBUG("Uring peek batch cqe return empty");
    return 0;
  }
  for (unsigned i = 0; i < ret; i++) {
    process_fn(ctx, cqes[i]);
  }
  io_uring_cq_advance(uring.get(), ret);
  task_nr -= ret;
  return ret;
}

CORNET_MAYBE_UNUSED bool uring_t::register_buffers(iovec* buffers, size_t buffer_nr) {
  for (size_t index = 0; index < buffer_nr; ++index) {
    iovec& buffer = buffers[index];
    buffer.iov_len = posix_memalign(&buffer.iov_base, 4 * 1024, buffer.iov_len);
  }
  int x = io_uring_register_buffers(uring.get(), buffers, buffer_nr);
  if (x < 0) {
    SPDLOG_ERROR("failed to register buffer on io_uring with error: {}", strerror(-x));
    return false;
  }
  this->registered_buffers = std::make_unique<iovec[]>(buffer_nr);
  for (size_t index = 0; index < buffer_nr; ++index) {
    this->registered_buffers[index] = buffers[index];
  }
  return true;
}

CORNET_MAYBE_UNUSED bool uring_t::register_files(int* files, size_t file_nr) {
  if (io_uring_register_files(uring.get(), files, file_nr) < 0) {
    SPDLOG_ERROR("failed to register files on io_uring with error: {}", strerror(errno));
  }
  this->registered_files = std::make_unique<int[]>(file_nr);
  for (size_t index = 0; index < file_nr; ++index) {
    this->registered_files[index] = files[index];
  }
  return true;
}

} // cornet