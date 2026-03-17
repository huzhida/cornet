#include "core/uring.h"
#include <sys/eventfd.h>

namespace cornet {

uring_t::uring_t(uint32_t entries_nr, uint32_t flags)
  : uring(std::make_unique<io_uring>()), remain_sqe_nr(entries_nr) {
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

bool uring_t::submit() {
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(errno));
    return false;
  }
  remain_sqe_nr += submit_nr;
  task_nr += submit_nr;
  return true;
}

uint32_t uring_t::wait_cqes(int (*process_fn)(cqe_t), uint32_t wait_nr, int timeout_s,
                                        int timeout_ns, sigset_t* mask) {
  uint32_t count{0};

  if (timeout_s == 0 && timeout_ns == 0) {
    std::vector<cqe_t> cqes(wait_nr);
    uint32_t ret = io_uring_peek_batch_cqe(uring.get(), cqes.data(), wait_nr);
    if (ret == 0) {
      SPDLOG_DEBUG("Uring peek batch cqe return empty");
      return 0;
    }
    for (unsigned i = 0; i < ret; i++) {
      process_fn(cqes[i]);
    }
    io_uring_cq_advance(uring.get(), ret);
    task_nr -= ret;
    return ret;
  }

  cqe_t cqe;
  uint32_t head;
  if (timeout_s > 0 || timeout_ns > 0) {
    __kernel_timespec ts{timeout_s, timeout_ns};
    int ret = io_uring_wait_cqes(uring.get(), &cqe, wait_nr, &ts, mask);
    if (ret == -ETIME) {
      SPDLOG_DEBUG("Uring wait_and_process_cqes timeout.");
      return 0;
    }
  } else {
    if (io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
      SPDLOG_ERROR("failed to wait io_uring cqes with error: {}", strerror(errno));
      return 0;
    }
  }

  io_uring_for_each_cqe(uring.get(), head, cqe) {
    process_fn(cqe);
    ++count;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
  return count;
}

uint32_t uring_t::peek_cqes(int(* process_fn)(cqe_t), uint32_t peek_nr, sigset_t* mask) {
  return wait_cqes(process_fn, peek_nr, 0, 0, mask);
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