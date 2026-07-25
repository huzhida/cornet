#include "io_uring/uring.h"
#include "scheduling/context.h"
#include "base/metrics.h"

namespace cornet {

uring_t::uring_t(uint32_t entries_nr, uint32_t flags)
  : uring(std::make_unique<io_uring>()) {
  if (io_uring_queue_init(entries_nr, uring.get(), flags) < 0) {
    SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(errno));
    throw std::runtime_error("failed to init io_uring queue");
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
#ifdef CORNET_METRICS
  if (metrics_) metrics_->get_sqe_calls++;
#endif
  auto sqe = io_uring_get_sqe(uring.get());
  if (!sqe) {
#ifdef CORNET_METRICS
    if (metrics_) metrics_->get_sqe_submit_forced++;
#endif
    int ret = io_uring_submit(uring.get());
    if (ret > 0) task_nr += ret;
    sqe = io_uring_get_sqe(uring.get());
    if (!sqe) {
      SPDLOG_ERROR("failed to get sqe even after submit");
      throw std::runtime_error("failed to get sqe even after submit");
    }
  }
  return sqe;
}

void uring_t::get_sqes(io_uring_sqe** out, size_t n) {
#ifdef CORNET_METRICS
  if (metrics_) metrics_->get_sqe_calls += n;
#endif
  // try to acquire all n SQEs without intermediate submit
  for (size_t i = 0; i < n; ++i) {
    out[i] = io_uring_get_sqe(uring.get());
    if (!out[i]) {
      // not enough space, submit pending and retry all from scratch
#ifdef CORNET_METRICS
      if (metrics_) metrics_->get_sqe_submit_forced++;
#endif
      int ret = io_uring_submit(uring.get());
      if (ret > 0) task_nr += ret;
      for (size_t j = 0; j < n; ++j) {
        out[j] = io_uring_get_sqe(uring.get());
        if (!out[j]) {
          SPDLOG_ERROR("failed to get {} sqes even after submit", n);
          throw std::runtime_error("failed to get sqes even after submit");
        }
      }
      return;
    }
  }
}

int uring_t::submit() {
#ifdef CORNET_METRICS
  if (metrics_) metrics_->submit_calls++;
  scoped_timer_t timer(metrics_ ? &metrics_->submit_latency : nullptr);
#endif
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
#ifdef CORNET_METRICS
    if (metrics_) metrics_->submit_failures++;
#endif
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(-submit_nr));
    return submit_nr;
  }
  task_nr += submit_nr;
#ifdef CORNET_METRICS
  if (metrics_) metrics_->submit_sqes += submit_nr;
#endif
  return submit_nr;
}

uint32_t uring_t::process_cqes(context_t &ctx, cqe_t cqe) {
  uint32_t count{0}, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    utask_t::process_utask(ctx, cqe);
    ++count;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
  return count;
}

uint32_t uring_t::wait_cqes(context_t &ctx, uint32_t wait_nr, sigset_t *mask) {
#ifdef CORNET_METRICS
  if (metrics_) metrics_->wait_calls++;
  scoped_timer_t timer(metrics_ ? &metrics_->wait_latency : nullptr);
#endif
  cqe_t cqe;
  if (io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
#ifdef CORNET_METRICS
    if (metrics_) metrics_->wait_timeouts++;
#endif
    return 0;
  }
  uint32_t n = process_cqes(ctx, cqe);
#ifdef CORNET_METRICS
  if (metrics_) metrics_->wait_cqes_processed += n;
#endif
  return n;
}

uint32_t uring_t::peek_cqes(context_t& ctx, uint32_t peek_nr) {
#ifdef CORNET_METRICS
  if (metrics_) metrics_->peek_calls++;
#endif
  cqe_t cqe;
  uint32_t count = 0, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    utask_t::process_utask(ctx, cqe);
    if (++count >= peek_nr) break;
  }
  if (count == 0) {
#ifdef CORNET_METRICS
    if (metrics_) metrics_->peek_empty++;
#endif
    return 0;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
#ifdef CORNET_METRICS
  if (metrics_) metrics_->peek_cqes_processed += count;
#endif
  return count;
}

} // cornet
