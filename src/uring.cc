#include "cornet/base/metrics.h"
#include "cornet/io_uring/uring.h"
#include "cornet/scheduling/context.h"

namespace cornet {

uring_t::uring_t(uint32_t entries_nr, uint32_t flags)
  : uring(std::make_unique<io_uring>()) {
  SPDLOG_INFO("n = {}", entries_nr);
  if (io_uring_queue_init(entries_nr, uring.get(), flags) < 0) {
    SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(errno));
    throw std::runtime_error("failed to init io_uring queue");
  }
}

uring_t::~uring_t() {
  if (uring) io_uring_queue_exit(uring.get());
}

uring_t::uring_t(uring_t&& r) noexcept {
  if (this != &r) {
    this->uring = std::move(r.uring);
    this->task_nr = r.task_nr;
    r.uring = nullptr;
    r.task_nr = 0;
  }
}

uring_t& uring_t::operator=(uring_t&& r) noexcept {
  if (this != &r) {
    if (uring) io_uring_queue_exit(uring.get());
    this->uring = std::move(r.uring);
    this->task_nr = r.task_nr;
    r.uring = nullptr;
    r.task_nr = 0;
  }
  return *this;
}

io_uring_sqe* uring_t::get_sqe() {
  CORNET_METRICS_ADD(metrics_->get_sqe_calls);
  auto sqe = io_uring_get_sqe(uring.get());
  if (!sqe) {
    CORNET_METRICS_ADD(metrics_->get_sqe_submit_forced);
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
  CORNET_METRICS_ADD_N(metrics_->get_sqe_calls, n);
  // try to acquire all n SQEs without intermediate submit
  for (size_t i = 0; i < n; ++i) {
    out[i] = io_uring_get_sqe(uring.get());
    if (!out[i]) {
      // not enough space, submit pending and retry all from scratch
      CORNET_METRICS_ADD(metrics_->get_sqe_submit_forced);
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
  CORNET_METRICS_ADD(metrics_->submit_calls);
  CORNET_METRICS_SCOPE_TIMER(metrics_->submit_latency);
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
    CORNET_METRICS_ADD(metrics_->submit_failures);
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(-submit_nr));
    return submit_nr;
  }
  task_nr += submit_nr;
  CORNET_METRICS_ADD_N(metrics_->submit_sqes, submit_nr);
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
  CORNET_METRICS_ADD(metrics_->wait_calls);
  CORNET_METRICS_SCOPE_TIMER(metrics_->wait_latency);
  cqe_t cqe;
  if (io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
    CORNET_METRICS_ADD(metrics_->wait_timeouts);
    return 0;
  }
  uint32_t n = process_cqes(ctx, cqe);
  CORNET_METRICS_ADD_N(metrics_->wait_cqes_processed, n);
  return n;
}

uint32_t uring_t::peek_cqes(context_t& ctx, uint32_t peek_nr) {
  CORNET_METRICS_ADD(metrics_->peek_calls);
  cqe_t cqe;
  uint32_t count = 0, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    utask_t::process_utask(ctx, cqe);
    if (++count >= peek_nr) break;
  }
  if (count == 0) {
    CORNET_METRICS_ADD(metrics_->peek_empty);
    return 0;
  }
  io_uring_cq_advance(uring.get(), count);
  task_nr -= count;
  CORNET_METRICS_ADD_N(metrics_->peek_cqes_processed, count);
  return count;
}

} // cornet
