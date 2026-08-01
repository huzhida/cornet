#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <unistd.h>

#include "cornet/io_uring/awaiters.h"

namespace cornet {

context_t::context_t(config_t* config)
  : tracker_(),
    config_(config),
    uring_(tracker_, config),
    scheduler_(tracker_, config),
    executor_(tracker_, config) {
  tracker_.bind(this);
  cancellation_.init(*this);
  #ifdef CORNET_METRICS
  uring_.metrics_ = &metrics_;
  #endif
  wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_fd_ < 0) {
    SPDLOG_ERROR("failed to create eventfd: {}", strerror(errno));
    throw std::runtime_error("failed to create eventfd");
  }
}

context_t::~context_t() {
  executor_.terminate();
  if (signal_fd_ >= 0) {
    ::close(signal_fd_);
    signal_fd_ = -1;
  }
  if (wakeup_fd_ >= 0) {
    ::close(wakeup_fd_);
    wakeup_fd_ = -1;
  }
}

void context_t::run() {
  spawn(wakeup_watch_loop());
  switch_to(state_t::Running);
  state_t current_state;
  while (!idle()) {
    current_state = state_.load(std::memory_order_acquire);

    if (current_state == state_t::Canceling) {
      spawn(cancellation_.cancel_pending_io());
      switch_to(state_t::Terminated);
    }

    scheduler_.sched(*this);

    if (user_idle()) {
      switch_to(state_t::Canceling);
    }
  }
  switch_to(state_t::Terminated);
}

void context_t::shutdown(std::chrono::nanoseconds timeout) {
  set_keep_alive(false);
  auto expected = state_t::Running;
  if (!state_.compare_exchange_strong(expected, state_t::Draining, std::memory_order_acq_rel)) {
    return;
  }
  spawn_remote([this, timeout] () -> ccoro_t<void> {
    auto ok = co_await sleep(*this, timeout);
    if (!ok) {
      co_return;
    }
    auto expected = state_t::Draining;
    if (!state_.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel)) {
      co_return;
    }
  });
}

void context_t::stop() {
  set_keep_alive(false);
  auto expected = state_t::Running;
  if (!state_.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel)) {
    expected = state_t::Draining;
    state_.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel);
  }
  wakeup();
}

void context_t::on_signal(std::initializer_list<int> signals, std::function<void(int)> handler) {
  for (int sig : signals) {
    signal_handlers_[sig] = handler;
  }

  // rebuild signalfd with all registered signals
  sigset_t mask;
  sigemptyset(&mask);
  for (const auto& [sig, _] : signal_handlers_) {
    sigaddset(&mask, sig);
  }
  sigprocmask(SIG_BLOCK, &mask, nullptr);

  if (signal_fd_ >= 0) {
    // update existing signalfd
    signalfd(signal_fd_, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  } else {
    signal_fd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signal_fd_ < 0) {
      SPDLOG_ERROR("failed to create signalfd: {}", strerror(errno));
      return;
    }
    spawn(signal_watch_loop());
  }
}

coro_t<void> context_t::signal_watch_loop() {
  struct signalfd_siginfo siginfo{};
  while (is_running()) {
    persistent_guard guard(*this);
    auto ret = co_await async_read(*this, signal_fd_, &siginfo, sizeof(siginfo));
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      SPDLOG_ERROR("signal_watch_loop read failed: {}", ret.error().message());
      co_return;
    }
    int sig = siginfo.ssi_signo;
    auto it = signal_handlers_.find(sig);
    if (it != signal_handlers_.end()) {
      it->second(sig);
    }
  }
  co_return;
}

coro_t<void> context_t::wakeup_watch_loop() {
  uint64_t buf;
  while (is_running()) {
    persistent_guard guard(*this);
    auto ret = co_await read_awaiter(*this, wakeup_fd_, &buf, sizeof(buf));
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      SPDLOG_ERROR("wakeup_watch_loop read failed: {}", ret.error().message());
      co_return;
    }
  }
  co_return;
}

void context_t::wakeup() {
  uint64_t val = 1;
  auto _ = ::write(wakeup_fd_, &val, sizeof(val));
}

void context_t::drain_remote_queue() {
  std::function<void()> fn;
  while (remote_queue_.try_dequeue(fn)) {
    fn();
  }
}


context_t::cancel_awaiter::cancel_awaiter(context_t& ctx, void* user_data, int flags)
  : user_data_(user_data), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<cancel_awaiter*>(self);
    io_uring_prep_cancel(sqe, t->user_data_, t->flags_);
  };
}

} // cornet
