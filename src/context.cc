#include "cornet/scheduling/context.h"

#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <unistd.h>

#include "cornet/io_uring/awaiters.h"

namespace cornet {

context_t::context_t(config_t* config)
: uring(config ? config->at_path("cornet.context.uring.capacity").value_or(128) : 128) {
  cancellation_.init(*this);
  #ifdef CORNET_METRICS
  uring.metrics_ = &metrics_;
  #endif
  scheduler = scheduler_t::scheduler(config);
  executor_thread_nr = config ? config->at_path("cornet.context.executor.thread_nr").value_or(1) : 1;
  executor_max_task_nr = config ? config->at_path("cornet.context.executor.max_task_nr").value_or(16384) : 16384;

  wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_fd < 0) {
    SPDLOG_ERROR("failed to create eventfd: {}", strerror(errno));
    throw std::runtime_error("failed to create eventfd");
  }
}

context_t::~context_t() {
  if (executor) executor->terminate();
  if (signal_fd >= 0) {
    ::close(signal_fd);
    signal_fd = -1;
  }
  if (wakeup_fd >= 0) {
    ::close(wakeup_fd);
    wakeup_fd = -1;
  }
}

void context_t::run() {
  spawn(wakeup_watch_loop());
  switch_to(state_t::Running);
  state_t current_state;
  while (!idle()) {
    current_state = state.load(std::memory_order_acquire);

    if (current_state == state_t::Canceling) {
      spawn(cancellation_.cancel_pending_io());
      switch_to(state_t::Terminated);
    }

    scheduler->sched(*this);

    if (current_state <= state_t::Draining) {
      if (user_idle()
      || shutdown_deadline_.has_value() && std::chrono::steady_clock::now() > shutdown_deadline_.value()) {
        switch_to(state_t::Canceling);
      }
    }
  }
  switch_to(state_t::Terminated);
}

void context_t::shutdown(std::chrono::nanoseconds timeout) {
  set_keep_alive(false);
  auto expected = state_t::Running;
  if (!state.compare_exchange_strong(expected, state_t::Draining, std::memory_order_acq_rel)) {
    return;
  }
  shutdown_deadline_ = std::chrono::steady_clock::now() + timeout;
  wakeup();
}

void context_t::stop() {
  set_keep_alive(false);
  auto expected = state_t::Running;
  if (!state.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel)) {
    expected = state_t::Draining;
    state.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel);
  }
  wakeup();
}

void context_t::on_signal(std::initializer_list<int> signals, std::function<void(int)> handler) {
  for (int sig : signals) {
    signal_handlers[sig] = handler;
  }

  // rebuild signalfd with all registered signals
  sigset_t mask;
  sigemptyset(&mask);
  for (const auto& [sig, _] : signal_handlers) {
    sigaddset(&mask, sig);
  }
  sigprocmask(SIG_BLOCK, &mask, nullptr);

  if (signal_fd >= 0) {
    // update existing signalfd
    signalfd(signal_fd, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  } else {
    signal_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signal_fd < 0) {
      SPDLOG_ERROR("failed to create signalfd: {}", strerror(errno));
      return;
    }
    spawn(signal_watch_loop());
  }
}

coro_t<void> context_t::signal_watch_loop() {
  struct signalfd_siginfo siginfo{};
  while (!is_draining()) {
    uring.add_persistent();
    auto ret = co_await async_read(*this, signal_fd, &siginfo, sizeof(siginfo));
    uring.remove_persistent();
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      SPDLOG_ERROR("signal_watch_loop read failed: {}", ret.error().message());
      co_return;
    }
    int sig = siginfo.ssi_signo;
    auto it = signal_handlers.find(sig);
    if (it != signal_handlers.end()) {
      it->second(sig);
    }
  }
  co_return;
}

coro_t<void> context_t::wakeup_watch_loop() {
  uint64_t buf;
  while (true) {
    uring.add_persistent();
    auto ret = co_await read_awaiter(*this, wakeup_fd, &buf, sizeof(buf));
    uring.remove_persistent();
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      SPDLOG_ERROR("wakeup_watch_loop read failed: {}", ret.error().message());
      co_return;
    }
    if (is_draining()) break;
  }
  co_return;
}

void context_t::wakeup() {
  uint64_t val = 1;
  auto _ = ::write(wakeup_fd, &val, sizeof(val));
}

void context_t::drain_remote_queue() {
  std::function<void()> fn;
  while (remote_queue_.try_dequeue(fn)) {
    fn();
  }
}

void context_t::set_scheduler_type(scheduler_type_t type) {
  if (type == scheduler_type) return;
  scheduler_type = type;
  auto s = scheduler_t::scheduler(scheduler_type);
  scheduler->transfer_to(*s);
  scheduler = std::move(s);
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
