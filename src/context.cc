#include "core/context.h"
#include "core/combinators.h"
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <unistd.h>

namespace cornet {

std::mutex context_t::contexts_mutex;
std::unordered_map<std::thread::id, context_t*> context_t::contexts;

context_t::context_t()
: uring(config_t::get()["cornet"]["context"]["uring"]["capacity"].value_or(32)) {
  uring.metrics_ = &metrics_;
  if (auto scheduler_name = config_t::get()["cornet"]["context"]["scheduler"]["name"]) {
    scheduler_type = scheduler_t::to_scheduler_type(scheduler_name.as_string()->value_or(""));
  }
  scheduler = scheduler_t::scheduler(scheduler_type);

  wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_fd < 0) {
    CORNET_FATAL("failed to create eventfd: {}", strerror(errno));
  }

  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts[std::this_thread::get_id()] = this;
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
  std::lock_guard<std::mutex> guard(contexts_mutex);
  contexts.erase(std::this_thread::get_id());
}

void context_t::run() {
  if (owner != std::this_thread::get_id()) {
    SPDLOG_ERROR("never run context in other thread");
    return;
  }

  spawn(wakeup_watch_loop());
  switch_to(state_t::Running);
  state_t current_state;
  while (!idle()) {
    current_state = state.load(std::memory_order_acquire);

    if (current_state == state_t::Canceling) {
      spawn(cancel_pending_io());
      switch_to(state_t::Terminating);
    }

    scheduler->sched(*this);

    if (current_state <= state_t::Draining && user_idle()) {
      switch_to(state_t::Canceling);
    }
  }
  switch_to(state_t::Terminated);
}

void context_t::shutdown(std::chrono::nanoseconds timeout) {
  auto current = state.load(std::memory_order_acquire);
  if (current == state_t::Running) {
    switch_to(state_t::Draining);
    if (std::this_thread::get_id() == owner) {
      spawn(shutdown_sequence(timeout));
    } else {
      remote_queue_.enqueue([this, timeout]() {
        spawn(shutdown_sequence(timeout));
      });
    }
  }
  wakeup();
}

void context_t::stop() {
  auto expected = state_t::Running;
  if (!state.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel)) {
    expected = state_t::Draining;
    state.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel);
  }
  wakeup();
}

coro_t<void> context_t::shutdown_sequence(std::chrono::nanoseconds timeout) {
  co_await sleep(timeout);

  // Only transition if still in Draining (may have been superseded by stop() or user_idle)
  auto current = state.load(std::memory_order_acquire);
  if (current == state_t::Draining && !user_idle()) {
    switch_to(state_t::Canceling);
  }
  co_return;
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
    auto ret = co_await io([this, &siginfo](io_uring_sqe* sqe) {
      io_uring_prep_read(sqe, signal_fd, &siginfo, sizeof(siginfo), 0);
    });
    uring.remove_persistent();
    if (!ret || *ret <= 0) {
      break;
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
    auto ret = co_await read_awaiter(wakeup_fd, &buf, sizeof(buf));
    uring.remove_persistent();
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      SPDLOG_ERROR("wakeup_watch_loop read failed: {}", ret.error().message());
      break;
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

std::thread::id context_t::owner_thread() const {
  return owner;
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
