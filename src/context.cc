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
  io_uring_register_eventfd(uring.raw(), wakeup_fd);

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

  switch_to(state_t::Running);
  state_t current_state;
  while ((current_state = state.load(std::memory_order_acquire)) != state_t::Terminated) {

    if (current_state == state_t::Canceling) {
      spawn(cancel_pending_io());
    }

    scheduler->sched(*this);

    if (idle()) {
      switch_to(state_t::Terminated);
    }
  }
}

void context_t::shutdown(std::chrono::nanoseconds timeout) {
  shutdown_timeout = timeout;
  auto current = state.load(std::memory_order_acquire);
  if (current == state_t::Running) {
    switch_to(state_t::Draining);
    spawn(shutdown_sequence());
  }
  wakeup();
}

void context_t::stop() {
  state.store(state_t::Canceling, std::memory_order_release);
  wakeup();
}

coro_t<void> context_t::shutdown_sequence() {
  // wait for existing work to finish, or timeout
  co_await sleep(shutdown_timeout);

  // timeout expired, force cancel remaining io
  if (!idle()) {
    switch_to(state_t::Canceling);
  } else {
    switch_to(state_t::Terminated);
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

void context_t::wakeup() {
  uint64_t val = 1;
  auto _ = ::write(wakeup_fd, &val, sizeof(val));
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
