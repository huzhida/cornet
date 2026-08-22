#include "cornet/concurrency/combinators.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/scheduling/context.h"

#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/utsname.h>
#include <signal.h>
#include <unistd.h>
#include <cstdlib>
#include <vector>

#include "cornet/io_uring/awaiters.h"

#ifndef IORING_ASYNC_CANCEL_ANY
#define IORING_ASYNC_CANCEL_ANY (1U << 2)
#endif

namespace cornet {

namespace {

/**
 * @brief probe the RUNNING kernel for >= 5.19.
 *
 * Compile-time kernel headers cannot answer this: in a container they describe
 * the image that was built, not the host kernel the process actually runs on.
 * uname() asks the kernel itself, which is the same kernel inside and outside
 * a container.
 */
bool detect_kernel_ge_5_19() {
  struct utsname u {};
  if (::uname(&u) != 0) return false;
  char* end = nullptr;
  long major = std::strtol(u.release, &end, 10);
  if (end == u.release || *end != '.') return false;
  long minor = std::strtol(end + 1, nullptr, 10);
  return major > 5 || (major == 5 && minor >= 19);
}

} // namespace

context_t::context_t(config_t* config)
  : config_(config),
    tracker_(*this),
    uring_(tracker_, config),
    scheduler_(tracker_, config),
    kernel_ge_5_19_(detect_kernel_ge_5_19())
    {
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
  // Stop the wheels before teardown: their runners loop on ctx state, and the
  // cancel sweep has already reaped any tick SQE by now.
  stop_wheels();
  if (signal_fd_ >= 0) {
    ::close(signal_fd_);
    signal_fd_ = -1;
  }
  if (wakeup_fd_ >= 0) {
    ::close(wakeup_fd_);
    wakeup_fd_ = -1;
  }
}

std::shared_ptr<timer_wheel_t> context_t::wheel_for(std::chrono::milliseconds tick) {
  // The wheel falls back to its default tick for a nonsensical one; key on that
  // same value, or two callers asking for 0ms would get two wheels that tick
  // identically.
  if (tick.count() <= 0) tick = timer_wheel_t::kDefaultTick;
  // Reclaimed entries are reused rather than piling up: the vector is only as long
  // as the set of distinct ticks ever asked for.
  std::pair<std::chrono::milliseconds, std::weak_ptr<timer_wheel_t>>* free_slot = nullptr;
  for (auto& entry : wheels_) {
    if (auto wheel = entry.second.lock()) {
      if (entry.first == tick) return wheel;
    } else if (!free_slot) {
      free_slot = &entry;
    }
  }
  // No runner is spawned here: the wheel starts ticking on its first arm(), so a
  // wheel that gets created and never used is a few kilobytes and nothing else.
  auto wheel = timer_wheel_t::make(*this, tick);
  if (free_slot) {
    *free_slot = {tick, wheel};
  } else {
    wheels_.emplace_back(tick, wheel);
  }
  return wheel;
}

timer_wheel_t& context_t::timeout_wheel() {
  if (!deadline_wheel_) {
    // 5ms tick: coarse enough that an idle wheel is noise on the ring (~200
    // wakeups/s), fine enough that coroutine-level deadlines quantize to at
    // most one small scheduler quantum late.
    deadline_wheel_ = wheel_for(std::chrono::milliseconds(5));
  }
  return *deadline_wheel_;
}

void context_t::stop_wheels() {
  for (auto& [tick, weak] : wheels_) {
    if (auto wheel = weak.lock()) wheel->stop();
  }
}

void context_t::run() {
  spawn(watch_loop("wakeup", wakeup_fd_, sizeof(uint64_t), nullptr));
  // An early stop()/shutdown() arriving before run() sets state to Canceling;
  // only a plain start (still Terminated) earns the Running phase. Storing
  // unconditionally used to clobber a just-posted Canceling and lose the stop.
  auto expected = state_t::Terminated;
  if (state_.compare_exchange_strong(expected, state_t::Running, std::memory_order_acq_rel)) {
    SPDLOG_DEBUG("context switch to state:{}", to_string(state_t::Running));
  }
  // exit only when nothing is inflight, framework io included
  while (!idle()) {
    // one clock read per turn serves every request handled in that turn
    clock_.refresh();

    if (state_.load(std::memory_order_acquire) == state_t::Canceling
        && !cancel_inflight_) {
      // latched: one sweep at a time, re-armable. Stop the wheels here so a
      // re-arm on the way out cannot start another runner behind the sweep's
      // back. Tenants (an http server, a client) never stop a wheel they share.
      cancel_inflight_ = true;
      stop_wheels();
      spawn(cancel_sweep());
    }

    scheduler_.sched(*this);

    if (user_idle()) {
      switch_to(state_t::Canceling);
    }
  }
  // published only here: sweep and watchers have fully wound down
  switch_to(state_t::Terminated);
}

void context_t::shutdown(std::chrono::nanoseconds timeout) {
  set_keep_alive(false);
  auto expected = state_t::Running;
  if (!state_.compare_exchange_strong(expected, state_t::Draining, std::memory_order_acq_rel)) {
    // Not running (yet). An early shutdown must still stop a later run() from
    // entering a fresh Running phase, so post Canceling the way stop() does.
    auto before_run = state_t::Terminated;
    state_.compare_exchange_strong(before_run, state_t::Canceling, std::memory_order_acq_rel);
    return;
  }
  spawn_remote([this, timeout] () -> ccoro_t<void> {
    // as_system: drain timer must not pin user_idle() at false
    auto ok = co_await as_system(sleep(*this, timeout));
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
    if (expected == state_t::Draining || expected == state_t::Terminated) {
      // Terminated covers "stop() before run()": without it the request was
      // silently dropped and a later run() would start a full Running phase.
      state_.compare_exchange_strong(expected, state_t::Canceling, std::memory_order_acq_rel);
    }
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
    spawn(watch_loop("signal", signal_fd_, sizeof(struct signalfd_siginfo),
                        [this](const void* data) {
      int sig = static_cast<const struct signalfd_siginfo*>(data)->ssi_signo;
      auto it = signal_handlers_.find(sig);
      if (it != signal_handlers_.end()) {
        it->second(sig);
      }
    }));
  }
}

coro_t<void> context_t::watch_loop(const char* name, int fd, size_t len,
                                   std::function<void(const void*)> on_data) {
  std::vector<std::byte> buf(len);
  // no state check: only ECANCELED retires a watcher, so signals stay served
  // through Draining
  for (;;) {
    auto ret = co_await as_system(async_read(*this, fd, buf.data(), len));
    if (!ret) {
      if (ret.error().code == ECANCELED) break;
      if (ret.error().code == EINTR) continue;
      SPDLOG_ERROR("{}_watch_loop read failed: {}", name, ret.error().message());
      break;
    }
    if (on_data) on_data(buf.data());
  }
  co_return;
}

coro_t<void> context_t::cancel_sweep() {
  if (kernel_ge_5_19_) {
    // CANCEL_ANY reaps every inflight op in one SQE, but only those already
    // visible to the kernel — loop until it reports nothing left
    while (true) {
      auto ret = co_await as_system(cancel_awaiter{*this, nullptr, IORING_ASYNC_CANCEL_ANY});
      if (!ret) {
        if (ret.error().code != ENOENT) {
          SPDLOG_WARN("cancel sweep failed: {}", ret.error().message());
        }
        break;
      }
      if (*ret == 0) break;
    }
  } else {
    // older kernels have no CANCEL_ANY: walk the slot table and cancel per op
    std::vector<uint64_t> active;
    io_slots().for_each_active([&](uint64_t sd) { active.push_back(sd); });
    for (auto sd : active) {
      co_await as_system(cancel_awaiter{*this, reinterpret_cast<void*>(sd), 0});
    }
  }
  // cleared after the sweep's own CQE lands, so run() cannot stack sweeps
  cancel_inflight_ = false;
  co_return;
}

void context_t::wakeup() {
  // no token means the owner is not blocked: it will harvest on its next
  // sched() cycle, skip the syscall
  if (!parked_.exchange(false, std::memory_order_seq_cst)) return;
  uint64_t val = 1;
  if (::write(wakeup_fd_, &val, sizeof(val)) < 0) {
    SPDLOG_WARN("wakeup write failed: {}", strerror(errno));
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
