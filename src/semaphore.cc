#include "cornet/concurrency/semaphore.h"

#include "cornet/scheduling/context.h"

namespace cornet {

semaphore_t::semaphore_t(context_t& ctx, uint32_t initial) : ctx_(ctx), count_(initial) {}

semaphore_t::~semaphore_t() {
  // Borrowed nodes live in frames the same way timer nodes do: still-queued
  // waiters at this point is a framework-level misuse, not a runtime
  // condition to paper over.
  CORNET_ASSERT(head_ == nullptr, "semaphore_t destroyed with waiters still queued");
}

// ───────────────────────────── acquire awaiter ─────────────────────────────

semaphore_t::acquire_awaiter::acquire_awaiter(semaphore_t& sem, uint32_t n)
  : sem_(sem), need_(n) {}

semaphore_t::acquire_awaiter::~acquire_awaiter() {
  if (queued_) sem_.unlink(this);
  if (granted_ && !consumed_) {
    // The grant happened, the frame never resumed (it was destroyed in the
    // window between wake and resume): the permits go back to the pool. Every
    // other accounting path would drain one permit per dead waiter forever.
    sem_.count_ += need_;
    if (!sem_.aborted_) sem_.wake_ready();
  }
}

bool semaphore_t::acquire_awaiter::await_ready() {
  if (sem_.aborted_) {
    err_ = sem_.abort_err_;
    // nothing was granted, so nothing will be repaid
    consumed_ = true;
    return true;
  }
  if (sem_.count_ >= need_) {
    // Fast path completes without a suspension point, so there is no window
    // in which this grant could be lost: mark it consumed immediately.
    granted_ = true;
    consumed_ = true;
    sem_.count_ -= need_;
    return true;
  }
  return false;
}

bool semaphore_t::acquire_awaiter::await_suspend(std::coroutine_handle<> h) {
  handle_ = h;
  queued_ = true;
  // FIFO tail-append; unlink is driven by the wake loop, abort(), or the dtor
  prev_ = sem_.tail_;
  next_ = nullptr;
  if (sem_.tail_) {
    sem_.tail_->next_ = this;
  } else {
    CORNET_ASSERT(sem_.head_ == nullptr, "non-empty queue with no tail");
    sem_.head_ = this;
  }
  sem_.tail_ = this;
  return true;
}

expected<void> semaphore_t::acquire_awaiter::await_resume() {
  // Whichever way the wait ended, the bookkeeping is settled from here on:
  // the permits (if any) belong to the caller now.
  consumed_ = true;
  if (!granted_) {
    CORNET_ASSERT(err_.code != 0, "ungranted waiter resumed without an error");
    return unexpected(err_.code, err_.domain);
  }
  return {};
}

// ───────────────────────────── semaphore core ─────────────────────────────

void semaphore_t::unlink(acquire_awaiter* w) {
  CORNET_ASSERT(w->queued_, "unlinking a node that is not queued");
  if (w->prev_) {
    w->prev_->next_ = w->next_;
  } else {
    CORNET_ASSERT(head_ == w, "headless queue entry");
    head_ = w->next_;
  }
  if (w->next_) {
    w->next_->prev_ = w->prev_;
  } else {
    CORNET_ASSERT(tail_ == w, "tailless queue entry");
    tail_ = w->prev_;
  }
  w->next_ = nullptr;
  w->prev_ = nullptr;
  w->queued_ = false;
}

void semaphore_t::wake_ready() {
  while (head_ && head_->need_ <= count_) {
    auto* w = head_;
    unlink(w);
    w->granted_ = true;
    count_ -= w->need_;
    // wake through the ready queue, never in the caller's frame: a resumed
    // waiter that immediately releases (the guard idiom) must not build a
    // wake→resume→wake chain on the stack
    ctx_.scheduler().schedule(w->handle_);
  }
}

bool semaphore_t::try_acquire(uint32_t n) {
  if (aborted_ || count_ < n) return false;
  count_ -= n;
  return true;
}

void semaphore_t::release(uint32_t n) {
  if (n == 0) return;
  count_ += n;
  if (!aborted_) wake_ready();
}

void semaphore_t::abort(error_t err) {
  if (aborted_) return;
  aborted_ = true;
  abort_err_ = err;
  while (head_) {
    auto* w = head_;
    unlink(w);
    w->err_ = err;
    ctx_.scheduler().schedule(w->handle_);
  }
}

uint32_t semaphore_t::waiting() const {
  uint32_t n = 0;
  for (auto* w = head_; w; w = w->next_) ++n;
  return n;
}

coro_t<expected<semaphore_t::guard_t>> semaphore_t::guard(uint32_t n) {
  auto ok = co_await acquire(n);
  if (!ok) co_return unexpected(ok.error());
  co_return guard_t{this, n};
}

} // namespace cornet
