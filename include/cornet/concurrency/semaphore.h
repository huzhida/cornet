#ifndef CORNET_SEMAPHORE_H
#define CORNET_SEMAPHORE_H

#include <coroutine>
#include <cstdint>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/coroutine/coro.h"

namespace cornet {

struct context_t;

/**
 * @brief a counting semaphore for coroutines on one context.
 *
 * A semaphore is how "at most N of these at once" is spelled when the work
 * must be awaited: bounded fan-out, a connection that accepts one request in
 * flight, a critical section that spans a co_await. A semaphore_t{1} is the
 * coroutine mutex — there is no separate type, because exclusivity is the
 * degenerate case of counting, not a distinct discipline.
 *
 * Suspension and resumption never touch io_uring: a waiter is woken by
 * release() pushing its handle onto the owner context's ready queue, which
 * is also what keeps release() safe to call from anywhere on that thread
 * (an expired timer callback included) — no wakeup ever resumes a waiter in
 * the caller's own frame.
 *
 * Lifetime: the semaphore must outlive every waiter queued on it, exactly
 * like every other intrusive structure in the codebase. Destroying a frame
 * that is suspended in acquire() is safe: its awaiter waits from inside the
 * frame and unlinks itself on the way out, repaying an already-granted
 * permit rather than draining the pool.
 *
 * Not supported (by design, not oversight):
 * - canceler_t preemption: cancellation in this framework completes kernel
 *   operations, and a semaphore wait is not one. Frame destruction is the
 *   cancellation channel, and abort() is the teardown channel;
 * - cross-context use: the queue is single-threaded, as is everything here.
 */
class semaphore_t {
 public:
  semaphore_t(context_t& ctx, uint32_t initial);
  ~semaphore_t();

  semaphore_t(const semaphore_t&) = delete;
  semaphore_t& operator=(const semaphore_t&) = delete;

  /**
   * @brief the awaiting half of the semaphore — a plain awaiter, no SQE.
   *
   * Borrows a slot in the wait queue while suspended; ownership never moves.
   * All bookkeeping is driven synchronously: suspend links, release unlinks,
   * and neither races anything on a single-threaded context.
   */
  struct acquire_awaiter {
    acquire_awaiter(semaphore_t& sem, uint32_t n);
    ~acquire_awaiter();

    acquire_awaiter(const acquire_awaiter&) = delete;
    acquire_awaiter& operator=(const acquire_awaiter&) = delete;

    CORNET_MAYBE_UNUSED bool await_ready();
    CORNET_MAYBE_UNUSED bool await_suspend(std::coroutine_handle<> h);
    CORNET_MAYBE_UNUSED CORNET_NODISCARD expected<void> await_resume();

   private:
    friend class semaphore_t;

    semaphore_t& sem_;
    uint32_t need_;
    std::coroutine_handle<> handle_{};
    // intrusive FIFO node: frame-owned, the semaphore borrows it while queued.
    // Plain doubly-linked — anything trickier proved subtly wrong under
    // head-pop wake loops (anchor-of-anchor chains die with the anchor).
    acquire_awaiter* next_{nullptr};
    acquire_awaiter* prev_{nullptr};
    // abort reason delivered to this waiter; system-domain while no abort fired
    error_t err_{};
    bool queued_{false};
    // permits were deducted for this waiter; repaid at destroy if never consumed
    bool granted_{false};
    // await_resume() ran — the permits are the caller's responsibility now
    bool consumed_{false};
  };

  /**
   * @brief acquire n permits, suspending in strict FIFO order if short.
   *
   * FIFO is strict: a large request at the head blocks small ones behind it
   * even when they would fit — fairness is the contract, head-of-line
   * blocking is its price.
   */
  CORNET_MAYBE_UNUSED acquire_awaiter acquire(uint32_t n = 1) {
    return acquire_awaiter(*this, n);
  }

  /**
   * @brief non-suspending acquire. Never touches the wait queue.
   */
  CORNET_NODISCARD bool try_acquire(uint32_t n = 1);

  /**
   * @brief return n permits and wake the longest-waiting suffix that fits, FIFO.
   */
  void release(uint32_t n = 1);

  /**
   * @brief refuse all future acquires and fail every queued waiter with `err`.
   *
   * Sticky teardown: a semaphore is aborted once, on the way out (the gate
   * behind it is closing). Idempotent; the state never reopens.
   */
  void abort(error_t err = error_t{ECANCELED, error_domain::System});

  CORNET_NODISCARD uint32_t value() const { return count_; }
  CORNET_NODISCARD bool aborted() const { return aborted_; }
  CORNET_NODISCARD CORNET_MAYBE_UNUSED uint32_t waiting() const;

  /**
   * @brief RAII permit holder: acquires, releases at scope exit.
   *
   * guard() is the natural spelling of a critical region that must co_await:
   *   auto g = co_await sem.guard();
   *   if (!g) co_return g.error();
   *   ... g stays alive across every co_await in here ...
   */
  class guard_t {
   public:
    ~guard_t() { reset(); }
    guard_t(guard_t&& o) noexcept : sem_(o.sem_), n_(o.n_) { o.sem_ = nullptr; }
    guard_t& operator=(guard_t&& o) noexcept {
      if (this != &o) {
        reset();
        sem_ = o.sem_;
        n_ = o.n_;
        o.sem_ = nullptr;
      }
      return *this;
    }
    guard_t(const guard_t&) = delete;
    guard_t& operator=(const guard_t&) = delete;

    CORNET_NODISCARD explicit operator bool() const { return sem_ != nullptr; }

    /**
     * @brief release early; also makes the guard inert.
     */
    void reset() {
      if (sem_) {
        sem_->release(n_);
        sem_ = nullptr;
      }
    }

   private:
    friend class semaphore_t;
    explicit guard_t(semaphore_t* sem, uint32_t n) : sem_(sem), n_(n) {}
    semaphore_t* sem_{nullptr};
    uint32_t n_{0};
  };

  CORNET_MAYBE_UNUSED CORNET_NODISCARD coro_t<expected<guard_t>> guard(uint32_t n = 1);

 private:
  // grant loop shared by release() and the repay path: FIFO while the head fits
  void wake_ready();
  void unlink(acquire_awaiter* w);

  context_t& ctx_;
  uint32_t   count_;
  acquire_awaiter* head_{nullptr};
  acquire_awaiter* tail_{nullptr};
  error_t    abort_err_{};
  bool       aborted_{false};
};

} // namespace cornet

#endif // CORNET_SEMAPHORE_H
