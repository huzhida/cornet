#include "cornet/concurrency/semaphore.h"

#include <vector>

#include <gtest/gtest.h>

#include "cornet/scheduling/context.h"

using namespace cornet;

namespace {

class semaphore_test : public ::testing::Test {
 protected:
  void SetUp() override { ctx = new context_t(); }
  void TearDown() override { delete ctx; }
  context_t* ctx;
};

} // namespace

TEST_F(semaphore_test, static_counting) {
  semaphore_t sem(*ctx, 3);
  EXPECT_EQ(sem.value(), 3u);
  EXPECT_TRUE(sem.try_acquire(2));
  EXPECT_EQ(sem.value(), 1u);
  EXPECT_FALSE(sem.try_acquire(2));
  EXPECT_TRUE(sem.try_acquire(1));
  EXPECT_FALSE(sem.try_acquire(1));
  sem.release(5);
  EXPECT_EQ(sem.value(), 5u);
}

// The scheduler runs spawned coroutines in spawn order, so the queueing order
// inside the semaphore is fixed by construction — no sleeps, no racing.
// w1 blocks first, w3 last; release(3) must wake them strictly 1, 2, 3.
TEST_F(semaphore_test, fifo_wake_order) {
  std::vector<int> order;
  semaphore_t sem(*ctx, 0);

  auto waiter = [&](int id) -> coro_t<void> {
    auto ok = co_await sem.acquire();
    EXPECT_TRUE(ok);
    order.push_back(id);
  };
  auto releaser = [&]() -> coro_t<void> {
    sem.release(3);
    co_return;
  };

  ctx->spawn(waiter(1));
  ctx->spawn(waiter(2));
  ctx->spawn(waiter(3));
  ctx->spawn(releaser());
  ctx->run();
  EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

// A(3) heads the queue, B(1) waits behind it: releasing 1 into an empty pool
// must not wake B past A — strict FIFO, not best-fit.
TEST_F(semaphore_test, weighted_requests_block_fifo) {
  std::vector<char> order;
  semaphore_t sem(*ctx, 0);

  auto big = [&]() -> coro_t<void> {
    auto _ = co_await sem.acquire(3);
    (void)_;
    order.push_back('A');
  };
  auto small = [&]() -> coro_t<void> {
    auto _ = co_await sem.acquire(1);
    (void)_;
    order.push_back('B');
    EXPECT_EQ(sem.value(), 0u) << "B consumed the last permit after A drained the pool";
  };
  auto releaser = [&]() -> coro_t<void> {
    sem.release(1);
    EXPECT_TRUE(order.empty()) << "release(1) must not wake anyone (head needs 3)";
    sem.release(2);  // count 3: A fits, count back to 0
    sem.release(1);  // B finally fits
    co_return;
  };

  ctx->spawn(big());
  ctx->spawn(small());
  ctx->spawn(releaser());
  ctx->run();
  EXPECT_EQ(order, (std::vector<char>{'A', 'B'}));
}

TEST_F(semaphore_test, abort_fails_waiters_and_then_newcomers) {
  semaphore_t sem(*ctx, 0);
  int woke_with_cancel = 0;

  auto waiter = [&]() -> coro_t<void> {
    auto ok = co_await sem.acquire();
    EXPECT_FALSE(ok);
    EXPECT_EQ(ok.error().code, ECANCELED);
    ++woke_with_cancel;
  };
  auto checker = [&]() -> coro_t<void> {
    // the checker is spawned last, so all waiters are already queued here
    EXPECT_EQ(sem.waiting(), 3u);
    sem.abort();
    sem.abort();  // idempotent: the second call is a no-op
    EXPECT_TRUE(sem.aborted());
    co_return;
  };

  ctx->spawn(waiter());
  ctx->spawn(waiter());
  ctx->spawn(waiter());
  ctx->spawn(checker());
  ctx->run();
  EXPECT_EQ(woke_with_cancel, 3);

  // sticky: fresh acquires fail fast without suspending
  auto a = sem.acquire(1);
  EXPECT_TRUE(a.await_ready());
  auto ok = a.await_resume();
  EXPECT_FALSE(ok);
  EXPECT_EQ(ok.error().code, ECANCELED);
  EXPECT_FALSE(sem.try_acquire(1));
}

TEST_F(semaphore_test, guard_releases_at_scope_exit) {
  auto main = [&](context_t& ctx) -> coro_t<void> {
    semaphore_t sem(ctx, 1);
    {
      auto g = co_await sem.guard();
      EXPECT_TRUE(static_cast<bool>(*g));
      EXPECT_EQ(sem.value(), 0u);
      EXPECT_FALSE(sem.try_acquire());
    }
    EXPECT_EQ(sem.value(), 1u) << "guard dtor returned the permit";
    // early reset works too
    auto g2 = co_await sem.guard();
    g2->reset();
    EXPECT_TRUE(sem.try_acquire());
    co_return;
  };
  ctx->spawn(main(*ctx));
  ctx->run();
}

// ─── destroy-safety, driven without ctx->run() ───

TEST_F(semaphore_test, destroyed_waiter_unlinks) {
  semaphore_t sem(*ctx, 0);
  auto noop = []() -> coro_t<void> { co_return; };
  auto t = noop();

  {
    auto a = sem.acquire(1);
    EXPECT_FALSE(a.await_ready());
    EXPECT_TRUE(a.await_suspend(t.handle));
    EXPECT_EQ(sem.waiting(), 1u);
  }  // awaiter dies here: the node must unhook itself
  EXPECT_EQ(sem.waiting(), 0u);

}

TEST_F(semaphore_test, granted_but_dead_waiter_repays_permit) {
  semaphore_t sem(*ctx, 0);
  auto noop = []() -> coro_t<void> { co_return; };
  auto t = noop();

  {
    auto a = sem.acquire(1);
    EXPECT_FALSE(a.await_ready());
    EXPECT_TRUE(a.await_suspend(t.handle));
    sem.release(1);  // grants the permit and schedules the wake...
    EXPECT_EQ(sem.value(), 0u);
  }  // ...but the frame never resumes: destruction must repay, not drain
  EXPECT_EQ(sem.value(), 1u);

  // the scheduled wake is still in the ready queue; consummate it, then clean
  ctx->run();
}

TEST_F(semaphore_test, abort_wakes_through_scheduler) {
  semaphore_t sem(*ctx, 0);
  auto noop = []() -> coro_t<void> { co_return; };
  auto t = noop();

  {
    auto a = sem.acquire(1);
    EXPECT_FALSE(a.await_ready());
    EXPECT_TRUE(a.await_suspend(t.handle));
    EXPECT_EQ(sem.waiting(), 1u);
    sem.abort();
    EXPECT_EQ(sem.waiting(), 0u) << "abort unlinked the queued waiter";
    auto ok = a.await_resume();
    EXPECT_FALSE(ok) << "sticky error is deliverable to the woken waiter";
  }

  // abort enqueued the wake rather than resuming in-place; consume it
  ctx->run();
}
