#include "cornet/concurrency/deadline.h"

#include <cerrno>

#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

#include <gtest/gtest.h>

using namespace cornet;
using namespace std::chrono_literals;

class deadline : public ::testing::Test {
protected:
  void SetUp() override { ctx = new context_t(); }
  void TearDown() override { delete ctx; }

  context_t* ctx;
};

/**
 * The core contract: one fire reaches every derived kill scope. Modules hang
 * their io scopes off canceler() as children, so a single deadline fire
 * cascades into all of them — the tree replacement for the old two-pointer
 * firing.
 */
TEST_F(deadline, a_fire_cascades_into_every_derived_scope) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    uint64_t timeouts = 0;
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel, 0ms, &timeouts);
    canceler_t read_scope(ctx, dl.canceler());
    canceler_t drain_scope(ctx, dl.canceler());
    EXPECT_FALSE(drain_scope.is_cancelled());
    dl.cap(5ms);
    co_await sleep(ctx, 20ms);
    EXPECT_TRUE(dl.fired());
    EXPECT_TRUE(read_scope.is_cancelled());
    EXPECT_TRUE(drain_scope.is_cancelled());
    EXPECT_EQ(timeouts, 1u);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * Firing one scope manually must not touch the others or the parent: the
 * pattern request_close() relies on to spare the graceful drain window.
 */
TEST_F(deadline, a_scope_killed_manually_spares_its_siblings) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    canceler_t read_scope(ctx, dl.canceler());
    canceler_t drain_scope(ctx, dl.canceler());
    read_scope.cancel();
    EXPECT_TRUE(read_scope.is_cancelled());
    EXPECT_FALSE(drain_scope.is_cancelled());
    EXPECT_FALSE(dl.canceler().is_cancelled());
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * Ending the window (set_budget(0)) before the fire silences it: no node
 * left on the wheel, nothing ever rings.
 */
TEST_F(deadline, ending_the_window_before_the_fire_silences_it) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    canceler_t scope(ctx, dl.canceler());
    dl.cap(5ms);
    dl.set_budget(0ms);
    co_await sleep(ctx, 20ms);
    EXPECT_FALSE(dl.fired());
    EXPECT_FALSE(scope.is_cancelled());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * The two escape hatches from FIRED have opposite faces: set_budget(0) stops
 * any pending fire but KEEPS the latch (run-down code still learns that it
 * fired); reset() gives the whole object back — latch, canceler and all.
 */
TEST_F(deadline, set_budget_zero_keeps_latch_reset_clears_everything) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    dl.cap(5ms);
    co_await sleep(ctx, 20ms);
    EXPECT_TRUE(dl.fired());
    dl.set_budget(0ms);
    EXPECT_TRUE(dl.fired());            // latch survives ending the window
    dl.cap(200ms);
    auto stuck = co_await with_deadline(ctx, sleep(ctx, 1ms), dl);
    EXPECT_FALSE(stuck);                // latched canceler kills instantly
    EXPECT_TRUE(dl.reset());
    EXPECT_FALSE(dl.fired());
    auto r = co_await with_deadline(ctx, sleep(ctx, 1ms), dl);
    EXPECT_TRUE(r);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * map() must only rewrite errors the deadline actually caused: anything else
 * (peer reset, context shutdown sweep) keeps its real reason.
 */
TEST_F(deadline, map_translates_only_what_it_caused) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    cornet::error_t econn{ECONNRESET, error_domain::System};
    EXPECT_EQ(dl.map(econn).code, ECONNRESET);
    dl.cap(5ms);
    co_await sleep(ctx, 20ms);
    cornet::error_t m = dl.map(econn);
    EXPECT_EQ(m.code, ETIMEDOUT);
    EXPECT_EQ(m.domain, error_domain::System);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * The budget spans caps: a 10s cap clamped to a 30ms budget fires after the
 * budget, long before the cap would.
 */
TEST_F(deadline, a_budget_clamps_even_long_caps) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel, 30ms);
    canceler_t scope(ctx, dl.canceler());
    dl.cap(10s);
    co_await sleep(ctx, 60ms);
    EXPECT_TRUE(dl.fired());
    EXPECT_TRUE(scope.is_cancelled());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * A spent budget means the next op gets nothing: cap() takes the
 * immediate-fire path synchronously, with no timer involved at all.
 */
TEST_F(deadline, a_spent_budget_fires_the_next_cap_now) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    canceler_t scope(ctx, dl.canceler());
    dl.set_budget(1ms);          // the budget ends almost immediately
    co_await sleep(ctx, 15ms);   // the coarse clock is far past it by now
    // a fresh long cap must not get to run: fire() happens inside cap()
    dl.cap(10s);
    EXPECT_TRUE(dl.fired());
    EXPECT_TRUE(scope.is_cancelled());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(deadline, clearing_the_budget_unclamps_caps) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel, 20ms);
    dl.set_budget(0ms);
    dl.cap(120ms);
    co_await sleep(ctx, 50ms);
    // had the 20ms budget survived, this would have fired by now
    EXPECT_FALSE(dl.fired());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * cap() normalizes a nonsense duration rather than arming the wheel at 0, so
 * a zero timeout is "one tick", not "never".
 */
TEST_F(deadline, a_zero_duration_still_fires) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    dl.cap(0ms);
    co_await sleep(ctx, 20ms);
    EXPECT_TRUE(dl.fired());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * A destroyed-but-armed deadline takes its node off the wheel — the wheel's
 * own count drops, and nothing stale can ring later into freed memory.
 */
TEST_F(deadline, the_destructor_disarms) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    {
      deadline_t dl(ctx, wheel);
      dl.cap(5ms);
      EXPECT_EQ(wheel->armed_count(), 1u);
    }
    EXPECT_EQ(wheel->armed_count(), 0u);
    co_await sleep(ctx, 20ms);   // a stale payload would crash here
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(deadline, recapping_moves_rather_than_stacks) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    uint64_t timeouts = 0;
    deadline_t dl(ctx, wheel, 0ms, &timeouts);
    dl.cap(10s);
    dl.cap(5ms);   // re-cap: the far deadline must be replaced, not joined
    co_await sleep(ctx, 20ms);
    EXPECT_TRUE(dl.fired());
    EXPECT_EQ(timeouts, 1u);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * The self-contained ctor fetches its own coarse wheel and switch: nothing to
 * declare in the surrounding code for a one-off orchestration.
 */
TEST_F(deadline, the_self_contained_form_needs_nothing_declared) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    deadline_t dl(ctx, 1ms);
    canceler_t scope(ctx, dl.canceler());
    dl.cap(5ms);
    co_await sleep(ctx, 20ms);
    EXPECT_TRUE(dl.fired());
    EXPECT_TRUE(scope.is_cancelled());
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * Budgetless with_deadline scopes one op to a capped window: a leaf op that
 * outlives the cap comes back cancelled, and the fire's latch SURVIVES the
 * scope — a fire is terminal for the canceler anyway, so map() can still
 * answer ETIMEDOUT.
 */
TEST_F(deadline, with_deadline_kills_a_leaf_op_that_outlives_it) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    uint64_t timeouts = 0;
    deadline_t dl(ctx, ctx.wheel_for(1ms), 0ms, &timeouts);
    dl.cap(5ms);
    auto r = co_await with_deadline(ctx, sleep(ctx, 10s), dl);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error().code, ECANCELED);
    EXPECT_EQ(timeouts, 1u);
    EXPECT_TRUE(dl.fired());
    EXPECT_EQ(dl.map(r.error()).code, ETIMEDOUT);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * Same scope over a whole ccoro: the deadline's canceler lands in the
 * coroutine's promise, so the ops inside are what actually die.
 */
TEST_F(deadline, with_deadline_kills_a_whole_coroutine) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto work = [](context_t& ctx) -> ccoro_t<expected<int>> {
      auto s = co_await sleep(ctx, 10s);
      if (!s) co_return unexpected(s.error());
      co_return 1;
    };
    deadline_t dl(ctx, ctx.wheel_for(1ms));
    dl.cap(5ms);
    auto r = co_await with_deadline(ctx, work(ctx), dl);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error().code, ECANCELED);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * An op that beats the cap returns its own result, and the deadline comes out
 * untouched — no fire, no latch, ready for the next one.
 */
TEST_F(deadline, with_deadline_passes_through_a_timely_op) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto wheel = ctx.wheel_for(1ms);
    deadline_t dl(ctx, wheel);
    dl.cap(200ms);
    auto r = co_await with_deadline(ctx, sleep(ctx, 5ms), dl);
    EXPECT_TRUE(r);
    EXPECT_FALSE(dl.fired());
    EXPECT_EQ(wheel->armed_count(), 0u);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * A budget is the Go shape: the absolute end is armed once by the ctor, and
 * op after op draws against it with NO lifecycle call anywhere in between —
 * the second op gets only what the first left over.
 */
TEST_F(deadline, a_budget_is_armed_once_and_shared_across_ops) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    uint64_t timeouts = 0;
    deadline_t dl(ctx, ctx.wheel_for(1ms), 30ms, &timeouts);
    auto r1 = co_await with_deadline(ctx, sleep(ctx, 20ms), dl);
    EXPECT_TRUE(r1);                       // beat the budget; end still armed
    auto r2 = co_await with_deadline(ctx, sleep(ctx, 10s), dl);
    EXPECT_FALSE(r2);                      // died at what was left of it
    EXPECT_EQ(timeouts, 1u);
    EXPECT_TRUE(dl.fired());
    EXPECT_EQ(dl.map(r2.error()).code, ETIMEDOUT);
    // an expired context stays expired: the next op never gets to run
    auto r3 = co_await with_deadline(ctx, sleep(ctx, 1ms), dl);
    EXPECT_FALSE(r3);
    EXPECT_EQ(r3.error().code, ECANCELED);
    EXPECT_EQ(timeouts, 1u);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * A cap over a budget is restored to the shared end on the way out: cap(d)
 * cuts one op's window, and the next op sees the full remainder.
 */
TEST_F(deadline, a_cap_over_a_budget_restores_the_end_on_exit) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    deadline_t dl(ctx, ctx.wheel_for(1ms), 200ms);
    dl.cap(50ms);                          // cap only this op's window
    auto r1 = co_await with_deadline(ctx, sleep(ctx, 20ms), dl);
    EXPECT_TRUE(r1);                       // beat the cap; end restored
    // ran past t0+50: had the cap not been restored this would be dead
    auto r2 = co_await with_deadline(ctx, sleep(ctx, 100ms), dl);
    EXPECT_TRUE(r2);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

/**
 * reset() between ops brings a fired deadline back for another era: canceler
 * unlatched, latch and budget cleared, and a fresh budget serves new ops.
 * Sequential use is always quiescent at that point — the op that died holds
 * no more references once its result is in hand.
 */
TEST_F(deadline, reset_after_a_fire_starts_a_new_era) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    deadline_t dl(ctx, ctx.wheel_for(1ms), 20ms);
    auto r1 = co_await with_deadline(ctx, sleep(ctx, 10s), dl);
    EXPECT_FALSE(r1);
    EXPECT_TRUE(dl.fired());

    auto ok = dl.reset();
    EXPECT_TRUE(ok);
    EXPECT_FALSE(dl.fired());
    EXPECT_FALSE(dl.has_budget());

    dl.set_budget(100ms);
    auto r2 = co_await with_deadline(ctx, sleep(ctx, 5ms), dl);
    EXPECT_TRUE(r2);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}
