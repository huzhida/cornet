#include "cornet/concurrency/timer_wheel.h"

#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

#include <gtest/gtest.h>

using namespace cornet;
using namespace std::chrono_literals;

namespace {

/**
 * @brief a timer node plus the flag its callback sets.
 */
struct flag_t {
  bool         fired{false};
  timer_node_t node{};

  flag_t() {
    node.owner = this;
    node.on_expire = [](void* owner) { static_cast<flag_t*>(owner)->fired = true; };
  }
};

} // namespace

class timer_wheel : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = new context_t();
  }

  void TearDown() override {
    delete ctx;
  }

  context_t* ctx;
};

TEST_F(timer_wheel, fires_after_delay) {
  flag_t f;
  auto test = [](context_t& ctx, flag_t& f) -> coro_t<void> {
    auto wheel = ctx.wheel_for(5ms);
    wheel->arm(f.node, 10ms);
    co_await sleep(ctx, 30ms);
    EXPECT_TRUE(f.fired);
    EXPECT_EQ(wheel->armed_count(), 0u);
  };
  ctx->spawn(test(*ctx, f));
  ctx->run();
}

/**
 * The whole reason wheels hang off the context: same tick, same wheel, so a server
 * plus a handful of clients cost one tick SQE rather than one each.
 */
TEST_F(timer_wheel, shared_per_tick) {
  auto coarse = ctx->wheel_for(500ms);
  EXPECT_EQ(coarse, ctx->wheel_for(500ms));
  EXPECT_NE(coarse, ctx->wheel_for(5ms));
  // the coroutine-deadline wheel is just the 5ms one
  EXPECT_EQ(&ctx->timeout_wheel(), ctx->wheel_for(5ms).get());
  // a nonsensical tick lands on the default wheel rather than one of its own
  EXPECT_EQ(ctx->wheel_for(0ms), ctx->wheel_for(timer_wheel_t::kDefaultTick));
}

/**
 * A wheel nobody arms must not put anything in the ring — that laziness is what
 * makes it fine for the context to hand out wheels eagerly.
 */
TEST_F(timer_wheel, unarmed_wheel_never_ticks) {
  auto wheel = ctx->wheel_for(5ms);
  auto test = [](context_t& ctx) -> coro_t<void> {
    co_await sleep(ctx, 20ms);
  };
  ctx->spawn(test(*ctx));
  ctx->run();
  EXPECT_EQ(wheel->ticks(), 0u);
}

/**
 * The runner is not a permanent fixture: it leaves when the last timer is gone and
 * a later arm() brings a new one back.
 */
TEST_F(timer_wheel, runner_comes_and_goes) {
  flag_t first, second;
  auto test = [](context_t& ctx, flag_t& first, flag_t& second) -> coro_t<void> {
    auto wheel = ctx.wheel_for(5ms);
    wheel->arm(first.node, 10ms);
    co_await sleep(ctx, 30ms);
    EXPECT_TRUE(first.fired);
    auto ticked = wheel->ticks();
    // nothing armed for a while: the runner is gone, so the wheel stands still
    co_await sleep(ctx, 30ms);
    EXPECT_EQ(wheel->ticks(), ticked);
    // and comes back for the next timer
    wheel->arm(second.node, 10ms);
    co_await sleep(ctx, 30ms);
    EXPECT_TRUE(second.fired);
    EXPECT_GT(wheel->ticks(), ticked);
  };
  ctx->spawn(test(*ctx, first, second));
  ctx->run();
}

/**
 * A wheel whose tenants are all gone is reclaimed rather than kept around empty,
 * and the context hands out a fresh one next time.
 */
TEST_F(timer_wheel, reclaimed_when_last_tenant_leaves) {
  std::weak_ptr<timer_wheel_t> weak;
  flag_t f;
  {
    auto wheel = ctx->wheel_for(5ms);
    weak = wheel;
    auto test = [](context_t& ctx, timer_wheel_t& wheel, flag_t& f) -> coro_t<void> {
      wheel.arm(f.node, 10ms);
      co_await sleep(ctx, 30ms);
    };
    ctx->spawn(test(*ctx, *wheel, f));
    ctx->run();
    EXPECT_TRUE(f.fired);
    // the runner has left, but this share still keeps the wheel
    EXPECT_FALSE(weak.expired());
  }
  EXPECT_TRUE(weak.expired());
  // the registry slot was holding a weak reference, so the next ask builds anew
  EXPECT_NE(ctx->wheel_for(5ms), nullptr);
}

/**
 * Standalone wheels still work, and the runner co-owns the wheel: the owner's
 * reference can die (with the coroutine frame it lives in) while the runner is
 * asleep on its tick. That used to free the wheel under the runner, which then
 * read `running_` out of freed memory when the timeout completed. Here the runner
 * is also the one that reclaims it.
 */
TEST_F(timer_wheel, survives_owner_dropped_mid_tick) {
  flag_t f;
  std::weak_ptr<timer_wheel_t> weak;
  auto test = [](context_t& ctx, flag_t& f, std::weak_ptr<timer_wheel_t>& weak) -> coro_t<void> {
    auto wheel = timer_wheel_t::make(ctx, 5ms);
    weak = wheel;
    // armed far out so it never fires: all this timer does is start the runner and
    // keep it ticking while the owner walks away
    wheel->arm(f.node, 10s);
    co_await sleep(ctx, 20ms);
    wheel->cancel(f.node);
    wheel->stop();
    // and here the frame — with the owner's only reference in it — goes away
  };
  ctx->spawn(test(*ctx, f, weak));
  ctx->run();
  EXPECT_FALSE(f.fired);
  // the runner outlived its owner, then let go: no leak either
  EXPECT_TRUE(weak.expired());
}
