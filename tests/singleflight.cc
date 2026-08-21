#include "cornet/concurrency/singleflight.h"

#include <string>

#include <gtest/gtest.h>

#include "cornet/concurrency/semaphore.h"
#include "cornet/scheduling/context.h"

using namespace cornet;

namespace {

class singleflight_test : public ::testing::Test {
 protected:
  void SetUp() override { ctx = new context_t(); }
  void TearDown() override { delete ctx; }
  context_t* ctx;
};

using sf_t = singleflight_t<std::string, int>;

} // namespace

/**
 * Three callers for the same key while the leader is parked mid-flight:
 * fn runs exactly once and every caller gets a copy of the same pointer.
 *
 * Determinism comes from spawn order plus a gate: the leader parks inside fn
 * until every follower has attached, then the gate opens.
 */
TEST_F(singleflight_test, merges_in_flight) {
  sf_t sf(*ctx);
  semaphore_t open(*ctx, 0);
  int fn_calls = 0;
  std::shared_ptr<int> seen[3];

  auto fn = [&]() -> coro_t<expected<std::shared_ptr<int>>> {
    ++fn_calls;
    auto ok = co_await open.acquire();
    EXPECT_TRUE(ok);
    co_return std::make_shared<int>(42);
  };
  auto caller = [&](int slot) -> coro_t<void> {
    auto r = co_await sf.run("key", fn);
    if (!r) { ADD_FAILURE() << r.error().message(); co_return; }
    seen[slot] = *r;
  };
  auto opener = [&]() -> coro_t<void> {
    // runs last by spawn order: both followers have already attached
    EXPECT_EQ(sf.in_flight(), 1u);
    open.release(1);
    co_return;
  };

  ctx->spawn(caller(0));  // leader: creates the flight, parks inside fn
  ctx->spawn(caller(1));  // attaches as follower
  ctx->spawn(caller(2));  // attaches as follower
  ctx->spawn(opener());
  ctx->run();

  EXPECT_EQ(fn_calls, 1);
  EXPECT_EQ(*seen[0], 42);
  EXPECT_EQ(seen[0], seen[1]);
  EXPECT_EQ(seen[1], seen[2]);
  EXPECT_EQ(sf.in_flight(), 0u) << "a landed flight leaves no bookkeeping";
}

TEST_F(singleflight_test, no_caching_after_completion) {
  sf_t sf(*ctx);
  int fn_calls = 0;
  auto fn = [&]() -> coro_t<expected<std::shared_ptr<int>>> {
    co_return std::make_shared<int>(++fn_calls);
  };
  auto once = [&]() -> coro_t<void> { co_await sf.run("k", fn); };

  ctx->spawn(once());
  ctx->run();
  ctx->spawn(once());
  ctx->run();
  EXPECT_EQ(fn_calls, 2) << "a landed flight is forgotten, not cached";
}

TEST_F(singleflight_test, error_fans_out_to_followers_and_does_not_stick) {
  sf_t sf(*ctx);
  semaphore_t open(*ctx, 0);
  int attempts = 0;
  int got_eperm = 0;
  int later_value = 0;

  auto fn = [&]() -> coro_t<expected<std::shared_ptr<int>>> {
    ++attempts;
    if (attempts == 1) {
      // only the first flight needs to hold: every follower must attach
      // before it fails — later retries execute straight through
      auto ok = co_await open.acquire();
      EXPECT_TRUE(ok);
      co_return unexpected(EPERM);
    }
    co_return std::make_shared<int>(7);
  };
  auto caller = [&]() -> coro_t<void> {
    auto r = co_await sf.run("k", fn);
    if (!r && r.error().code == EPERM) ++got_eperm;
    if (r) later_value = **r;
  };
  auto opener = [&]() -> coro_t<void> {
    open.release(1);
    co_return;
  };

  ctx->spawn(caller());  // leader
  ctx->spawn(caller());  // follower
  ctx->spawn(opener());
  ctx->run();
  EXPECT_EQ(attempts, 1);
  EXPECT_EQ(got_eperm, 2) << "both callers see the leader's failure";

  ctx->spawn(caller());
  ctx->run();
  EXPECT_EQ(attempts, 2) << "an error is not cached either";
  EXPECT_EQ(later_value, 7);
  EXPECT_EQ(sf.in_flight(), 0u);
}


