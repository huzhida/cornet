#include "core/tools.h"
#include "core/context.h"
#include <gtest/gtest.h>

using namespace cornet;


class tools : public ::testing::Test {
 protected:
  void SetUp() override {
    ctx = &context_t::context();
  }

  void TearDown() override {
  }

  context_t* ctx{};
};

coro_t<int> async_task(context_t& ctx) {
  auto f = []() -> int {
    int sum = 0;
    for (int i = 0; i < 102410241; ++i)  {
      sum += 1;
    }
    return sum;
  };
  co_return co_await async(ctx, f);
}

TEST_F(tools, async_task) {
  auto a = async_task(*ctx);
  ctx->sched(a);
  ctx->run();

  EXPECT_EQ(a.value() , 102410241);
}