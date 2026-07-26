#include "cornet/scheduling/context.h"

#include <gtest/gtest.h>
#include <stdexcept>

using namespace cornet;

class async_test : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = new context_t();
  }

  void TearDown() override {
    delete ctx;
  }

  context_t* ctx;
};

TEST_F(async_test, basic_return_value) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto result = co_await ctx.async([] {
      return 42;
    });
    EXPECT_EQ(result, 42);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(async_test, void_callable) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    int x = 0;
    co_await ctx.async([&x] {
      x = 123;
    });
    EXPECT_EQ(x, 123);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(async_test, heavy_computation) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto result = co_await ctx.async([] {
      long long sum = 0;
      for (int i = 0; i < 1000000; ++i) sum += i;
      return sum;
    });
    EXPECT_EQ(result, 499999500000LL);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(async_test, exception_propagation) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    bool caught = false;
    try {
      co_await ctx.async([]() -> int {
        throw std::runtime_error("test error");
      });
    } catch (const std::runtime_error& e) {
      caught = true;
      EXPECT_STREQ(e.what(), "test error");
    }
    EXPECT_TRUE(caught);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(async_test, multiple_async_calls) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto a = co_await ctx.async([] { return 10; });
    auto b = co_await ctx.async([] { return 20; });
    auto c = co_await ctx.async([] { return 30; });
    EXPECT_EQ(a + b + c, 60);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(async_test, string_return) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto result = co_await ctx.async([] {
      return std::string("hello from thread pool");
    });
    EXPECT_EQ(result, "hello from thread pool");
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}
