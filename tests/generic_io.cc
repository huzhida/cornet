#include "cornet/scheduling/context.h"
#include "cornet/concurrency/combinators.h"

#include <gtest/gtest.h>

using namespace cornet;

class generic_io : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = new context_t();
  }

  void TearDown() override {
    delete ctx;
  }

  context_t* ctx;
};

TEST_F(generic_io, nop) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto result = co_await ctx.io([](io_uring_sqe* sqe) {
      io_uring_prep_nop(sqe);
    });
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(generic_io, multiple_nops) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    for (int i = 0; i < 10; ++i) {
      auto result = co_await ctx.io([](io_uring_sqe* sqe) {
        io_uring_prep_nop(sqe);
      });
      EXPECT_TRUE(result.has_value());
    }
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(generic_io, pipe_read_write) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    int pipefd[2];
    EXPECT_EQ(pipe(pipefd), 0);

    const char* msg = "hello";
    auto write_result = co_await ctx.io([&](io_uring_sqe* sqe) {
      io_uring_prep_write(sqe, pipefd[1], msg, 5, 0);
    });
    EXPECT_TRUE(write_result.has_value());
    EXPECT_EQ(*write_result, 5);

    char buf[16] = {};
    auto read_result = co_await ctx.io([&](io_uring_sqe* sqe) {
      io_uring_prep_read(sqe, pipefd[0], buf, sizeof(buf), 0);
    });
    EXPECT_TRUE(read_result.has_value());
    EXPECT_EQ(*read_result, 5);
    EXPECT_STREQ(buf, "hello");

    ::close(pipefd[0]);
    ::close(pipefd[1]);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}
