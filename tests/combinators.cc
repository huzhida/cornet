#include "core/socket.h"
#include "core/combinators.h"

#include <gtest/gtest.h>

using namespace cornet;

class combinators : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = &context_t::current();
  }

  context_t* ctx;
};

TEST_F(combinators, sleep_basic) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto start = std::chrono::steady_clock::now();
    auto ret = co_await sleep(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
    EXPECT_TRUE(ret.has_value());
    EXPECT_GE(elapsed, 40);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(combinators, sleep_multiple) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto start = std::chrono::steady_clock::now();
    co_await sleep(std::chrono::milliseconds(20));
    co_await sleep(std::chrono::milliseconds(20));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
    EXPECT_GE(elapsed, 35);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(combinators, with_timeout_completes_before_timeout) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t server_sock;
    server_sock.address_reuse(true);
    EXPECT_TRUE(server_sock.listen("127.0.0.1", 23456).has_value());

    auto client_task = [](context_t& ctx) -> coro_t<void> {
      tcp::v4::socket_t sock;
      co_await sock.connect("127.0.0.1", 23456);
      co_await sock.send("hello", 5);
    };
    ctx.spawn(client_task(ctx));

    auto client = co_await server_sock.accept(0);
    EXPECT_TRUE(client.has_value());

    char buf[16] = {};
    auto result = co_await with_timeout(client->recv(buf, sizeof(buf)), std::chrono::seconds(5));
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result, 5);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(combinators, with_timeout_expires) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t server_sock;
    server_sock.address_reuse(true);
    EXPECT_TRUE(server_sock.listen("127.0.0.1", 23457).has_value());

    auto client_task = [](context_t& ctx) -> coro_t<void> {
      tcp::v4::socket_t sock;
      co_await sock.connect("127.0.0.1", 23457);
      co_await sleep(std::chrono::seconds(5));
    };
    ctx.spawn(client_task(ctx));

    auto client = co_await server_sock.accept(0);
    EXPECT_TRUE(client.has_value());

    char buf[16] = {};
    auto result = co_await with_timeout(client->recv(buf, sizeof(buf)), std::chrono::milliseconds(50));
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ETIMEDOUT);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(combinators, when_all_basic) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto task1 = []() -> coro_t<int> {
      co_await sleep(std::chrono::milliseconds(10));
      co_return 1;
    };
    auto task2 = []() -> coro_t<int> {
      co_await sleep(std::chrono::milliseconds(20));
      co_return 2;
    };

    auto result = co_await when_all(task1(), task2());
    auto& r1 = result.get<0>();
    auto& r2 = result.get<1>();
    EXPECT_TRUE(r1.has_value());
    EXPECT_TRUE(r2.has_value());
    EXPECT_EQ(r1.value(), 1);
    EXPECT_EQ(r2.value(), 2);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(combinators, when_any_basic) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    auto fast = []() -> coro_t<int> {
      co_await sleep(std::chrono::milliseconds(10));
      co_return 1;
    };
    auto slow = []() -> coro_t<int> {
      co_await sleep(std::chrono::milliseconds(200));
      co_return 2;
    };

    auto result = co_await when_any(fast(), slow());
    EXPECT_EQ(result.index, 0);
    auto& winner = result.get<0>();
    EXPECT_TRUE(winner.has_value());
    EXPECT_EQ(winner.value(), 1);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}
