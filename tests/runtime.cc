#include "cornet/scheduling/runtime.h"
#include "cornet/io_uring/awaiters.h"

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>

using namespace cornet;

class runtime_test : public ::testing::Test {};

TEST_F(runtime_test, start_and_shutdown) {
  runtime_t rt(2);
  std::atomic<int> init_count{0};

  rt.start([&](size_t idx, context_t&) {
    init_count.fetch_add(1);
  });

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();

  EXPECT_EQ(init_count.load(), 2);
}

TEST_F(runtime_test, spawn_remote_basic) {
  // Test that spawn_remote executes a coroutine on a different thread
  runtime_t rt(2);
  std::atomic<int> executed{0};
  std::atomic<std::thread::id> exec_thread{};

  rt.start([&](size_t idx, context_t&) {
    if (idx == 0) {
      // thread 0 submits a coroutine to thread 1 via spawn_remote
      context_t& ctx1 = rt.context_at(1);

      // spawn_remote takes a callable returning coro_t<void>
      // We use a simple fire-and-forget coroutine that records its thread
      auto task = [&]() -> coro_t<void> {
        exec_thread.store(std::this_thread::get_id());
        executed.fetch_add(1);
        co_return;
      };
      ctx1.spawn_remote(task);
    }
  });

  // Give the spawned coroutine time to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();

  EXPECT_EQ(executed.load(), 1);
  // The coroutine should have run on thread 1, not thread 0
  EXPECT_NE(exec_thread.load(), std::thread::id{});
  // Thread 0's id should NOT match (confirming cross-thread execution)
  EXPECT_NE(exec_thread.load(), std::this_thread::get_id());
}

TEST_F(runtime_test, multi_producer_spawn_remote) {
  // Test that multiple threads can submit coroutines to the same context
  runtime_t rt(4);
  std::atomic<int> counter{0};

  rt.start([&](size_t idx, context_t&) {
    if (idx != 0) {
      context_t& ctx0 = rt.context_at(0);

      // Each thread submits a coroutine to thread 0
      auto task = [&]() -> coro_t<void> {
        counter.fetch_add(1);
        co_return;
      };
      ctx0.spawn_remote(task);
    }
  });

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();

  // 3 threads (1, 2, 3) each submitted a coroutine to thread 0
  EXPECT_EQ(counter.load(), 3);

}

TEST_F(runtime_test, spawn_remote_io_operation) {
  // Test that spawn_remote coroutines can perform IO on the target thread
  runtime_t rt(2);
  std::atomic<bool> io_succeeded{false};

  rt.start([&](size_t idx, context_t&) {
    if (idx == 0) {
      context_t& ctx1 = rt.context_at(1);

      // Submit a coroutine that performs a simple nop IO operation
      // to verify IO works on the target thread
      auto task = [&]() -> coro_t<void> {
        auto ret = co_await nop_awaiter(ctx1);
        if (ret) {
          io_succeeded.store(true);
        }
        co_return;
      };
      ctx1.spawn_remote(task);
    }
  });

  // Give time for the IO operation to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
  EXPECT_TRUE(io_succeeded.load());
}
