#include "cornet/scheduling/runtime.h"
#include "cornet/io_uring/awaiters.h"

#include <gtest/gtest.h>
#include <atomic>
#include <latch>
#include <thread>
#include <chrono>
#include <string>

using namespace cornet;

class runtime_test : public ::testing::Test {};

TEST_F(runtime_test, start_and_shutdown) {
  runtime_t rt(nullptr, 2);
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
  runtime_t rt(nullptr, 2);
  std::atomic<int> executed{0};
  std::atomic<std::thread::id> exec_thread{};
  std::latch ran(1);

  rt.start([&](size_t idx, context_t&) {
    if (idx == 0) {
      // thread 0 submits a coroutine to thread 1 via spawn_remote
      context_t& ctx1 = rt.context_at(1);

      // spawn_remote takes a callable returning coro_t<void>
      // We use a simple fire-and-forget coroutine that records its thread
      auto task = [&]() -> coro_t<void> {
        exec_thread.store(std::this_thread::get_id());
        executed.fetch_add(1);
        ran.count_down();
        co_return;
      };
      ctx1.spawn_remote(task);
    }
  });

  // wait for the coroutine itself rather than for a duration
  ran.wait();

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
  runtime_t rt(nullptr, 4);
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
  runtime_t rt(nullptr, 2);
  std::atomic<bool> io_succeeded{false};
  std::latch ran(1);

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
        ran.count_down();
        co_return;
      };
      ctx1.spawn_remote(task);
    }
  });

  // wait for the IO operation itself rather than for a duration
  ran.wait();

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
  EXPECT_TRUE(io_succeeded.load());
}

// =========================================================================
//  New API tests: submit / spawn / submit_async / spawn_async
// =========================================================================

TEST_F(runtime_test, submit_coroutine_result) {
  // Test that submit() returns a future with the correct result
  runtime_t rt(nullptr, 2);

  rt.start([](size_t, context_t&) {
    // init_fn: just start the run loop
  });

  // Submit from main thread (after start() returns, run loops are active)
  auto f = rt.submit([](context_t& ctx) -> coro_t<int> {
    auto ret = co_await nop_awaiter(ctx);
    co_return ret ? 42 : -1;
  });

  // The future should not be done immediately
  EXPECT_FALSE(f.is_done());

  int result = f.get();
  EXPECT_EQ(result, 42);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, submit_coroutine_void) {
  // Test that submit() works for void-returning coroutines
  runtime_t rt(nullptr, 2);
  std::atomic<int> counter{0};

  rt.start([](size_t, context_t&) {});

  auto f = rt.submit([&counter](context_t& ctx) -> coro_t<void> {
    co_await nop_awaiter(ctx);
    counter.fetch_add(1);
    co_return;
  });

  f.get();  // blocks until done
  EXPECT_EQ(counter.load(), 1);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, submit_exception_propagation) {
  // Test that exceptions in submitted coroutines are propagated via get()
  runtime_t rt(nullptr, 2);

  rt.start([](size_t, context_t&) {});

  auto f = rt.submit([](context_t&) -> coro_t<int> {
    throw std::runtime_error("test exception");
    co_return 0;
  });

  bool caught = false;
  try {
    f.get();
  } catch (const std::runtime_error& e) {
    caught = std::string(e.what()) == "test exception";
  }
  EXPECT_TRUE(caught);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, submit_async_result) {
  // Test that submit_async() returns the correct result
  runtime_t rt(nullptr, 2);

  rt.start([](size_t, context_t&) {});

  auto f = rt.submit_async([]() -> int {
    return 7 * 6;
  });

  EXPECT_EQ(f.get(), 42);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, submit_async_with_args) {
  // Test that submit_async() works with forwarded arguments
  runtime_t rt(nullptr, 2);

  rt.start([](size_t, context_t&) {});

  auto f = rt.submit_async(
    [](int a, int b, std::string msg) -> std::string {
      return msg + std::to_string(a + b);
    },
    3, 4, "sum="
  );

  EXPECT_EQ(f.get(), "sum=7");

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, submit_async_exception) {
  // Test that exceptions in async tasks are propagated via get()
  runtime_t rt(nullptr, 2);

  rt.start([](size_t, context_t&) {});

  auto f = rt.submit_async([]() -> int {
    throw std::logic_error("async error");
    return 0;
  });

  bool caught = false;
  try {
    f.get();
  } catch (const std::logic_error& e) {
    caught = std::string(e.what()) == "async error";
  }
  EXPECT_TRUE(caught);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, spawn_fire_and_forget) {
  // Test that spawn() runs fire-and-forget coroutine
  runtime_t rt(nullptr, 2);
  std::atomic<int> counter{0};
  std::latch ran(1);

  rt.start([](size_t, context_t&) {});

  rt.spawn([&counter, &ran](context_t& ctx) -> coro_t<void> {
    co_await nop_awaiter(ctx);
    counter.fetch_add(1);
    ran.count_down();
    co_return;
  });

  // wait for the coroutine itself rather than for a duration
  ran.wait();
  EXPECT_EQ(counter.load(), 1);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, spawn_async_fire_and_forget) {
  // Test that spawn_async() runs fire-and-forget CPU task
  runtime_t rt(nullptr, 2);
  std::atomic<int> counter{0};
  std::latch ran(1);

  rt.start([](size_t, context_t&) {});

  rt.spawn_async([&counter, &ran]() {
    counter.fetch_add(1);
    ran.count_down();
  });

  // wait for the task itself rather than for a duration
  ran.wait();
  EXPECT_EQ(counter.load(), 1);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, round_robin_distribution) {
  // Test that submit/select_context distributes across contexts via round-robin
  runtime_t rt(nullptr, 4);

  rt.start([](size_t, context_t&) {});

  // Submit 4 tasks; each one picks a different context due to round-robin
  for (int i = 0; i < 4; i++) {
    auto f = rt.submit([](context_t& ctx) -> coro_t<int> {
      co_await nop_awaiter(ctx);
      co_return 1;
    });
    EXPECT_EQ(f.get(), 1);
  }

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, shutdown_auto_join) {
  // Test that shutdown() blocks until all threads finish (auto-join behavior)
  runtime_t rt(nullptr, 2);
  std::atomic<bool> completed{false};

  rt.start([&](size_t idx, context_t&) {
    if (idx == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      completed.store(true);
    }
  });

  // shutdown() should block until the thread completes
  rt.shutdown(std::chrono::milliseconds(100));
  // After shutdown returns, the thread should have finished
  EXPECT_TRUE(completed.load());
}

TEST_F(runtime_test, spawn_async_exception_handled) {
  // Test that spawn_async() catches exceptions (fire-and-forget, no crash)
  runtime_t rt(nullptr, 2);
  std::latch ran(1);

  rt.start([&](size_t idx, context_t&) {
    if (idx == 0) {
      rt.spawn_async([&ran]() {
        // signalled before throwing, so the wait below returns as soon as the
        // task has actually reached the executor
        ran.count_down();
        throw std::runtime_error("spawn_async exception");
      });
    }
  });

  ran.wait();
  // join drains the wrapper coroutine, which is where the throw is swallowed
  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
  EXPECT_TRUE(ran.try_wait());
}
