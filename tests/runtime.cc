#include "core/runtime.h"
#include "core/combinators.h"

#include <gtest/gtest.h>
#include <atomic>
#include <set>

using namespace cornet;

class runtime_test : public ::testing::Test {};

TEST_F(runtime_test, start_and_shutdown) {
  runtime_t rt(2);
  std::atomic<int> init_count{0};

  rt.start([&](context_t& ctx, size_t idx) {
    init_count.fetch_add(1);
  });

  EXPECT_EQ(init_count.load(), 2);
  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, spawn_remote_basic) {
  runtime_t rt(2);
  std::atomic<int> executed{0};
  std::atomic<std::thread::id> exec_thread{};

  rt.start([&](context_t& ctx, size_t idx) {
    if (idx == 0) {
      // thread 0 submits a coroutine to thread 1
      rt.context(1)->spawn_remote([&executed, &exec_thread]() -> coro_t<void> {
        executed.store(1);
        exec_thread.store(std::this_thread::get_id());
        co_return;
      });
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(executed.load(), 1);

  // verify coroutine executed on thread 1 (not thread 0)
  std::thread::id t1_owner = rt.context(1)->owner_thread();
  EXPECT_EQ(exec_thread.load(), t1_owner);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, spawn_remote_with_value) {
  runtime_t rt(2);
  std::atomic<int> result{0};

  rt.start([&](context_t& ctx, size_t idx) {
    if (idx == 0) {
      int value = 42;
      rt.context(1)->spawn_remote([value, &result]() -> coro_t<void> {
        result.store(value);
        co_return;
      });
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(result.load(), 42);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, multi_producer_spawn_remote) {
  runtime_t rt(4);
  std::atomic<int> counter{0};

  rt.start([&](context_t& ctx, size_t idx) {
    if (idx != 0) {
      // threads 1, 2, 3 all submit to thread 0
      rt.context(0)->spawn_remote([&counter]() -> coro_t<void> {
        counter.fetch_add(1);
        co_return;
      });
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(counter.load(), 3);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, next_context_round_robin) {
  runtime_t rt(4);

  rt.start([](context_t& ctx, size_t) {});

  std::set<context_t*> seen;
  for (int i = 0; i < 8; ++i) {
    seen.insert(&rt.next_context());
  }
  EXPECT_EQ(seen.size(), 4u);

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}

TEST_F(runtime_test, spawn_remote_io_operation) {
  // test that spawn_remote coroutines can perform IO on the target thread
  runtime_t rt(2);
  std::atomic<bool> io_succeeded{false};

  rt.start([&](context_t& ctx, size_t idx) {
    if (idx == 0) {
      rt.context(1)->spawn_remote([&io_succeeded]() -> coro_t<void> {
        // perform a sleep (which is an io_uring timeout operation)
        auto ret = co_await sleep(std::chrono::milliseconds(10));
        if (ret.has_value()) {
          io_succeeded.store(true);
        }
        co_return;
      });
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(io_succeeded.load());

  rt.shutdown(std::chrono::milliseconds(100));
  rt.join();
}
