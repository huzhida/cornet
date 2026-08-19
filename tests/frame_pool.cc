// frame_pool.cc — allocation-path tests that need no io_uring: coroutine
// frames are driven by hand through coro_t/generator_t, so the pool's local,
// remote-free, global and over-aligned paths can be exercised on any kernel.

#include "cornet/coroutine/coro.h"
#include "cornet/coroutine/frame_pool.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <thread>
#include <vector>

using namespace cornet;

namespace {

coro_t<int> add(int a, int b) {
  co_return a + b;
}

coro_t<int> nested(int a, int b) {
  int x = co_await add(a, b);
  co_return x * 2;
}

coro_t<void> thrower() {
  throw std::runtime_error("boom");
  co_return; // unreachable, keeps the coroutine shape
}

generator_t<int> gen(int n) {
  for (int i = 0; i < n; i++) co_yield i;
}

coro_t<int> big_frame(int v) {
  // frame grows past the 16 KiB pool ceiling → global operator new path
  char buf[24 * 1024];
  buf[0] = char(v);
  buf[sizeof(buf) - 1] = 1;
  co_return buf[0] + buf[sizeof(buf) - 1];
}

struct alignas(64) over_aligned_t {
  char c;
};

coro_t<int> aligned_frame(int v) {
  // forces >16B frame alignment → align_val_t operator new path
  over_aligned_t x{char(v)};
  co_return x.c;
}

TEST(frame_pool, value_roundtrip) {
  auto c = add(40, 2);
  c.resume();
  ASSERT_TRUE(c.done());
  EXPECT_EQ(c.value(), 42);
}

TEST(frame_pool, nested_co_await) {
  auto c = nested(10, 5);
  c.resume();
  ASSERT_TRUE(c.done());
  EXPECT_EQ(c.value(), 30);
}

TEST(frame_pool, exception_propagates) {
  auto c = thrower();
  c.resume();
  ASSERT_TRUE(c.done());
  EXPECT_THROW(c.value(), std::runtime_error);
}

TEST(frame_pool, generator) {
  int sum = 0;
  for (int v : gen(100)) sum += v;
  EXPECT_EQ(sum, 4950);
}

TEST(frame_pool, reuse_under_churn) {
  // Steady state: each iteration frees a frame that the next alloc reuses.
  for (int i = 0; i < 200000; i++) {
    auto c = add(i, 1);
    c.resume();
    ASSERT_TRUE(c.done());
    ASSERT_EQ(c.value(), i + 1);
  }
}

TEST(frame_pool, mixed_sizes_churn) {
  for (int i = 0; i < 20000; i++) {
    auto a = add(i, 1);         // small frame
    auto b = big_frame(i & 7);  // >16 KiB, global path interleaved
    a.resume();
    b.resume();
    ASSERT_EQ(a.value(), i + 1);
    ASSERT_EQ(b.value(), (i & 7) + 1);
  }
}

TEST(frame_pool, over_aligned_frame) {
  auto c = aligned_frame(17);
  c.resume();
  ASSERT_TRUE(c.done());
  EXPECT_EQ(c.value(), 17);
}

TEST(frame_pool, released_without_resume) {
  // abandoned frames must free cleanly through the same header path
  std::vector<coro_t<int>> held;
  for (int i = 0; i < 1000; i++) held.push_back(add(i, i));
}

TEST(frame_pool, cross_thread_destroy) {
  // The spawn_remote shape: frames created on this thread, resumed and
  // destroyed on another. The free must land on the creating pool's inbound
  // stack, never on the destroying thread's freelist.
  std::vector<coro_t<int>> cs;
  for (int i = 0; i < 4000; i++) cs.push_back(add(i, 0));

  std::thread t([&] {
    for (auto& c : cs) c.resume();
    // vector elements destroyed here, on the consumer thread
  });
  t.join();
}

TEST(frame_pool, cross_thread_ping_pong) {
  for (int round = 0; round < 50; round++) {
    std::vector<coro_t<int>> cs;
    for (int i = 0; i < 200; i++) cs.push_back(nested(i, 1));
    std::thread t([&] {
      for (auto& c : cs) c.resume();
    });
    t.join();
  }
}

TEST(frame_pool, pool_reclaims_remote_blocks) {
  // After remote frees, this thread's pool must hand the blocks back out.
  std::vector<coro_t<int>> cs;
  for (int i = 0; i < 500; i++) cs.push_back(add(i, 0));
  std::thread t([&] {
    for (auto& c : cs) c.resume();
  });
  t.join();
  for (int i = 0; i < 500; i++) {
    auto c = add(i, 2);
    c.resume();
    ASSERT_EQ(c.value(), i + 2);
  }
}

} // namespace
