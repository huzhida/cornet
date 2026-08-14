#include <gtest/gtest.h>

#include "cornet/http/buffer.h"

using namespace cornet;
using namespace cornet::http;

namespace {

buffer_pool_t& pool() { return buffer_pool_t::local(); }

} // namespace

// ─────────────────────────── buffer_pool_t ───────────────────────────

TEST(http_buffer, pool_reuses_blocks) {
  buffer_pool_t p;
  char* first = nullptr;
  {
    auto lease = p.acquire(1024);
    ASSERT_TRUE(lease.valid());
    EXPECT_EQ(lease.capacity(), buffer_pool_t::kClassSize[0]);
    first = lease.data();
  }
  auto again = p.acquire(1024);
  ASSERT_TRUE(again.valid());
  // the block came back from the free list rather than the allocator
  EXPECT_EQ(again.data(), first);
  EXPECT_EQ(p.hits(), 1u);
  EXPECT_EQ(p.allocations(), 1u);
}

TEST(http_buffer, pool_picks_size_class) {
  buffer_pool_t p;
  EXPECT_EQ(p.acquire(1).capacity(), 4u << 10);
  EXPECT_EQ(p.acquire(5000).capacity(), 16u << 10);
  EXPECT_EQ(p.acquire(20000).capacity(), 64u << 10);
  EXPECT_EQ(p.acquire(70000).capacity(), 256u << 10);
}

TEST(http_buffer, oversized_blocks_are_not_pooled) {
  buffer_pool_t p;
  uint32_t huge = (1u << 20);
  {
    auto lease = p.acquire(huge);
    ASSERT_TRUE(lease.valid());
    EXPECT_EQ(lease.capacity(), huge);
  }
  EXPECT_EQ(p.oversized(), 1u);
  // returning it must not poison a size class
  auto again = p.acquire(1024);
  EXPECT_EQ(again.capacity(), 4u << 10);
  EXPECT_EQ(p.hits(), 0u);
}

TEST(http_buffer, lease_move_transfers_ownership) {
  buffer_pool_t p;
  auto a = p.acquire(1024);
  char* data = a.data();
  auto b = std::move(a);
  EXPECT_FALSE(a.valid());
  EXPECT_EQ(b.data(), data);
}

// ─────────────────────────── head_buffer_t ───────────────────────────

TEST(http_buffer, head_writable_never_grows) {
  head_buffer_t buf;
  buf.reset(pool().acquire(4096));
  auto cap = buf.capacity();

  auto w = buf.writable();
  EXPECT_EQ(w.size(), cap);
  buf.commit(100);
  EXPECT_EQ(buf.readable(), 100u);
  EXPECT_EQ(buf.writable().size(), cap - 100);

  // filling it completely leaves no writable tail; the caller answers 431
  buf.commit(cap - 100);
  EXPECT_TRUE(buf.writable().empty());
}

TEST(http_buffer, head_offsets_survive_writes) {
  head_buffer_t buf;
  buf.reset(pool().acquire(4096));

  const char* text = "Host: example.com";
  std::memcpy(buf.writable().data(), text, std::strlen(text));
  buf.commit(uint32_t(std::strlen(text)));
  auto view = buf.view(6, 11);
  EXPECT_EQ(view, "example.com");

  // appending more must not disturb a recorded view: that is the whole point of
  // storing offsets instead of pointers
  const char* more = "\r\nAccept: */*";
  std::memcpy(buf.writable().data(), more, std::strlen(more));
  buf.commit(uint32_t(std::strlen(more)));
  EXPECT_EQ(buf.view(6, 11), "example.com");
}

TEST(http_buffer, head_compact_reclaims_consumed_space) {
  head_buffer_t buf;
  buf.reset(pool().acquire(4096));
  std::memcpy(buf.writable().data(), "AAAABBBB", 8);
  buf.commit(8);
  buf.consume(4);
  EXPECT_EQ(buf.readable(), 4u);

  EXPECT_TRUE(buf.compact());
  EXPECT_EQ(buf.read_pos(), 0u);
  EXPECT_EQ(buf.readable(), 4u);
  EXPECT_EQ(buf.view(0, 4), "BBBB");
}

TEST(http_buffer, head_compact_is_noop_at_origin) {
  head_buffer_t buf;
  buf.reset(pool().acquire(4096));
  buf.commit(4);
  EXPECT_FALSE(buf.compact());
}

// ─────────────────────────── body_buffer_t ───────────────────────────

TEST(http_buffer, body_exact_reserve_holds_content_length) {
  body_buffer_t body;
  ASSERT_TRUE(body.reserve_exact(pool(), 11));
  ASSERT_TRUE(body.append("hello", 5));
  ASSERT_TRUE(body.append(" world", 6));
  EXPECT_EQ(body.view(), "hello world");
  EXPECT_EQ(body.size(), 11u);
}

TEST(http_buffer, body_append_beyond_reservation_fails) {
  body_buffer_t body;
  ASSERT_TRUE(body.reserve_exact(pool(), 4));
  ASSERT_TRUE(body.append("abcd", 4));
  auto r = body.append("e", 1);
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().domain, error_domain::Http);
  EXPECT_EQ(r.error().code, int(http_error_t::OutputOverflow));
}

TEST(http_buffer, body_append_concatenates_separate_runs) {
  // A chunked body arrives as separate runs with chunk-size lines in between. The
  // buffer only ever receives the payload runs, so appending them must produce one
  // contiguous view — that is what lets request_t::body() be a string_view.
  body_buffer_t body;
  ASSERT_TRUE(body.reserve_exact(pool(), 32));
  ASSERT_TRUE(body.append("Wiki", 4));
  ASSERT_TRUE(body.append("pedia", 5));
  ASSERT_TRUE(body.append(" in chunks", 10));
  EXPECT_EQ(body.view(), "Wikipedia in chunks");
  EXPECT_EQ(body.size(), 19u);
}

TEST(http_buffer, body_zero_length_reserve_allocates_nothing) {
  body_buffer_t body;
  ASSERT_TRUE(body.reserve_exact(pool(), 0));
  EXPECT_FALSE(body.attached());
  EXPECT_TRUE(body.view().empty());
}

// ─────────────────────────── spill_buffer_t ───────────────────────────

TEST(http_buffer, spill_put_and_extend) {
  spill_buffer_t spill;
  auto off = spill.put("abc", 3);
  ASSERT_TRUE(off);
  EXPECT_EQ(*off, 0u);
  ASSERT_TRUE(spill.extend("def", 3));
  EXPECT_EQ(spill.view(0, 6), "abcdef");
  EXPECT_EQ(spill.used(), 6u);
}

TEST(http_buffer, spill_overflow_reports_header_too_large) {
  spill_buffer_t spill;
  std::vector<char> big(spill_buffer_t::kCapacity, 'x');
  ASSERT_TRUE(spill.put(big.data(), uint32_t(big.size())));
  auto r = spill.put("y", 1);
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().code, int(http_error_t::HeaderTooLarge));
}
