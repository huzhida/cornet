#ifndef CORNET_HTTP_COMMON_BUFFER_H
#define CORNET_HTTP_COMMON_BUFFER_H

#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include "cornet/base/defines.h"
#include "cornet/http/common/protocol.h"

namespace cornet::http {

class buffer_pool_t;

/**
 * @brief body aggregation mode, chosen once the headers are known.
 */
enum class body_mode_t : uint8_t {
  Empty,    // no body at all
  Exact,    // Content-Length known and small enough to aggregate
  Stream,   // chunked, oversized, or a route that asked for streaming
};

/**
 * @brief where the bytes of an outbound body live.
 *
 * Shared by both directions: "who owns these bytes until the write completes" is
 * the same question whether a server is answering or a client is asking, and the
 * answer is what decides whether the write can avoid a copy.
 */
enum class body_source_t : uint8_t {
  None,      // no body
  Inline,    // copied into the sender's own body output buffer
  External,  // referenced in place (static storage, or a block we own)
  Streaming, // chunked transfer encoding; chunks are sent incrementally
};

/**
 * @brief RAII lease on a pooled block. Returns it on destruction.
 */
class buffer_lease_t {
 public:
  buffer_lease_t() = default;
  buffer_lease_t(buffer_pool_t* pool, char* data, uint32_t cap)
    : pool_(pool), data_(data), cap_(cap) {}

  ~buffer_lease_t() { release(); }

  buffer_lease_t(const buffer_lease_t&) = delete;
  buffer_lease_t& operator=(const buffer_lease_t&) = delete;

  buffer_lease_t(buffer_lease_t&& o) noexcept
    : pool_(o.pool_), data_(o.data_), cap_(o.cap_) {
    o.pool_ = nullptr; o.data_ = nullptr; o.cap_ = 0;
  }

  buffer_lease_t& operator=(buffer_lease_t&& o) noexcept {
    if (this != &o) {
      release();
      pool_ = o.pool_; data_ = o.data_; cap_ = o.cap_;
      o.pool_ = nullptr; o.data_ = nullptr; o.cap_ = 0;
    }
    return *this;
  }

  CORNET_NODISCARD char*    data() const { return data_; }
  CORNET_NODISCARD uint32_t capacity() const { return cap_; }
  CORNET_NODISCARD bool     valid() const { return data_ != nullptr; }
  explicit operator bool() const { return data_ != nullptr; }

  /**
   * @brief give the block back early; the lease becomes empty.
   */
  void release();

 private:
  buffer_pool_t* pool_{nullptr};
  char*          data_{nullptr};
  uint32_t       cap_{0};
};

/**
 * @brief per-thread block pool, size-classed, intrusive free list.
 *
 * A pool exists so that an idle keep-alive connection can hand its buffers back
 * instead of pinning them: with tens of thousands of mostly-idle connections the
 * resident buffers, not the parser, dominate memory.
 *
 * Blocks larger than the biggest class are served straight from the allocator —
 * rare by construction, and pooling them would let one oversized request poison
 * a class for the process lifetime.
 */
class buffer_pool_t {
 public:
  static constexpr uint32_t kClassCount = 4;
  // 4K covers a bare request line plus a few headers; 16K is the header default;
  // 64K/256K serve aggregated bodies
  static constexpr uint32_t kClassSize[kClassCount] = {4u << 10, 16u << 10, 64u << 10, 256u << 10};

  buffer_pool_t() = default;
  ~buffer_pool_t();

  buffer_pool_t(const buffer_pool_t&) = delete;
  buffer_pool_t& operator=(const buffer_pool_t&) = delete;

  /**
   * @brief take a block of at least min_bytes.
   * @return a lease; invalid only if the allocator failed
   */
  CORNET_NODISCARD buffer_lease_t acquire(uint32_t min_bytes);

  /**
   * @brief return a block. Called by buffer_lease_t; no memset, the next user
   * overwrites what it reads.
   */
  void recycle(char* data, uint32_t cap);

  /**
   * @brief the thread's pool. HTTP is thread-per-core and shared-nothing, so a
   * thread_local pool needs no locks. Kept here rather than on context_t until
   * another module wants pooled blocks too.
   */
  static buffer_pool_t& local();

  CORNET_NODISCARD uint64_t allocations() const { return allocations_; }
  CORNET_NODISCARD uint64_t hits() const { return hits_; }
  CORNET_NODISCARD uint64_t oversized() const { return oversized_; }

 private:
  static int class_of(uint32_t bytes);

  struct node_t { node_t* next; };

  node_t*  free_[kClassCount]{};
  uint64_t allocations_{0};   // blocks obtained from the allocator
  uint64_t hits_{0};          // requests served from a free list
  uint64_t oversized_{0};     // requests larger than the biggest class
};

/**
 * @brief fixed-capacity buffer for the header section of one message.
 *
 * Every header view is a (offset, length) pair rather than a pointer, so that
 * views stay valid no matter what the buffer does. That only works if the buffer
 * does not move bytes underneath a message being parsed, which is the one
 * invariant this class enforces:
 *
 *   compact() may only run at a message boundary.
 *
 * Keeping the header buffer separate from the body — and fixed in size — is what
 * makes the invariant cheap. A single growable buffer holding both would have to
 * honour the same rule while expanding to megabytes, so a large upload would
 * both balloon per-connection memory and copy the whole body on every doubling.
 * Here the capacity is max_header_bytes and an over-long header section is
 * simply a 431.
 */
class head_buffer_t {
 public:
  head_buffer_t() = default;

  /**
   * @brief attach a leased block. Existing contents are dropped.
   */
  void reset(buffer_lease_t lease) {
    lease_ = std::move(lease);
    r_ = w_ = 0;
    parsing_ = false;
  }

  /**
   * @brief hand the block back to the pool.
   */
  void release() {
    lease_.release();
    r_ = w_ = 0;
    parsing_ = false;
  }

  CORNET_NODISCARD bool     attached() const { return lease_.valid(); }
  CORNET_NODISCARD char*    base() const { return lease_.data(); }
  CORNET_NODISCARD uint32_t capacity() const { return lease_.capacity(); }
  CORNET_NODISCARD uint32_t readable() const { return w_ - r_; }
  CORNET_NODISCARD uint32_t read_pos() const { return r_; }
  CORNET_NODISCARD uint32_t write_pos() const { return w_; }

  /**
   * @brief remaining writable tail. Never grows and never moves data, so all
   * recorded offsets stay valid.
   * @return the writable span, empty when the buffer is full
   */
  CORNET_NODISCARD std::span<char> writable() {
    if (!lease_.valid()) return {};
    return {lease_.data() + w_, capacity() - w_};
  }

  void commit(uint32_t n) { w_ += n; }
  void consume(uint32_t n) { r_ += n; }

  /**
   * @brief mark the start/end of parsing one message.
   * Used by the debug assertion that guards the no-move invariant.
   */
  void begin_message() { parsing_ = true; }
  void end_message() { parsing_ = false; }
  CORNET_NODISCARD bool parsing() const { return parsing_; }

  /**
   * @brief move unread bytes to the front, reclaiming consumed space.
   * Only legal between messages: it invalidates every recorded offset.
   * @return true if anything moved
   */
  bool compact();

  /**
   * @brief drop everything written past `pos` and carry on writing there.
   *
   * A message body can be far larger than this buffer, and compact() is illegal
   * while a message is being parsed — it would move the bytes every header view
   * points at. Rewinding to the end of the header section reuses the tail region
   * for the rest of the body instead: body bytes are consumed (copied into the
   * aggregation buffer, or handed to the streaming reader) before the next read
   * overwrites them, while every recorded header offset stays below `pos` and is
   * therefore untouched.
   *
   * Only call this when the parser has nothing pending, i.e. every byte in the
   * window has already been handed over.
   */
  void rewind_to(uint32_t pos) {
    w_ = pos;
    r_ = pos;
  }

  CORNET_NODISCARD std::string_view view(uint32_t off, uint32_t len) const {
    return {lease_.data() + off, len};
  }

  CORNET_NODISCARD uint32_t offset_of(const char* p) const {
    return uint32_t(p - lease_.data());
  }

 private:
  buffer_lease_t lease_;
  uint32_t r_{0};
  uint32_t w_{0};
  bool     parsing_{false};
};

/**
 * @brief exact-size buffer for one aggregated body.
 *
 * Sized once from Content-Length, so there is no reallocation and no copy while
 * the body streams in. For chunked bodies the parser hands over several
 * non-adjacent runs; append() compacts them in place so that body() can hand out
 * one contiguous string_view. That costs a single pass over the body and never
 * needs to grow, because compaction only ever shortens.
 */
class body_buffer_t {
 public:
  body_buffer_t() = default;

  /**
   * @brief prepare for a body of exactly `expected` bytes.
   */
  CORNET_NODISCARD expected<void> reserve_exact(buffer_pool_t& pool, uint64_t expected);

  /**
   * @brief prepare a fixed-size window for streaming reads.
   */
  CORNET_NODISCARD expected<void> reserve_window(buffer_pool_t& pool, uint32_t bytes);

  /**
   * @brief grow to at least `bytes`, keeping what is already there.
   *
   * Needed for a body whose length only becomes known when it ends — a chunked
   * response, or one delimited by connection close. Reserving max_body_bytes up
   * front instead would mean an 8 MB allocation for every small response.
   */
  CORNET_NODISCARD expected<void> grow(buffer_pool_t& pool, uint32_t bytes);

  /**
   * @brief append a run of body bytes, compacting chunked gaps away.
   * @return OutputOverflow if the run does not fit the reservation
   */
  CORNET_NODISCARD expected<void> append(const char* data, uint32_t len);

  void release() {
    lease_.release();
    size_ = 0;
    cap_ = 0;
  }

  void clear() { size_ = 0; }

  CORNET_NODISCARD std::string_view view() const {
    return size_ ? std::string_view(lease_.data(), size_) : std::string_view{};
  }
  CORNET_NODISCARD char*    data() const { return lease_.data(); }
  CORNET_NODISCARD uint32_t size() const { return size_; }
  CORNET_NODISCARD uint32_t capacity() const { return cap_; }
  CORNET_NODISCARD bool     attached() const { return lease_.valid(); }

 private:
  buffer_lease_t lease_;
  uint32_t size_{0};
  uint32_t cap_{0};
};

/**
 * @brief append-only scratch space for values the parser could not keep as a
 * view into the header buffer.
 *
 * llhttp splits a header value across callbacks when it straddles a recv
 * boundary. Almost always the two runs are adjacent in the header buffer and the
 * view simply gets longer. Obsolete line folding and a few other oddities break
 * adjacency; those land here. The counter exists so that a deployment can find
 * out whether its real traffic ever takes this path.
 */
class spill_buffer_t {
 public:
  static constexpr uint32_t kCapacity = 512;

  /**
   * @brief copy bytes in.
   * @return offset of the copied run, or an error when full
   */
  CORNET_NODISCARD expected<uint32_t> put(const char* data, uint32_t len);

  /**
   * @brief extend the run that put() most recently returned.
   */
  CORNET_NODISCARD expected<void> extend(const char* data, uint32_t len);

  CORNET_NODISCARD std::string_view view(uint32_t off, uint32_t len) const {
    return {data_ + off, len};
  }

  void clear() { used_ = 0; }
  CORNET_NODISCARD uint32_t used() const { return used_; }

 private:
  uint32_t used_{0};
  char     data_[kCapacity]{};
};

} // namespace cornet::http

#endif // CORNET_HTTP_COMMON_BUFFER_H
