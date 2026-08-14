#include "cornet/http/buffer.h"

#include <cstdlib>

#include <spdlog/spdlog.h>

namespace cornet::http {

// ─────────────────────────── buffer_lease_t ───────────────────────────

void buffer_lease_t::release() {
  if (pool_ && data_) {
    pool_->recycle(data_, cap_);
  } else if (data_) {
    std::free(data_);
  }
  pool_ = nullptr;
  data_ = nullptr;
  cap_ = 0;
}

// ──────────────────────────── buffer_pool_t ────────────────────────────

buffer_pool_t::~buffer_pool_t() {
  for (uint32_t i = 0; i < kClassCount; ++i) {
    auto* node = free_[i];
    while (node) {
      auto* next = node->next;
      std::free(node);
      node = next;
    }
    free_[i] = nullptr;
  }
}

int buffer_pool_t::class_of(uint32_t bytes) {
  for (uint32_t i = 0; i < kClassCount; ++i) {
    if (bytes <= kClassSize[i]) return int(i);
  }
  return -1;
}

buffer_lease_t buffer_pool_t::acquire(uint32_t min_bytes) {
  int cls = class_of(min_bytes);
  if (cls < 0) {
    // Above the largest class: serve directly and never pool it. Caching such a
    // block would let a single oversized request hold that memory for the
    // process lifetime.
    ++oversized_;
    auto* raw = static_cast<char*>(std::malloc(min_bytes));
    if (!raw) {
      SPDLOG_ERROR("http: failed to allocate {} bytes for oversized body", min_bytes);
      return {};
    }
    return buffer_lease_t{nullptr, raw, min_bytes};
  }

  uint32_t cap = kClassSize[cls];
  if (auto* node = free_[cls]) {
    free_[cls] = node->next;
    ++hits_;
    return buffer_lease_t{this, reinterpret_cast<char*>(node), cap};
  }

  ++allocations_;
  auto* raw = static_cast<char*>(std::malloc(cap));
  if (!raw) {
    SPDLOG_ERROR("http: failed to allocate {} byte pool block", cap);
    return {};
  }
  return buffer_lease_t{this, raw, cap};
}

void buffer_pool_t::recycle(char* data, uint32_t cap) {
  int cls = class_of(cap);
  // Only exact class sizes go back; an oversized block was never ours to keep.
  if (cls < 0 || kClassSize[cls] != cap) {
    std::free(data);
    return;
  }
  auto* node = reinterpret_cast<node_t*>(data);
  node->next = free_[cls];
  free_[cls] = node;
}

buffer_pool_t& buffer_pool_t::local() {
  static thread_local buffer_pool_t pool;
  return pool;
}

// ──────────────────────────── head_buffer_t ────────────────────────────

bool head_buffer_t::compact() {
  // Moving bytes invalidates every (offset, len) view recorded so far, so this
  // must never run while a message is being parsed. Cheap to assert, and the
  // assertion is the only thing keeping the invariant honest as the code grows.
  CORNET_ASSERT(!parsing_, "head_buffer_t::compact() during message parse would invalidate views");
  if (r_ == 0) return false;
  uint32_t n = w_ - r_;
  if (n > 0) {
    std::memmove(lease_.data(), lease_.data() + r_, n);
  }
  r_ = 0;
  w_ = n;
  return true;
}

// ──────────────────────────── body_buffer_t ────────────────────────────

expected<void> body_buffer_t::reserve_exact(buffer_pool_t& pool, uint64_t expected_bytes) {
  release();
  if (expected_bytes == 0) return {};
  if (expected_bytes > 0xffffffffull) {
    return http_unexpected(http_error_t::BodyTooLarge);
  }
  auto want = uint32_t(expected_bytes);
  lease_ = pool.acquire(want);
  if (!lease_) return unexpected(ENOMEM);
  cap_ = want;
  size_ = 0;
  return {};
}

expected<void> body_buffer_t::reserve_window(buffer_pool_t& pool, uint32_t bytes) {
  release();
  if (bytes == 0) return {};
  lease_ = pool.acquire(bytes);
  if (!lease_) return unexpected(ENOMEM);
  cap_ = bytes;
  size_ = 0;
  return {};
}

expected<void> body_buffer_t::append(const char* data, uint32_t len) {
  if (len == 0) return {};
  if (size_ + len > cap_) {
    return http_unexpected(http_error_t::OutputOverflow);
  }
  // For a chunked body the runs handed over by the parser are separated by the
  // chunk-size lines; copying each run to the write cursor squeezes those gaps
  // out so body() can be one contiguous view.
  std::memcpy(lease_.data() + size_, data, len);
  size_ += len;
  return {};
}

// ─────────────────────────── spill_buffer_t ───────────────────────────

expected<uint32_t> spill_buffer_t::put(const char* data, uint32_t len) {
  if (used_ + len > kCapacity) {
    return http_unexpected(http_error_t::HeaderTooLarge);
  }
  uint32_t off = used_;
  std::memcpy(data_ + off, data, len);
  used_ += len;
  return off;
}

expected<void> spill_buffer_t::extend(const char* data, uint32_t len) {
  if (used_ + len > kCapacity) {
    return http_unexpected(http_error_t::HeaderTooLarge);
  }
  std::memcpy(data_ + used_, data, len);
  used_ += len;
  return {};
}

} // namespace cornet::http
