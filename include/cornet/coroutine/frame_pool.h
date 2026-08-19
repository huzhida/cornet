#ifndef CORNET_FRAME_POOL_H
#define CORNET_FRAME_POOL_H

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>

// Deliberately NOT keyed on __SANITIZE_ADDRESS__: user translation units may
// compile without -fsanitize=address yet run under an ASan-instrumented
// binary (this project's own tests do exactly that — the flag is PRIVATE to
// the library targets while the runtime is linked in process-wide). A weak
// symbol resolves at runtime instead: null without an LSan runtime, callable
// with one.
extern "C" void __lsan_ignore_object(const void* p) __attribute__((weak));

namespace cornet::detail {

/**
 * @brief keep the intentional arena leaks out of LeakSanitizer reports.
 */
inline void lsan_ignore(const void* p) noexcept {
  if (&__lsan_ignore_object != nullptr) __lsan_ignore_object(p);
}

/**
 * @brief thread-local size-class pool for coroutine frames.
 *
 * Frames may be created on one thread and destroyed on another:
 * spawn_remote() builds the wrapper coroutine on the calling thread, then the
 * handle crosses to the owner context which eventually destroys it. A plain
 * thread_local freelist would corrupt in that case (thread B would push
 * thread A's block onto B's list), and std::pmr::unsynchronized_pool_resource
 * has the same problem; the synchronized variant pays a lock and a virtual
 * call per op. tcmalloc-style instead:
 *
 * - alloc: pop the thread-local freelist, or carve from a 64 KiB chunk.
 * - free: read the header's owner; same thread pushes locally, a foreign
 *   thread publishes on the owner's inbound stack (Treiber, single consumer,
 *   so a plain exchange-based drain is safe) which the owner reclaims on its
 *   next alloc.
 *
 * Pool objects are intentionally leaked so a remote frame freed after the
 * creating thread has exited is still safe. Chunks are likewise never
 * returned to the OS: RSS plateaus at the steady-state working set.
 *
 * The pool is also reachable from non-context threads (user threads calling
 * runtime_t::submit), which get their own instances.
 */
class frame_pool_t {
public:
  static constexpr std::size_t kHeaderSize = 16;
  static constexpr std::size_t kMinClassSize = 64;
  static constexpr std::size_t kMaxClassSize = 1 << 14; // 16 KiB
  static constexpr std::size_t kChunkSize = 1 << 16;

  // pow2 classes 64,128,...,16384 → bit_width-based index
  static constexpr int kClassCount = 9;

private:
  struct node_t {
    node_t* next;
  };

  // Owner field doubles as the freelist/remote link: once a block is pushed,
  // ownership has been decided, so overlaying is safe. class_idx at offset 8
  // stays intact while linked, letting drain_remote() route the block without
  // size information.
  struct header_t {
    frame_pool_t* owner;   // null ⇒ global ::operator new block
    std::uint32_t class_idx;
    std::uint32_t frame_size; // kept for unsized/global deletes
  };
  static_assert(sizeof(header_t) == kHeaderSize);

  static int class_index(std::size_t total) noexcept {
    // class i covers sizes (32 << i, 64 << i]; total ≥ 17 always
    int idx = int(std::bit_width(total - 1)) - 6; // 64 → class 0
    return idx < 0 ? 0 : idx;
  }

  static void* alloc_global(std::size_t frame_size, std::size_t total) {
    void* raw = ::operator new(total);
    auto* h = static_cast<header_t*>(raw);
    h->owner = nullptr;
    h->class_idx = 0;
    h->frame_size = static_cast<std::uint32_t>(frame_size);
    return h + 1;
  }

  void new_chunk() {
    void* c = ::operator new(kChunkSize, std::align_val_t{64});
    // Chunk-link stored in the first 16 bytes: every slot carved afterwards is
    // 16-byte aligned (slots are multiples of 64), matching the alignment
    // guarantee malloc gives coroutine frames.
    static_assert(sizeof(void*) <= 16);
    *static_cast<void**>(c) = chunks_;
    chunks_ = c;
    lsan_ignore(c); // intentionally never returned to the OS
    chunk_cur_ = static_cast<char*>(c) + 16;
    chunk_left_ = kChunkSize - 16;
  }

  void drain_remote() noexcept {
    node_t* n = remote_head_.exchange(nullptr, std::memory_order_acquire);
    while (n != nullptr) {
      node_t* next = n->next;
      auto* h = reinterpret_cast<header_t*>(n);
      std::uint32_t idx = h->class_idx;
      n->next = freelist_[idx];
      freelist_[idx] = n;
      n = next;
    }
  }

public:
  frame_pool_t() = default;
  frame_pool_t(const frame_pool_t&) = delete;
  frame_pool_t& operator=(const frame_pool_t&) = delete;

  void* alloc(std::size_t frame_size) {
    if (remote_head_.load(std::memory_order_relaxed) != nullptr) drain_remote();
    std::size_t total = frame_size + kHeaderSize;
    if (total > kMaxClassSize) return alloc_global(frame_size, total);
    int idx = class_index(total);
    node_t* n = freelist_[idx];
    if (n != nullptr) {
      freelist_[idx] = n->next;
    } else {
      std::size_t slot = kMinClassSize << idx;
      if (chunk_left_ < slot) new_chunk();
      n = reinterpret_cast<node_t*>(chunk_cur_);
      chunk_cur_ += slot;
      chunk_left_ -= slot;
    }
    auto* h = reinterpret_cast<header_t*>(n);
    h->owner = this;
    h->class_idx = static_cast<std::uint32_t>(idx);
    h->frame_size = static_cast<std::uint32_t>(frame_size);
    return h + 1;
  }

  /**
   * @brief free a frame allocated by alloc(); frame_size must match the alloc
   * call (the compiler's sized delete passes it) but is informational only —
   * the header drives every decision.
   */
  static void free(void* p, [[maybe_unused]] std::size_t frame_size) noexcept;

private:
  void free_local_or_remote(header_t* h) noexcept {
    frame_pool_t* owner = h->owner;
    auto* n = reinterpret_cast<node_t*>(h);
    if (owner == this) {
      std::uint32_t idx = h->class_idx;
      n->next = freelist_[idx];
      freelist_[idx] = n;
      return;
    }
    // Cross-thread free: publish on the owner's inbound stack. The pool
    // outlives every producer (it is leaked), so this CAS never targets
    // released memory.
    node_t* head = owner->remote_head_.load(std::memory_order_relaxed);
    do {
      n->next = head;
    } while (!owner->remote_head_.compare_exchange_weak(
        head, n, std::memory_order_release, std::memory_order_relaxed));
  }

  alignas(64) node_t* freelist_[kClassCount]{};
  char* chunk_cur_{nullptr};
  std::size_t chunk_left_{0};
  void* chunks_{nullptr};
  // Separate line: producers CAS this while the owner mutates the freelist.
  alignas(64) std::atomic<node_t*> remote_head_{nullptr};
};

/**
 * @brief the calling thread's private frame pool.
 */
inline frame_pool_t& tls_frame_pool() {
  // Leaked on purpose: outlives the thread so late frees of remotely-created
  // frames (spawn_remote) still have a live owner to push onto.
  static thread_local frame_pool_t* pool = new frame_pool_t();
  lsan_ignore(pool);
  return *pool;
}

inline void frame_pool_t::free(void* p, std::size_t frame_size) noexcept {
  auto* h = reinterpret_cast<header_t*>(p) - 1;
  frame_pool_t* owner = h->owner;
  if (owner == nullptr) {
    // global block: sized delete matching alloc_global()
    ::operator delete(h, static_cast<std::size_t>(h->frame_size) + kHeaderSize);
    return;
  }
  owner->free_local_or_remote(h);
}

/**
 * @brief allocation entry points used by promise operator new/delete.
 */
inline void* frame_alloc(std::size_t n) {
  return tls_frame_pool().alloc(n);
}

inline void frame_free(void* p, std::size_t n) noexcept {
  frame_pool_t::free(p, n);
}

inline void* frame_alloc(std::size_t n, std::align_val_t al) {
  if (static_cast<std::size_t>(al) <= frame_pool_t::kHeaderSize) return frame_alloc(n);
  // over-aligned frames are rare (e.g. alignas(64) locals); no header, direct
  return ::operator new(n, al);
}

inline void frame_free(void* p, std::size_t n, std::align_val_t al) noexcept {
  if (static_cast<std::size_t>(al) <= frame_pool_t::kHeaderSize) {
    frame_pool_t::free(p, n);
  } else if (n != 0) {
    ::operator delete(p, n, al);
  } else {
    ::operator delete(p, al);
  }
}

/**
 * @brief mixin giving coroutine promise types pooled frame allocation.
 * Inherited once by base_promise_t so every coroutine wrapper in the tree
 * (coro_t, ccoro_t, generator_t, wrappers) shares one pool.
 * All six overloads: compilers may pick sized or unsized delete, and aligned
 * forms whenever a frame is over-aligned.
 */
struct frame_allocator_mixin_t {
  static void* operator new(std::size_t n) { return frame_alloc(n); }
  static void operator delete(void* p, std::size_t n) noexcept { frame_free(p, n); }
  static void operator delete(void* p) noexcept { frame_free(p, 0); }

  static void* operator new(std::size_t n, std::align_val_t al) { return frame_alloc(n, al); }
  static void operator delete(void* p, std::size_t n, std::align_val_t al) noexcept {
    frame_free(p, n, al);
  }
  static void operator delete(void* p, std::align_val_t al) noexcept { frame_free(p, 0, al); }
};

} // namespace cornet::detail

#endif // CORNET_FRAME_POOL_H
