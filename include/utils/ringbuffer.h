#ifndef CORNET_RINGBUFFER_H
#define CORNET_RINGBUFFER_H

#include <atomic>
#include <memory>
#include "utils/defines.h"

namespace cornet {
/**
 * @brief container type for placement_new-able array
 * @tparam T value type
 * @tparam N size
 */
template <typename T, size_t N>
struct buffer_t {
  // storage
  alignas(T) std::byte a[N * sizeof(T)];

  /**
   * @brief get value by index
   * @param index value index in storage
   * @return value reference
   */
  T* at(size_t index) {
    return reinterpret_cast<T*>(a + index * sizeof(T));
  }

  /**
   * @brief do placement_new on storage index.
   * @tparam Args T's initial args type pack
   * @param index index that need placement new
   * @param args T's initial args pack
   */
  template <typename... Args>
  void placement_new(size_t index, Args&&... args) {
    new(at(index)) T(std::forward<Args>(args)...);
  }
};

/**
 * @brief single producer single consumer lock-free ringbuffer.
 * @tparam T value type
 * @tparam N ringbuffer size, must be power of 2
 */
template <typename T, size_t N>
struct ringbuffer_t {
  static_assert(N > 0 && !(N & (N - 1)), "ringbuffer_t need size N must be power of 2");
  using container_t = std::conditional_t<std::is_trivial_v<T>, std::array<T, N>, buffer_t<T, N> >;

  ringbuffer_t()
    : read(0), write(0), read_cache(0), write_cache(0) {}

  ~ringbuffer_t() {
    size_t w = write.load(std::memory_order_relaxed);
    size_t r = read.load(std::memory_order_relaxed);

    if constexpr (!std::is_trivial_v<T>) {
      while (r != w) {
        container.at(r)->~T();
        r = (r + 1) & (N - 1);
      }
    }
  }

  /**
   * @brief lock-free push value to back
   * @tparam U value type
   * @param t value
   * @return true for success / false for failed
   */
  template <typename U>
  bool push(U&& t) {
    size_t w = write.load(std::memory_order_relaxed);
    auto next_w = (w + 1) & (N - 1);

    if (next_w == read_cache) {
      read_cache = read.load(std::memory_order_acquire);
      if (next_w == read_cache)
        return false;
    }

    if constexpr (std::is_trivially_copyable<T>::value && std::is_trivially_destructible_v<T>) {
      container[w] = std::forward<U>(t);
    } else {
      container.placement_new(w, std::forward<U>(t));
    }

    write.store(next_w, std::memory_order_release);
    return true;
  }

  /**
   * @brief emplace value on back of ringbuffer
   * @tparam Args value's args type pack
   * @param args value's args pack
   * @return true for success / false for failed
   */
  template <typename... Args>
  bool emplace(Args&&... args) {
    size_t w = write.load(std::memory_order_relaxed);
    auto next_w = (w + 1) & (N - 1);

    if (next_w == read_cache) {
      read_cache = read.load(std::memory_order_acquire);
      if (next_w == read_cache)
        return false;
    }

    if constexpr (std::is_trivially_copyable<T>::value && std::is_trivially_destructible_v<T>) {
      container[w] = T(std::forward<Args>(args)...);
    } else {
      container.placement_new(w, std::forward<Args>(args)...);
    }

    write.store(next_w, std::memory_order_release);
    return true;
  }

  /**
   * @brief lock-free pop from ringbuffer front
   * @param t destination value reference
   * @return true for success / false for failed
   */
  bool pop(T& t) {
    size_t r = read.load(std::memory_order_relaxed);

    if (r == write_cache) {
      write_cache = write.load(std::memory_order_acquire);
      if (r == write_cache)
        return false;
    }

    if constexpr (std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>) {
      t = container[r];
    } else {
      auto slot = container.at(r);
      t = std::move(*slot);
      if constexpr (!std::is_trivially_destructible_v<T>) {
        slot->~T();
      }
    }

    read.store((r + 1) & (N - 1), std::memory_order_release);
    return true;
  }

private:
  // front cursor, align as cache line, avoid fake share.
  alignas(CORNET_CACHE_LINE) std::atomic<size_t> read;
  size_t write_cache;
  // back cursor, align as cache line, avoid fake share.
  alignas(CORNET_CACHE_LINE) std::atomic<size_t> write;
  size_t read_cache;
  // storage
  alignas(CORNET_CACHE_LINE) container_t container;
};
}


#endif //CORNET_RINGBUFFER_H