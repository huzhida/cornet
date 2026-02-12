#ifndef CORNET_RINGBUFFER_H
#define CORNET_RINGBUFFER_H

#include <atomic>
#include <memory>

template<typename T, size_t N>
struct ringbuffer_t {
  static_assert(N > 0 && !(N&(N-1)), "ringbuffer_t need size N must be power of 2");

  struct buffer {
    alignas(T) std::byte a[N * sizeof(T)];

    T* at(size_t index) {
      return reinterpret_cast<T*>(a + index * sizeof(T));
    }
  };
  using container_t = std::conditional_t<std::is_trivial_v<T>, std::array<T, N>, buffer>;
  ringbuffer_t() : read(0), write(0), read_cache(0), write_cache(0) {}
  ~ringbuffer_t() {
    size_t w = write.load(std::memory_order_relaxed);
    size_t r = read.load(std::memory_order_relaxed);

    if constexpr(!std::is_trivial_v<T>) {
      while(r != w) {
        container.at(r)->~T();
        r = (r+1) & (N-1);
      }
    }
  }

  template<typename U>
  bool push(U&& t) {
    size_t w = write.load(std::memory_order_relaxed);
    auto next_w = (w + 1) & (N -1);

    if (next_w == read_cache) {
      read_cache = read.load(std::memory_order_acquire);
      if (next_w == read_cache) return false;
    }

    if constexpr (std::is_trivially_copyable<T>::value && std::is_trivially_destructible_v<T>) {
      container[w] = std::forward<U>(t);
    } else {
      auto c = container.at(w);
      new (c) T(std::forward<U>(t));
    }

    write.store(next_w, std::memory_order_release);
    return true;
  }

  template<typename ...Args>
  bool emplace(Args... args) {
    size_t w = write.load(std::memory_order_relaxed);
    auto next_w = (w + 1) & (N -1);

    if (next_w == read_cache) {
      read_cache = read.load(std::memory_order_acquire);
      if (next_w == read_cache) return false;
    }

    if constexpr (std::is_trivially_copyable<T>::value && std::is_trivially_destructible_v<T>) {
      container[w] = T(std::forward<Args>(args)...);
    } else {
      auto c = container.at(w);
      new (c) T(std::forward<Args>(args)...);
    }

    write.store(next_w, std::memory_order_release);
    return true;
  }

  bool pop(T& t) {
    size_t r = read.load(std::memory_order_relaxed);

    if (r == write_cache) {
      write_cache = write.load(std::memory_order_acquire);
      if (r == write_cache) return false;
    }

    if constexpr (std::is_trivially_copyable<T>::value && std::is_trivially_destructible_v<T>) {
      t = container[r];
    } else {
      auto slot = container.at(r);
      t =  std::move(*slot);
      if constexpr(!std::is_trivially_destructible_v<T>) {
        slot->~T();
      }
    }

    read.store((r+1) & (N-1), std::memory_order_release);
    return true;
  }
 private:
  alignas(64) std::atomic<size_t> read;
  size_t write_cache;

  alignas(64) std::atomic<size_t> write;
  size_t read_cache;

  alignas(64) container_t container;
};

#endif //CORNET_RINGBUFFER_H
