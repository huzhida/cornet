#ifndef CORNET_EXPECTED_H
#define CORNET_EXPECTED_H

#include <cstring>
#include <netdb.h>
#include <utility>

namespace cornet {

/**
 * @brief error domain, distinguishes different error code namespaces
 */
enum class error_domain : uint8_t {
  none,       // no error
  system,     // errno (POSIX system call errors)
  resolve,    // EAI_* (getaddrinfo errors)
  internal,   // framework internal errors
};

/**
 * @brief unified error type supporting multiple error domains
 */
struct error_t {
  int code{0};
  error_domain domain{error_domain::none};

  explicit operator bool() const { return domain != error_domain::none; }

  /**
   * @brief get human-readable error message based on domain
   */
  const char* message() const {
    switch (domain) {
      case error_domain::system: return strerror(code);
      case error_domain::resolve: return gai_strerror(code);
      case error_domain::internal: return "internal error";
      default: return "no error";
    }
  }
};

/**
 * @brief wrapper type for constructing expected in error state.
 * Enables concise error returns: return unexpected(errno) or unexpected(code, domain).
 */
struct unexpected {
  error_t err;

  unexpected(int code, error_domain domain = error_domain::system)
    : err{code, domain} {}

  explicit unexpected(error_t e) : err(e) {}
};

/**
 * @brief lightweight expected type for non-void values.
 * Zero-overhead: no exceptions, no heap allocation.
 * Uses union storage to avoid requiring default-constructible T.
 * @tparam T value type on success
 */
template<typename T>
class expected {
 public:
  expected() : err_{}, ok_(false) {}

  expected(T val) : ok_(true) {
    new (&storage_) T(std::move(val));
  }

  expected(unexpected e) : err_(e.err), ok_(false) {}

  ~expected() {
    if (ok_) {
      reinterpret_cast<T*>(&storage_)->~T();
    }
  }

  expected(const expected& other) : err_(other.err_), ok_(other.ok_) {
    if (ok_) {
      new (&storage_) T(*reinterpret_cast<const T*>(&other.storage_));
    }
  }

  expected(expected&& other) noexcept : err_(other.err_), ok_(other.ok_) {
    if (ok_) {
      new (&storage_) T(std::move(*reinterpret_cast<T*>(&other.storage_)));
    }
  }

  expected& operator=(const expected& other) {
    if (this != &other) {
      if (ok_) reinterpret_cast<T*>(&storage_)->~T();
      ok_ = other.ok_;
      err_ = other.err_;
      if (ok_) new (&storage_) T(*reinterpret_cast<const T*>(&other.storage_));
    }
    return *this;
  }

  expected& operator=(expected&& other) noexcept {
    if (this != &other) {
      if (ok_) reinterpret_cast<T*>(&storage_)->~T();
      ok_ = other.ok_;
      err_ = other.err_;
      if (ok_) new (&storage_) T(std::move(*reinterpret_cast<T*>(&other.storage_)));
    }
    return *this;
  }

  explicit operator bool() const { return ok_; }

  bool has_value() const { return ok_; }

  T& value() { return *reinterpret_cast<T*>(&storage_); }
  const T& value() const { return *reinterpret_cast<const T*>(&storage_); }

  T& operator*() { return value(); }
  const T& operator*() const { return value(); }

  T* operator->() { return reinterpret_cast<T*>(&storage_); }
  const T* operator->() const { return reinterpret_cast<const T*>(&storage_); }

  error_t error() const { return err_; }

 private:
  alignas(T) unsigned char storage_[sizeof(T)];
  error_t err_{};
  bool ok_;
};

/**
 * @brief lightweight expected specialization for void.
 */
template<>
class expected<void> {
 public:
  expected() : err_{}, ok_(true) {}

  expected(unexpected e) : err_(e.err), ok_(false) {}

  explicit operator bool() const { return ok_; }

  bool has_value() const { return ok_; }

  error_t error() const { return err_; }

 private:
  error_t err_;
  bool ok_;
};

} // namespace cornet

#endif //CORNET_EXPECTED_H
