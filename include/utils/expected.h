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
 * Zero-overhead: no exceptions, no heap allocation, trivially copyable for trivial T.
 * @tparam T value type on success
 */
template<typename T>
class expected {
 public:
  expected(T val) : val_(std::move(val)), err_{}, ok_(true) {}

  expected(unexpected e) : val_{}, err_(e.err), ok_(false) {}

  explicit operator bool() const { return ok_; }

  bool has_value() const { return ok_; }

  T& value() { return val_; }
  const T& value() const { return val_; }

  T& operator*() { return val_; }
  const T& operator*() const { return val_; }

  T* operator->() { return &val_; }
  const T* operator->() const { return &val_; }

  error_t error() const { return err_; }

 private:
  T val_;
  error_t err_;
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
