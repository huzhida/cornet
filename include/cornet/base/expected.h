#ifndef CORNET_EXPECTED_H
#define CORNET_EXPECTED_H

#include <cstring>
#include <netdb.h>
#include <type_traits>
#include <utility>

namespace cornet {

/**
 * @brief error domain, distinguishes different error code namespaces
 */
enum class error_domain : uint8_t {
  None,       // no error
  System,     // errno (POSIX system call errors)
  Resolve,    // EAI_* (getaddrinfo errors)
  Internal,   // framework internal errors
  Exception,  // unexpected exception thrown from coroutine
  Http,       // HTTP protocol errors (llhttp errno), rendered via the resolver below
  Tls,        // tls module errors (tls_error_t), rendered via the resolver below
  Websocket,  // websocket module errors (websocket_error_t), rendered via the resolver below
};

/**
 * @brief renderer for a domain whose code table lives outside the core.
 *
 * The core links only against liburing, so it cannot call llhttp_errno_name()
 * itself. A module that owns such a domain registers a renderer at static-init
 * time; until then message() falls back to a generic string — no UB, only a
 * less specific message for errors raised very early in startup.
 */
using domain_message_fn = const char* (*)(int);

/**
 * @brief renderer slot for error_domain::Http.
 * Function-local static, so any translation unit can reach the slot without an
 * ordering dependency on some global object's construction.
 */
inline domain_message_fn& http_message_resolver() {
  static domain_message_fn fn = nullptr;
  return fn;
}

/**
 * @brief renderer slot for error_domain::Tls. Same pattern as the http one:
 * the core never sees OpenSSL, so the tls module registers its code table.
 */
inline domain_message_fn& tls_message_resolver() {
  static domain_message_fn fn = nullptr;
  return fn;
}

/**
 * @brief renderer slot for error_domain::Websocket. Same pattern again:
 * the websocket module owns its code table and registers it at load time.
 */
inline domain_message_fn& websocket_message_resolver() {
  static domain_message_fn fn = nullptr;
  return fn;
}

/**
 * @brief unified error type supporting multiple error domains
 */
struct error_t {
  int code{0};
  error_domain domain{error_domain::None};

  explicit operator bool() const { return domain != error_domain::None; }

  /**
   * @brief get human-readable error message based on domain
   */
  const char* message() const {
    switch (domain) {
      case error_domain::System: return strerror(code);
      case error_domain::Resolve: return gai_strerror(code);
      case error_domain::Internal: return "internal error";
      case error_domain::Exception: return "unexpected exception in coroutine";
      case error_domain::Http: {
        auto fn = http_message_resolver();
        return fn ? fn(code) : "http protocol error";
      }
      case error_domain::Tls: {
        auto fn = tls_message_resolver();
        return fn ? fn(code) : "tls error";
      }
      case error_domain::Websocket: {
        auto fn = websocket_message_resolver();
        return fn ? fn(code) : "websocket error";
      }
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

  unexpected(int code, error_domain domain = error_domain::System)
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

  expected(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
    : err_(other.err_), ok_(other.ok_) {
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

  expected& operator=(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T>) {
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

namespace detail {

template<typename T> struct is_expected : std::false_type {};
template<typename T> struct is_expected<expected<T>> : std::true_type {};
template<typename T>
inline constexpr bool is_expected_v = is_expected<T>::value;

/**
 * @brief result slot type used by the when_* combinators and task_scope.
 *
 * error_t is one global error type, so a nested expected carries no extra
 * information: a coroutine that already returns expected<U> collapses into
 * expected<U> instead of expected<expected<U>>. Framework-level errors
 * (exceptions caught by the combinator) land in the same slot and stay
 * distinguishable from coroutine-level errors via
 * error.domain == error_domain::Exception.
 */
template<typename T> struct result_slot { using type = expected<T>; };
template<typename U> struct result_slot<expected<U>> { using type = expected<U>; };
template<typename T> using result_slot_t = typename result_slot<T>::type;

} // namespace detail

} // namespace cornet

#endif //CORNET_EXPECTED_H
