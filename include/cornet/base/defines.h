#ifndef CORNET_DEFINES_H
#define CORNET_DEFINES_H

#include <chrono>
#include <linux/time_types.h>

// CPU cache line size for alignment purposes
#define CORNET_CACHE_LINE 64

// Linux kernel version check (5.19 = 393779) — IORING_ASYNC_CANCEL_ANY support
#include <linux/version.h>
#ifndef KERNEL_VERSION
#define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + ((c) > 255 ? 255 : (c)))
#endif
#define CORNET_LINUX_VERSION_GE_5_19 0

// suppress unused warnings
#define CORNET_MAYBE_UNUSED [[maybe_unused]]
// enforce callers to check return value
#define CORNET_NODISCARD [[nodiscard]]

/**
 * @brief debug-only invariant check.
 *
 * For contracts that are cheap to state but would cost real work to verify on
 * every call in a release build — the kind that quietly rot as code grows around
 * them. Compiles to nothing when NDEBUG is set.
 */
#ifndef NDEBUG
#include <cstdio>
#include <cstdlib>
#define CORNET_ASSERT(cond, msg)                                              \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::fprintf(stderr, "cornet: assertion failed at %s:%d: %s (%s)\n",    \
                   __FILE__, __LINE__, #cond, (msg));                         \
      std::abort();                                                           \
    }                                                                         \
  } while (0)
#else
#define CORNET_ASSERT(cond, msg) ((void)0)
#endif

/**
 * @brief convert std::chrono duration to __kernel_timespec for io_uring.
 * @tparam Rep ratio type of duration
 * @tparam Period period type of duration
 * @param t duration to convert
 * @return __kernel_timespec suitable for io_uring timeout operations
 */
template<typename Rep, typename Period>
inline __kernel_timespec to_kernel_timespec(std::chrono::duration<Rep, Period> t) {
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(t).count();
  auto nano_secs = std::chrono::duration_cast<std::chrono::nanoseconds>(t).count();
  __kernel_timespec ts{};
  ts.tv_sec = secs;
  ts.tv_nsec = nano_secs - secs * 1'000'000'000;
  return ts;
}

#endif //CORNET_DEFINES_H
