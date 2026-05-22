#ifndef CORNET_UTILS_H
#define CORNET_UTILS_H

#include "defines.h"
#include "config.h"
#include "logging.h"
#include "expected.h"

#include <linux/time_types.h>
template<typename Rep, typename Period>
inline __kernel_timespec to_kernel_timespec(std::chrono::duration<Rep, Period> t) {
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(t).count();
  auto nano_secs= std::chrono::duration_cast<std::chrono::nanoseconds>(t).count();
  __kernel_timespec ts{};
  ts.tv_sec = secs;
  ts.tv_nsec = nano_secs - secs * 1'000'000'000;
  return ts;
}

#define CORNET_FATAL(expr, ...) do { \
  SPDLOG_ERROR(expr, ##__VA_ARGS__);                      \
  throw std::runtime_error(fmt::format(expr, ##__VA_ARGS__)); \
} while(0)

#define CORNET_ASSERT(expr1, expr2) do { \
  auto _1 = expr1;                       \
  auto _2 = expr2;\
  if (_1 != _2) {                      \
    CORNET_FATAL("assert {}(actual:{}) == {}(actual:{})", #expr1, _1, #expr2, _2);      \
  }                                   \
} while(0)

#define CORNET_ASSERT_NOT(expr1, expr2) do { \
  auto _1 = expr1;                       \
  auto _2 = expr2;\
  if (_1 == _2) {                      \
    CORNET_FATAL("assert {}(actual:{}) != {}(actual:{})", #expr1, _1, #expr2, _2);      \
  }                                   \
} while(0)

#endif