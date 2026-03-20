#ifndef CORNET_UTILS_H
#define CORNET_UTILS_H

#include "defines.h"
#include "config.h"
#include "logging.h"

#include <linux/time_types.h>
template<typename Rep, typename Period>
inline __kernel_timespec to_kernel_timespec(std::chrono::duration<Rep, Period> t) {
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(t).count();
  auto nano_secs= std::chrono::duration_cast<std::chrono::nanoseconds>(t).count();
  __kernel_timespec ts{};
  ts.tv_sec = secs;
  ts.tv_nsec = nano_secs;
  return ts;
}

#endif