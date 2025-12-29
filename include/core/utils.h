#ifndef CORNET_UTILS_H
#define CORNET_UTILS_H

#include <spdlog/spdlog.h>

#define CORNET_UNIX_CHECK(expr, ...) \
  if ((expr) < 0) {             \
    SPDLOG_ERROR("Call {} error: {}", #expr, strerror(errno)); \
    __VA_ARGS__;                                      \
  }
#endif
