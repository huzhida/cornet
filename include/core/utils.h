#ifndef CORNET_UTILS_H
#define CORNET_UTILS_H

#include <spdlog/spdlog.h>

#define CORNET_UNIX_CHECK(expr, ...) \
  do {                               \
    int ret = expr;                    \
    if (ret < 0) {                     \
      SPDLOG_ERROR("Unix system call {} return_code:{} error: {}", #expr, ret, strerror(errno)); \
      __VA_ARGS__                                                \
    }                              \
  }while(0)

#endif
