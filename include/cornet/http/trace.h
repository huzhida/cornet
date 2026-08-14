#ifndef CORNET_HTTP_TRACE_H
#define CORNET_HTTP_TRACE_H

#include <spdlog/spdlog.h>

/**
 * @file trace.h
 * @brief opt-in phase tracing for the HTTP module.
 *
 * Debugging a request that never gets answered means finding which phase it
 * stopped in — accept, recv, parse, route, handler, frame, write. SPDLOG_DEBUG is
 * not usable for that by default: spdlog strips anything below SPDLOG_ACTIVE_LEVEL
 * at compile time, and that level is INFO unless the whole project is rebuilt.
 *
 * So tracing gets its own switch and logs at INFO, which is visible without
 * touching either the compile-time or the runtime level:
 *
 *   cmake --preset debug -DCORNET_HTTP_TRACE=ON
 *   cmake --build --preset debug --target hello-cornet
 *
 * Off, it compiles to nothing — the arguments are not even evaluated.
 */
#ifdef CORNET_HTTP_TRACE
#define CORNET_HTTP_TRACE_LOG(...) SPDLOG_INFO("[http] " __VA_ARGS__)
#else
#define CORNET_HTTP_TRACE_LOG(...) ((void)0)
#endif

#endif // CORNET_HTTP_TRACE_H
