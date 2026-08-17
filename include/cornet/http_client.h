#ifndef CORNET_HTTP_CLIENT_UMBRELLA_H
#define CORNET_HTTP_CLIENT_UMBRELLA_H

/**
 * @file http_client.h
 * @brief client-only umbrella: the common layer plus everything under http/client.
 *
 * Use this instead of cornet/http.h in a translation unit that only makes requests, so
 * a change on the server side cannot force it to recompile.
 *
 * @code
 *   context_t ctx;
 *   http::client_t cli(ctx);
 *   ctx.spawn([&]() -> coro_t<void> {
 *     auto resp = co_await cli.get("http://127.0.0.1:8080/hello");
 *     if (!resp) {
 *       SPDLOG_ERROR("request failed: {}", resp.error().message());
 *       co_return;
 *     }
 *     SPDLOG_INFO("{} {}", resp->status_code(), resp->body());
 *   }());
 *   ctx.run();
 * @endcode
 */

#include "cornet/http/common/buffer.h"
#include "cornet/http/common/headers.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/serializer.h"
#include "cornet/http/common/timer_wheel.h"
#include "cornet/http/common/trace.h"
#include "cornet/http/common/url.h"

#include "cornet/http/client/client.h"
#include "cornet/http/client/connection.h"
#include "cornet/http/client/message.h"
#include "cornet/http/client/pool.h"

#endif // CORNET_HTTP_CLIENT_UMBRELLA_H
