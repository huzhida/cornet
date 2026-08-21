#ifndef CORNET_HTTP_H
#define CORNET_HTTP_H

/**
 * @file http.h
 * @brief aggregate header for the HTTP/1.1 module — common layer, server and client.
 *
 * Kept out of cornet.h on purpose: the core promises to depend on nothing but
 * liburing, and llhttp is linked privately into this module. Including this header
 * does not pull llhttp into a translation unit either — the parser keeps it behind
 * an opaque state array.
 *
 * The module is laid out in three directories. common/ holds what both directions
 * need (protocol constants, buffers, header table, parser, serializer, timer
 * wheel); server/ and client/ hold what only one side does. Include
 * cornet/http_server.h or cornet/http_client.h to get just one of them.
 *
 * Minimal server:
 * @code
 *   context_t ctx;
 *   http::server_t server(ctx);
 *   server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });
 *   (void)server.listen("0.0.0.0", 8080);
 *   ctx.spawn(server.serve());
 *   ctx.run();
 * @endcode
 */

#include "cornet/http/common/buffer.h"
#include "cornet/http/common/headers.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/serializer.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/http/common/trace.h"
#include "cornet/http/common/url.h"

#include "cornet/http/server/connection.h"
#include "cornet/http/server/message.h"
#include "cornet/http/server/router.h"
#include "cornet/http/server/server.h"

#include "cornet/http/client/client.h"
#include "cornet/http/client/connection.h"
#include "cornet/http/client/message.h"
#include "cornet/http/client/pool.h"

#include "cornet/websocket.h"

#endif // CORNET_HTTP_H
