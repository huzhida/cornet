#ifndef CORNET_HTTP_H
#define CORNET_HTTP_H

/**
 * @file http.h
 * @brief aggregate header for the HTTP/1.1 module.
 *
 * Kept out of cornet.h on purpose: the core promises to depend on nothing but
 * liburing, and llhttp is linked privately into this module. Including this header
 * does not pull llhttp into a translation unit either — the parser keeps it behind
 * an opaque state array.
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

#include "cornet/http/buffer.h"
#include "cornet/http/common.h"
#include "cornet/http/connection.h"
#include "cornet/http/headers.h"
#include "cornet/http/message.h"
#include "cornet/http/parser.h"
#include "cornet/http/router.h"
#include "cornet/http/serializer.h"
#include "cornet/http/server.h"
#include "cornet/http/timer_wheel.h"
#include "cornet/http/trace.h"

#endif // CORNET_HTTP_H
