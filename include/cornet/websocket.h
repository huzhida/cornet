#ifndef CORNET_WEBSOCKET_H
#define CORNET_WEBSOCKET_H

/**
 * @file websocket.h
 * @brief aggregate header for the WebSocket (RFC 6455) module.
 *
 * Server endpoints are registered on the http router — a websocket route is
 * an http route that answers its upgrade request with a 101 and then speaks
 * frames instead of HTTP:
 * @code
 *   server.websocket("/echo", [](websocket::session_t& ws) -> coro_t<void> {
 *     while (auto msg = co_await ws.recv()) {
 *       if (msg->opcode == websocket::opcode_t::Close) break;
 *       if (auto ok = co_await ws.send(msg->payload, msg->opcode); !ok) break;
 *     }
 *   });
 * @endcode
 *
 * The client is one call:
 * @code
 *   auto ws = co_await websocket::connect(ctx, "ws://localhost:8080/echo");
 * @endcode
 */

#include "cornet/websocket/common/frame.h"
#include "cornet/websocket/common/handshake.h"
#include "cornet/websocket/common/protocol.h"

#include "cornet/websocket/session.h"
#include "cornet/websocket/server.h"

#include "cornet/websocket/client.h"

#endif // CORNET_WEBSOCKET_H
