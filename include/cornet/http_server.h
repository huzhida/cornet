#ifndef CORNET_HTTP_SERVER_UMBRELLA_H
#define CORNET_HTTP_SERVER_UMBRELLA_H

/**
 * @file http_server.h
 * @brief server-only umbrella: the common layer plus everything under http/server.
 *
 * Use this instead of cornet/http.h in a translation unit that only serves
 * requests, so a future client-side change cannot force it to recompile.
 */

#include "cornet/http/common/buffer.h"
#include "cornet/http/common/headers.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/common/protocol.h"
#include "cornet/http/common/serializer.h"
#include "cornet/http/common/timer_wheel.h"
#include "cornet/http/common/trace.h"
#include "cornet/http/common/url.h"

#include "cornet/http/server/connection.h"
#include "cornet/http/server/message.h"
#include "cornet/http/server/router.h"
#include "cornet/http/server/server.h"

#endif // CORNET_HTTP_SERVER_UMBRELLA_H
