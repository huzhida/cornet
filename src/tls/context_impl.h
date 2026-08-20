#ifndef CORNET_TLS_DETAIL_CONTEXT_IMPL_H
#define CORNET_TLS_DETAIL_CONTEXT_IMPL_H

/**
 * @brief shared definition of tls_context_t's pimpl.
 *
 * Lives in a module-internal header so that engine.cc can build an SSL object
 * from a context, while public headers keep OpenSSL out of sight — same pact
 * as the http module's opaque parser state.
 */

#include "cornet/tls/context.h"

#include <string>

#ifdef CORNET_WITH_TLS
#include <openssl/ssl.h>
#endif

namespace cornet::tls {

struct tls_context_t::impl {
#ifdef CORNET_WITH_TLS
  ~impl() {
    if (ctx) SSL_CTX_free(ctx);
  }
  SSL_CTX* ctx{nullptr};
#endif
  bool server_side{false};
  // alpn_select_cb receives a void*; the list must live as long as the impl
  std::string alpn;
};

} // namespace cornet::tls

#endif // CORNET_TLS_DETAIL_CONTEXT_IMPL_H
