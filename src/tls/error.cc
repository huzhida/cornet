#include "cornet/tls/error.h"

namespace cornet::tls {

const char* tls_error_name(int code) {
  switch (static_cast<tls_error_t>(code)) {
    case tls_error_t::Ok: return "no error";
    case tls_error_t::Disabled: return "tls disabled at build time (CORNET_WITH_TLS=OFF)";
    case tls_error_t::Init: return "tls initialisation failed";
    case tls_error_t::Handshake: return "tls handshake failed";
    case tls_error_t::VerifyFailed: return "peer certificate verification failed";
    case tls_error_t::UnexpectedEof: return "transport closed without close_notify";
    case tls_error_t::Protocol: return "tls protocol violation";
  }
  return "unknown tls error";
}

namespace {

/**
 * @brief register the TLS renderer with the core at load time.
 *
 * Same pattern as the http module: the core never links OpenSSL, so
 * error_t::message() consults the slot this initializer fills.
 */
const bool kResolverRegistered = [] {
  tls_message_resolver() = &tls_error_name;
  return true;
}();

} // namespace

} // namespace cornet::tls
