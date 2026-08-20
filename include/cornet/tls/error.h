#ifndef CORNET_TLS_ERROR_H
#define CORNET_TLS_ERROR_H

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"

namespace cornet::tls {

/**
 * @brief error codes of the tls module, carried in error_domain::Tls.
 *
 * Deliberately semantic rather than a copy of OpenSSL's error queue: the queue
 * is drained and logged at the site where the failure happens (with
 * ERR_error_string_n), and what travels through expected<> is what a caller
 * can act on.
 */
enum class tls_error_t : int {
  Ok = 0,
  // CORNET_WITH_TLS was off at build time; the code path exists but is inert
  Disabled = 1,
  // SSL_CTX / SSL could not be created, or a certificate/key failed to load
  Init = 2,
  // the handshake did not complete (alert, protocol violation, plain garbage
  // on the wire). verify failures are reported separately below
  Handshake = 3,
  // peer certificate verification failed
  VerifyFailed = 4,
  // the underlying transport went away without a close_notify
  UnexpectedEof = 5,
  // a protocol violation outside the handshake (bad record, failed MAC, ...)
  Protocol = 6,
};

CORNET_NODISCARD inline unexpected tls_unexpected(tls_error_t e) {
  return unexpected(static_cast<int>(e), error_domain::Tls);
}

/**
 * @brief code table for error_domain::Tls; registered with the core renderer
 * slot when this translation unit is loaded.
 */
const char* tls_error_name(int code);

} // namespace cornet::tls

#endif // CORNET_TLS_ERROR_H
