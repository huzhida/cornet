#ifndef CORNET_TLS_CONTEXT_H
#define CORNET_TLS_CONTEXT_H

#include <memory>
#include <string>
#include <string_view>

#include "cornet/base/expected.h"
#include "cornet/tls/error.h"

namespace cornet::tls {

/**
 * @brief server-side material for one listening socket family.
 *
 * File paths are the production form; the *_pem fields exist so tests and embedded
 * configs can skip the filesystem. A memory certificate is a single cert, not a
 * chain — chains belong in cert_file.
 */
struct tls_server_options_t {
  std::string cert_file;  // PEM certificate (chain)
  std::string key_file;   // PEM private key
  std::string cert_pem;   // in-memory certificate, used when cert_file is empty
  std::string key_pem;    // in-memory private key, used when key_file is empty
  // ALPN protocol list in order of preference; empty disables ALPN.
  // "http/1.1" is what cornet's http module speaks, whatever is offered.
  std::string alpn{"http/1.1"};
  // request (and require) a client certificate. Verification happens against
  // the same CA list a client context would load; off by default.
  bool require_client_cert{false};
  // TLS 1.3 NewSessionTickets issued after each handshake. Default 0: cornet
  // does not implement session resumption at all (no resumption store, and an
  // idle pooled connection is closed without close_notify anyway), so tickets
  // only pollute the wire — most visibly they trip a pool's raw-fd liveness
  // probe into discarding an otherwise perfectly good connection. Set >0 only
  // if you have a resumption design that also accounts for that probe.
  int num_tickets{0};
  std::string ca_file;
  std::string ca_dir;
};

/**
 * @brief client-side trust and identity material.
 */
struct tls_client_options_t {
  // verify the peer's certificate chain and hostname. Turning this off for
  // anything but a lab is how MITMs are born — the setting exists for tests.
  bool verify_peer{true};
  std::string ca_file;   // empty (with ca_dir): the system default verify paths
  std::string ca_dir;
  std::string ca_pem;    // in-memory CA bundle; wins over file/dir when set
  std::string alpn{"http/1.1"};
};

/**
 * @brief shared TLS configuration (an SSL_CTX, without the type leaking).
 *
 * One context serves any number of connections on this worker; connections
 * up-ref it for their own lifetime. Build once per context_t — the shared-
 * nothing rule applies here too: a tls_context_t is not thread-safe to *use*
 * for handshakes from several threads, so give each worker its own.
 */
class tls_context_t {
 public:
  tls_context_t();
  ~tls_context_t();

  tls_context_t(const tls_context_t&) = delete;
  tls_context_t& operator=(const tls_context_t&) = delete;
  tls_context_t(tls_context_t&&) noexcept;
  tls_context_t& operator=(tls_context_t&&) noexcept;

  CORNET_NODISCARD static expected<std::shared_ptr<tls_context_t>> make_server(
      const tls_server_options_t& opt);
  CORNET_NODISCARD static expected<std::shared_ptr<tls_context_t>> make_client(
      const tls_client_options_t& opt = {});

  CORNET_NODISCARD bool valid() const;
  // TLS 1.2 is the floor; 1.3 is negotiated when the peer offers it.
  CORNET_NODISCARD std::string_view min_version_string() const { return "TLSv1.2"; }

 private:
  friend class tls_engine_t;
  // Opaque so that no public header mentions OpenSSL. Defined in context.cc.
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace cornet::tls

#endif // CORNET_TLS_CONTEXT_H
