#include "cornet/tls/context.h"

#include "context_impl.h"

#include <spdlog/spdlog.h>

#ifdef CORNET_WITH_TLS

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>

#endif

namespace cornet::tls {

#ifdef CORNET_WITH_TLS

namespace {

/**
 * @brief drain OpenSSL's per-thread error queue into the log.
 *
 * Every failure point calls this before returning its semantic error code:
 * the queue is thread-local garbage if left behind, and the strings are the
 * difference between "Handshake" and "wrong curve".
 */
void drain_ssl_queue(const char* where) {
  char buf[256];
  unsigned long e;
  bool first = true;
  while ((e = ERR_get_error()) != 0) {
    ERR_error_string_n(e, buf, sizeof(buf));
    if (first) {
      SPDLOG_DEBUG("tls: {}: {}", where, buf);
      first = false;
    } else {
      SPDLOG_DEBUG("tls: {} (cont): {}", where, buf);
    }
  }
}

// ALPN wires carry length-prefixed strings, not NUL-terminated ones.
std::string alpn_wire(std::string_view alpn) {
  std::string out;
  out += static_cast<char>(alpn.size());
  out += alpn;
  return out;
}

int alpn_select_cb(SSL* /*ssl*/, const unsigned char** out, unsigned char* outlen,
                   const unsigned char* in, unsigned int inlen, void* arg) {
  auto* list = static_cast<const std::string*>(arg);
  // The server list has one entry in practice ("http/1.1"); accept only if the
  // client offered it too.
  unsigned int i = 0;
  while (i + 1 <= inlen) {
    unsigned int len = in[i];
    if (i + 1 + len > inlen) break;
    std::string_view offered(reinterpret_cast<const char*>(in + i + 1), len);
    if (offered == *list) {
      *out = in + i + 1;
      *outlen = static_cast<unsigned char>(len);
      return SSL_TLSEXT_ERR_OK;
    }
    i += 1 + len;
  }
  return SSL_TLSEXT_ERR_ALERT_FATAL;
}

expected<void> load_cert_chain(SSL_CTX* ctx, const tls_server_options_t& opt) {
  if (!opt.cert_file.empty()) {
    // chain_file has no filetype argument: PEM is all it speaks
    if (SSL_CTX_use_certificate_chain_file(ctx, opt.cert_file.c_str()) != 1) {
      return tls_unexpected(tls_error_t::Init);
    }
  } else if (!opt.cert_pem.empty()) {
    // Memory form: one certificate, no chain. See tls_server_options_t for why.
    BIO* bio = BIO_new_mem_buf(opt.cert_pem.data(), static_cast<int>(opt.cert_pem.size()));
    if (!bio) return tls_unexpected(tls_error_t::Init);
    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!cert) return tls_unexpected(tls_error_t::Init);
    int ok = SSL_CTX_use_certificate(ctx, cert);
    X509_free(cert);
    if (ok != 1) return tls_unexpected(tls_error_t::Init);
  } else {
    return tls_unexpected(tls_error_t::Init);
  }
  return {};
}

expected<void> load_key(SSL_CTX* ctx, const tls_server_options_t& opt) {
  if (!opt.key_file.empty()) {
    if (SSL_CTX_use_PrivateKey_file(ctx, opt.key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
      return tls_unexpected(tls_error_t::Init);
    }
  } else if (!opt.key_pem.empty()) {
    BIO* bio = BIO_new_mem_buf(opt.key_pem.data(), static_cast<int>(opt.key_pem.size()));
    if (!bio) return tls_unexpected(tls_error_t::Init);
    EVP_PKEY* key = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!key) return tls_unexpected(tls_error_t::Init);
    int ok = SSL_CTX_use_PrivateKey(ctx, key);
    EVP_PKEY_free(key);
    if (ok != 1) return tls_unexpected(tls_error_t::Init);
  } else {
    return tls_unexpected(tls_error_t::Init);
  }
  if (SSL_CTX_check_private_key(ctx) != 1) return tls_unexpected(tls_error_t::Init);
  return {};
}

expected<void> load_ca_into_store(X509_STORE* store, std::string_view pem) {
  BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  if (!bio) return tls_unexpected(tls_error_t::Init);
  uint32_t loaded = 0;
  for (;;) {
    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (!cert) break;  // PEM end sets a soft error; the queue is drained below
    if (X509_STORE_add_cert(store, cert) != 1) {
      X509_free(cert);
      BIO_free(bio);
      return tls_unexpected(tls_error_t::Init);
    }
    X509_free(cert);
    ++loaded;
  }
  BIO_free(bio);
  if (loaded == 0) return tls_unexpected(tls_error_t::Init);
  return {};
}

} // namespace

#endif // CORNET_WITH_TLS


tls_context_t::tls_context_t() = default;
tls_context_t::~tls_context_t() = default;
tls_context_t::tls_context_t(tls_context_t&&) noexcept = default;
tls_context_t& tls_context_t::operator=(tls_context_t&&) noexcept = default;

expected<std::shared_ptr<tls_context_t>> tls_context_t::make_server(
    const tls_server_options_t& opt) {
#ifdef CORNET_WITH_TLS
  auto holder = std::make_shared<tls_context_t>();
  holder->impl_ = std::make_unique<impl>();
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
  if (!ctx) {
    drain_ssl_queue("SSL_CTX_new(server)");
    return tls_unexpected(tls_error_t::Init);
  }
  holder->impl_->ctx = ctx;
  holder->impl_->server_side = true;
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  if (auto ok = load_cert_chain(ctx, opt); !ok) {
    drain_ssl_queue("load certificate");
    return tls_unexpected(tls_error_t::Init);
  }
  if (auto ok = load_key(ctx, opt); !ok) {
    drain_ssl_queue("load private key");
    return tls_unexpected(tls_error_t::Init);
  }

  if (opt.require_client_cert) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    if (!opt.ca_file.empty() || !opt.ca_dir.empty()) {
      if (SSL_CTX_load_verify_locations(ctx,
                                        opt.ca_file.empty() ? nullptr : opt.ca_file.c_str(),
                                        opt.ca_dir.empty() ? nullptr : opt.ca_dir.c_str()) != 1) {
        drain_ssl_queue("load client-ca");
        return tls_unexpected(tls_error_t::Init);
      }
    } else {
      SSL_CTX_set_default_verify_paths(ctx);
    }
  }

  if (!opt.alpn.empty()) {
    holder->impl_->alpn = opt.alpn;
    SSL_CTX_set_alpn_select_cb(ctx, alpn_select_cb, &holder->impl_->alpn);
  }
  if (opt.num_tickets >= 0) {
    SSL_CTX_set_num_tickets(ctx, size_t(opt.num_tickets));
  }
  return holder;
#else
  (void)opt;
  return tls_unexpected(tls_error_t::Disabled);
#endif
}

expected<std::shared_ptr<tls_context_t>> tls_context_t::make_client(
    const tls_client_options_t& opt) {
#ifdef CORNET_WITH_TLS
  auto holder = std::make_shared<tls_context_t>();
  holder->impl_ = std::make_unique<impl>();
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) {
    drain_ssl_queue("SSL_CTX_new(client)");
    return tls_unexpected(tls_error_t::Init);
  }
  holder->impl_->ctx = ctx;
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  if (opt.verify_peer) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (!opt.ca_pem.empty()) {
      if (auto ok = load_ca_into_store(SSL_CTX_get_cert_store(ctx), opt.ca_pem); !ok) {
        drain_ssl_queue("load ca from memory");
        return tls_unexpected(tls_error_t::Init);
      }
    } else if (!opt.ca_file.empty() || !opt.ca_dir.empty()) {
      if (SSL_CTX_load_verify_locations(ctx,
                                        opt.ca_file.empty() ? nullptr : opt.ca_file.c_str(),
                                        opt.ca_dir.empty() ? nullptr : opt.ca_dir.c_str()) != 1) {
        drain_ssl_queue("load ca");
        return tls_unexpected(tls_error_t::Init);
      }
    } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
      drain_ssl_queue("default verify paths");
      return tls_unexpected(tls_error_t::Init);
    }
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  }

  if (!opt.alpn.empty()) {
    auto wire = alpn_wire(opt.alpn);
    if (SSL_CTX_set_alpn_protos(ctx, reinterpret_cast<const unsigned char*>(wire.data()),
                                static_cast<unsigned int>(wire.size())) != 0) {
      drain_ssl_queue("set alpn");
      return tls_unexpected(tls_error_t::Init);
    }
  }
  return holder;
#else
  (void)opt;
  return tls_unexpected(tls_error_t::Disabled);
#endif
}

bool tls_context_t::valid() const {
#ifdef CORNET_WITH_TLS
  return impl_ && impl_->ctx != nullptr;
#else
  return false;
#endif
}

} // namespace cornet::tls
