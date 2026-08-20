#include "cornet/tls/engine.h"

#include "cornet/tls/context.h"

#include "context_impl.h"

#include <cerrno>

#include <spdlog/spdlog.h>

#ifdef CORNET_WITH_TLS

#include <openssl/err.h>
#include <openssl/ssl.h>

#endif

namespace cornet::tls {

struct tls_engine_t::impl {
#ifdef CORNET_WITH_TLS
  ~impl() {
    // The BIOs belong to the SSL once SSL_set_bio() took them.
    if (ssl) SSL_free(ssl);
  }
  SSL* ssl{nullptr};
  BIO* rbio{nullptr};
  BIO* wbio{nullptr};
#endif
  bool server_side{false};
};

namespace {

#ifdef CORNET_WITH_TLS

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

#endif

} // namespace

tls_engine_t::tls_engine_t() = default;
tls_engine_t::~tls_engine_t() = default;
tls_engine_t::tls_engine_t(tls_engine_t&&) noexcept = default;
tls_engine_t& tls_engine_t::operator=(tls_engine_t&&) noexcept = default;

expected<tls_engine_t> tls_engine_t::create(const tls_context_t& ctx, engine_mode_t mode,
                                            std::string_view server_name) {
#ifdef CORNET_WITH_TLS
  if (!ctx.valid()) return tls_unexpected(tls_error_t::Init);

  tls_engine_t engine;
  engine.impl_ = std::make_unique<impl>();
  SSL* ssl = SSL_new(ctx.impl_->ctx);
  if (!ssl) {
    drain_ssl_queue("SSL_new");
    return tls_unexpected(tls_error_t::Init);
  }
  BIO* rbio = BIO_new(BIO_s_mem());
  BIO* wbio = BIO_new(BIO_s_mem());
  if (!rbio || !wbio) {
    if (rbio) BIO_free(rbio);
    if (wbio) BIO_free(wbio);
    SSL_free(ssl);
    drain_ssl_queue("BIO_new");
    return tls_unexpected(tls_error_t::Init);
  }
  SSL_set_bio(ssl, rbio, wbio);  // ownership passes to ssl
  engine.impl_->ssl = ssl;
  engine.impl_->rbio = rbio;
  engine.impl_->wbio = wbio;
  engine.impl_->server_side = (mode == engine_mode_t::Server);

  if (mode == engine_mode_t::Server) {
    SSL_set_accept_state(ssl);
  } else {
    SSL_set_connect_state(ssl);
    if (!server_name.empty()) {
      // SNI, and hostname verification against the certificate's SAN/CN.
      // SSL_set1_host enables the check in OpenSSL's X509_verify_cert path.
      if (SSL_set_tlsext_host_name(ssl, std::string(server_name).c_str()) != 1 ||
          SSL_set1_host(ssl, std::string(server_name).c_str()) != 1) {
        drain_ssl_queue("set sni/host");
        return tls_unexpected(tls_error_t::Init);
      }
    }
  }
  return engine;
#else
  (void)ctx;
  (void)mode;
  (void)server_name;
  return tls_unexpected(tls_error_t::Disabled);
#endif
}

engine_step_t tls_engine_t::classify([[maybe_unused]] int ret,
                                     [[maybe_unused]] bool in_handshake) {
#ifdef CORNET_WITH_TLS
  int e = SSL_get_error(impl_->ssl, ret);
  switch (e) {
    case SSL_ERROR_NONE:
      return engine_step_t::Done;
    case SSL_ERROR_WANT_READ:
      return engine_step_t::WantRead;
    case SSL_ERROR_WANT_WRITE:
      return engine_step_t::WantWrite;
    case SSL_ERROR_ZERO_RETURN:
      return engine_step_t::Closed;
    case SSL_ERROR_SYSCALL:
      // ret==0 with no errno is an unclean transport EOF: the TCP stream ended
      // with no close_notify, which HTTP/1.1 has to know about (a truncated
      // response reads exactly the same as a complete one otherwise).
      if (ret == 0) {
        err_ = error_t{static_cast<int>(tls_error_t::UnexpectedEof), error_domain::Tls};
      } else {
        err_ = error_t{errno, error_domain::System};
      }
      drain_ssl_queue("syscall error");
      return engine_step_t::Failed;
    default: {
      // SSL_ERROR_SSL and friends: verification failures get their own code so
      // a client can tell "bad certificate" apart from "garbage on the wire".
      unsigned long q = ERR_peek_error();
      unsigned int reason = ERR_GET_REASON(q);
      if (reason == SSL_R_CERTIFICATE_VERIFY_FAILED) {
        err_ = error_t{static_cast<int>(tls_error_t::VerifyFailed), error_domain::Tls};
      } else {
        auto code = in_handshake ? tls_error_t::Handshake : tls_error_t::Protocol;
        err_ = error_t{static_cast<int>(code), error_domain::Tls};
      }
      drain_ssl_queue("tls error");
      return engine_step_t::Failed;
    }
  }
#else
  err_ = error_t{static_cast<int>(tls_error_t::Disabled), error_domain::Tls};
  return engine_step_t::Failed;
#endif
}

engine_step_t tls_engine_t::handshake() {
#ifdef CORNET_WITH_TLS
  if (!handshaken_) {
    ERR_clear_error();
    int ret = SSL_do_handshake(impl_->ssl);
    if (ret == 1) {
      handshaken_ = true;
      if (!impl_->server_side && SSL_get_verify_result(impl_->ssl) != X509_V_OK &&
          SSL_get_verify_mode(impl_->ssl) != SSL_VERIFY_NONE) {
        long v = SSL_get_verify_result(impl_->ssl);
        SPDLOG_DEBUG("tls: certificate verification failed: {}",
                     X509_verify_cert_error_string(v));
        err_ = error_t{static_cast<int>(tls_error_t::VerifyFailed), error_domain::Tls};
        return engine_step_t::Failed;
      }
      return engine_step_t::Done;
    }
    return classify(ret, true);
  }
  return engine_step_t::Done;
#else
  return classify(0, true);
#endif
}

engine_step_t tls_engine_t::read(void* buf, size_t len, size_t& got) {
#ifdef CORNET_WITH_TLS
  got = 0;
  ERR_clear_error();
  int ret = SSL_read(impl_->ssl, buf, static_cast<int>(len));
  if (ret > 0) {
    got = static_cast<size_t>(ret);
    return engine_step_t::Done;
  }
  return classify(ret, false);
#else
  (void)buf;
  (void)len;
  (void)got;
  return classify(0, false);
#endif
}

engine_step_t tls_engine_t::write(const void* buf, size_t len) {
#ifdef CORNET_WITH_TLS
  if (len > kRecordPayload) len = kRecordPayload;
  ERR_clear_error();
  int ret = SSL_write(impl_->ssl, buf, static_cast<int>(len));
  if (ret > 0) return engine_step_t::Done;
  return classify(ret, false);
#else
  (void)buf;
  (void)len;
  return classify(0, false);
#endif
}

engine_step_t tls_engine_t::shutdown() {
#ifdef CORNET_WITH_TLS
  ERR_clear_error();
  int ret = SSL_shutdown(impl_->ssl);
  // ret==0: close_notify queued, the peer's has not been seen. Phase one is all
  // the caller asked for; the peer's notify arrives through read() -> Closed.
  if (ret >= 0) return engine_step_t::Done;
  return classify(ret, false);
#else
  return classify(0, false);
#endif
}

size_t tls_engine_t::output_pending() const {
#ifdef CORNET_WITH_TLS
  return BIO_ctrl_pending(impl_->wbio);
#else
  return 0;
#endif
}

size_t tls_engine_t::take_output(void* buf, size_t len) {
#ifdef CORNET_WITH_TLS
  int ret = BIO_read(impl_->wbio, buf, static_cast<int>(len));
  return ret > 0 ? static_cast<size_t>(ret) : 0;
#else
  (void)buf;
  (void)len;
  return 0;
#endif
}

expected<void> tls_engine_t::feed_input(const void* buf, size_t len) {
#ifdef CORNET_WITH_TLS
  int ret = BIO_write(impl_->rbio, buf, static_cast<int>(len));
  if (ret != static_cast<int>(len)) {
    drain_ssl_queue("BIO_write");
    return tls_unexpected(tls_error_t::Protocol);
  }
  return {};
#else
  (void)buf;
  (void)len;
  return tls_unexpected(tls_error_t::Disabled);
#endif
}

bool tls_engine_t::valid_for_io() const {
#ifdef CORNET_WITH_TLS
  return impl_ && impl_->ssl != nullptr;
#else
  return false;
#endif
}

std::string_view tls_engine_t::version() const {
#ifdef CORNET_WITH_TLS
  if (!impl_ || !impl_->ssl) return {};
  return SSL_get_version(impl_->ssl);
#else
  return {};
#endif
}

std::string_view tls_engine_t::cipher() const {
#ifdef CORNET_WITH_TLS
  if (!impl_ || !impl_->ssl || !handshaken_) return {};
  return SSL_get_cipher_name(impl_->ssl);
#else
  return {};
#endif
}

} // namespace cornet::tls
