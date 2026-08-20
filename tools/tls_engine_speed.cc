/**
 * @file tls_engine_speed.cc
 * @brief pure-CPU TLS round-trip speed through two engines — no sockets, no
 * io_uring. If this is fast, the large_msg collapse cannot be crypto or
 * OpenSSL-call overhead and must be transport-event pacing.
 *
 *   g++ -O2 tools/tls_engine_speed.cc -o /tmp/tls_engine_speed -lssl -lcrypto
 *   /tmp/tls_engine_speed [payload_bytes] [rounds]
 */

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>

#include <openssl/err.h>
#include <openssl/ssl.h>

struct mem_end {
  SSL* ssl = nullptr;
  BIO* rbio = nullptr;
  BIO* wbio = nullptr;
};

static bool make_end(mem_end& e, SSL_CTX* ctx, bool server) {
  e.ssl = SSL_new(ctx);
  if (!e.ssl) return false;
  e.rbio = BIO_new(BIO_s_mem());
  e.wbio = BIO_new(BIO_s_mem());
  if (!e.rbio || !e.wbio) return false;
  SSL_set_bio(e.ssl, e.rbio, e.wbio);
  server ? SSL_set_accept_state(e.ssl) : SSL_set_connect_state(e.ssl);
  return true;
}

static size_t pump(mem_end& from, mem_end& to) {
  static char buf[16u * 1024u];
  size_t moved = 0;
  while (BIO_ctrl_pending(from.wbio) > 0) {
    int n = BIO_read(from.wbio, buf, sizeof(buf));
    if (n <= 0) break;
    BIO_write(to.rbio, buf, n);
    moved += (size_t)n;
  }
  return moved;
}

int main(int argc, char** argv) {
  size_t payload = argc > 1 ? strtoul(argv[1], nullptr, 10) : 65536;
  int rounds = argc > 2 ? atoi(argv[2]) : 200;

  // hermetic self-signed cert
  EVP_PKEY* key = EVP_EC_gen("prime256v1");
  X509* cert = X509_new();
  X509_set_version(cert, X509_VERSION_3);
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), -60);
  X509_gmtime_adj(X509_getm_notAfter(cert), 3600L * 24 * 365 * 10);
  X509_set_pubkey(cert, key);
  X509_NAME* name = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const uint8_t*)"localhost", -1, -1, 0);
  X509_set_issuer_name(cert, name);
  X509_sign(cert, key, EVP_sha256());

  SSL_CTX* sctx = SSL_CTX_new(TLS_server_method());
  SSL_CTX* cctx = SSL_CTX_new(TLS_client_method());
  SSL_CTX_use_certificate(sctx, cert);
  SSL_CTX_use_PrivateKey(sctx, key);
  SSL_CTX_set_min_proto_version(sctx, TLS1_3_VERSION);
  SSL_CTX_set_max_proto_version(sctx, TLS1_3_VERSION);
  SSL_CTX_set_num_tickets(sctx, 0);
  SSL_CTX_set_verify(cctx, SSL_VERIFY_NONE, nullptr);

  mem_end srv, cli;
  make_end(srv, sctx, true);
  make_end(cli, cctx, false);
  if (!SSL_set_ciphersuites(srv.ssl, "TLS_AES_128_GCM_SHA256") ||
      !SSL_set_ciphersuites(cli.ssl, "TLS_AES_128_GCM_SHA256")) {
    fprintf(stderr, "cipher\n");
    return 1;
  }

  // handshake
  bool sd = false, cd = false;
  for (int i = 0; i < 64 && !(sd && cd); ++i) {
    cd |= SSL_do_handshake(cli.ssl) == 1;
    pump(cli, srv);
    sd |= SSL_do_handshake(srv.ssl) == 1;
    pump(srv, cli);
  }
  if (!sd || !cd) {
    fprintf(stderr, "handshake failed\n");
    return 1;
  }
  printf("handshake ok (%s)\n", SSL_get_cipher_name(srv.ssl));

  std::string msg(payload, 'x');
  std::string echo(payload, 0);

  auto t0 = std::chrono::steady_clock::now();
  size_t crypto_bytes = 0;
  for (int i = 0; i < rounds; ++i) {
    // client writes full payload
    size_t off = 0;
    while (off < payload) {
      int r = SSL_write(cli.ssl, msg.data() + off, (int)std::min(payload - off, size_t(16384)));
      if (r <= 0) { fprintf(stderr, "write died at %zu\n", off); return 1; }
      off += (size_t)r;
      crypto_bytes += pump(cli, srv);
    }
    // server reads all, then echoes back
    size_t got = 0;
    while (got < payload) {
      int r = SSL_read(srv.ssl, (char*)echo.data() + got, (int)(payload - got));
      if (r > 0) { got += (size_t)r; continue; }
      int e = SSL_get_error(srv.ssl, r);
      if (e == SSL_ERROR_WANT_READ) { crypto_bytes += pump(cli, srv); continue; }
      fprintf(stderr, "server read err %d\n", e);
      return 1;
    }
    off = 0;
    while (off < payload) {
      int r = SSL_write(srv.ssl, echo.data() + off, (int)std::min(payload - off, size_t(16384)));
      if (r <= 0) { fprintf(stderr, "echo write died\n"); return 1; }
      off += (size_t)r;
      crypto_bytes += pump(srv, cli);
    }
    got = 0;
    while (got < payload) {
      int r = SSL_read(cli.ssl, (char*)echo.data() + got, (int)(payload - got));
      if (r > 0) { got += (size_t)r; continue; }
      int e = SSL_get_error(cli.ssl, r);
      if (e == SSL_ERROR_WANT_READ) { crypto_bytes += pump(srv, cli); continue; }
      fprintf(stderr, "client read err %d\n", e);
      return 1;
    }
  }
  double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  double mb = 2.0 * rounds * payload / (1024.0 * 1024.0);
  printf("%d round-trips of %zuB: %.3fs -> %.1f MB/s payload (crypto moved %.1f MB)\n",
         rounds, payload, secs, mb / secs, crypto_bytes / 1048576.0);
  return 0;
}
