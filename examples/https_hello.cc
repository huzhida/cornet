/**
 * @brief minimal HTTPS hello world.
 *
 * Certificate and key come from the filesystem (the layout deployments use);
 * for a one-off localhost pair:
 *
 *   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
 *     -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost" \
 *     -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
 *
 * then:
 *   ./https-hello [cert.pem] [key.pem] [port]
 *   curl --cacert cert.pem https://localhost:8443/hello
 */

#include <cstdio>

#include <cornet/http_server.h>
#include <cornet/tls/context.h>

using namespace cornet;

int main(int argc, char** argv) {
  const char* cert = argc > 1 ? argv[1] : "cert.pem";
  const char* key = argc > 2 ? argv[2] : "key.pem";
  uint16_t port = argc > 3 ? uint16_t(std::atoi(argv[3])) : 8443;

  auto tls_ctx = tls::tls_context_t::make_server(tls::tls_server_options_t{
      .cert_file = cert,
      .key_file = key,
  });
  if (!tls_ctx) {
    std::fprintf(stderr, "tls setup failed: %s\n(see the file header for how to "
                         "generate a localhost cert/key pair)\n",
                 tls_ctx.error().message());
    return 1;
  }

  context_t ctx;
  http::server_t server(ctx, http::server_options_t{
      .port = port,
      .tls = *tls_ctx,
  });

  server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet over tls"); });

  if (auto ok = server.listen(); !ok) {
    std::fprintf(stderr, "listen failed: %s\n", ok.error().message());
    return 1;
  }
  ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });
  ctx.spawn(server.serve());
  ctx.run();
}
