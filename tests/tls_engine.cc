/**
 * @brief TLS engine pair tests: handshake, data, shutdown — everything except
 * the socket.
 *
 * The engine is a pure state machine over two memory BIOs, so two engines can
 * be played against each other by shuffling ciphertext by hand. No io_uring,
 * no sockets, no threads: this file runs on any kernel, and that is exactly
 * why tls_engine_t is separate from tls_stream_t.
 */

#include "cornet/tls/engine.h"

#include <cstring>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "cornet/tls/context.h"

#include "tls_certs.h"

namespace cornet::tls {
namespace {

#ifdef CORNET_WITH_TLS

struct pair_t {
  std::shared_ptr<tls_context_t> server_ctx;
  std::shared_ptr<tls_context_t> client_ctx;
  tls_engine_t server;
  tls_engine_t client;
};

// Shuffle pending output from one engine into the other until nothing moves.
// Returns the bytes moved; 0 would turn a hand-driven loop into a deadlock.
size_t pump(tls_engine_t& from, tls_engine_t& to) {
  char buf[16u * 1024u];
  size_t moved = 0;
  while (from.output_pending() > 0) {
    size_t n = from.take_output(buf, sizeof(buf));
    if (n == 0) break;
    EXPECT_TRUE(to.feed_input(buf, n).has_value());
    moved += n;
  }
  return moved;
}

expected<pair_t> make_pair(std::string_view ca_pem, bool verify) {
  auto server_ctx = tls_context_t::make_server(tls_server_options_t{
      .cert_pem = std::string(test::tls::kServerCert),
      .key_pem = std::string(test::tls::kServerKey),
  });
  if (!server_ctx) return unexpected(server_ctx.error());
  auto client_ctx = tls_context_t::make_client(tls_client_options_t{
      .verify_peer = verify,
      .ca_pem = std::string(ca_pem),
  });
  if (!client_ctx) return unexpected(client_ctx.error());

  pair_t out;
  out.server_ctx = *server_ctx;
  out.client_ctx = *client_ctx;
  auto se = tls_engine_t::create(**server_ctx, engine_mode_t::Server);
  if (!se) return unexpected(se.error());
  auto ce = tls_engine_t::create(**client_ctx, engine_mode_t::Client, "localhost");
  if (!ce) return unexpected(ce.error());
  out.server = std::move(*se);
  out.client = std::move(*ce);
  return out;
}

// Drive both engines until both handshake steps report Done.
void handshake(pair_t& p) {
  bool server_done = false, client_done = false;
  for (int round = 0; round < 64 && !(server_done && client_done); ++round) {
    size_t moved = 0;
    if (!client_done) {
      auto s = p.client.handshake();
      ASSERT_NE(s, engine_step_t::Failed) << "client: " << p.client.error().message();
      client_done = (s == engine_step_t::Done);
      moved += pump(p.client, p.server);
    }
    if (!server_done) {
      auto s = p.server.handshake();
      ASSERT_NE(s, engine_step_t::Failed) << "server: " << p.server.error().message();
      server_done = (s == engine_step_t::Done);
      moved += pump(p.server, p.client);
    }
    ASSERT_GT(moved, 0u) << "engine pair stalled";
  }
  ASSERT_TRUE(server_done && client_done);
}

// client writes msg, server reads it back byte equal
void echo_round(pair_t& p, std::string_view msg) {
  ASSERT_LE(msg.size(), 64u * 1024u);
  std::string plane(msg);
  std::string buf(msg.size(), '\0');

  size_t off = 0;
  for (int round = 0; round < 64 && off < plane.size(); ++round) {
    size_t n = std::min(plane.size() - off, tls_engine_t::kRecordPayload);
    ASSERT_EQ(p.client.write(plane.data() + off, n), engine_step_t::Done);
    off += n;
    ASSERT_GT(pump(p.client, p.server), 0u);
  }

  size_t got_total = 0;
  for (int round = 0; round < 64 && got_total < plane.size(); ++round) {
    size_t got = 0;
    auto s = p.server.read(buf.data() + got_total, buf.size() - got_total, got);
    if (s == engine_step_t::WantRead) {
      ASSERT_GT(pump(p.client, p.server), 0u);
      continue;
    }
    ASSERT_EQ(s, engine_step_t::Done);
    got_total += got;
  }
  EXPECT_EQ(buf, plane);
}

TEST(TlsEngine, HandshakeCompletes) {
  auto pair = make_pair(test::tls::kCaCert, false);
  ASSERT_TRUE(pair.has_value());
  handshake(*pair);
  EXPECT_EQ(pair->server.version(), "TLSv1.3");
  EXPECT_EQ(pair->client.version(), "TLSv1.3");
  EXPECT_FALSE(pair->server.cipher().empty());
}

TEST(TlsEngine, VerificationPassesWithRightCa) {
  auto pair = make_pair(test::tls::kCaCert, true);
  ASSERT_TRUE(pair.has_value());
  handshake(*pair);
}

TEST(TlsEngine, VerificationFailsWithWrongCa) {
  auto pair = make_pair(test::tls::kEvilCaCert, true);
  ASSERT_TRUE(pair.has_value());

  bool client_failed = false;
  for (int round = 0; round < 64 && !client_failed; ++round) {
    auto cs = pair->client.handshake();
    if (cs == engine_step_t::Failed) {
      client_failed = true;
      EXPECT_EQ(pair->client.error().code, int(tls_error_t::VerifyFailed));
      break;
    }
    pump(pair->client, pair->server);
    auto ss = pair->server.handshake();
    pump(pair->server, pair->client);
    if (ss == engine_step_t::Failed) break;  // a server alert is fine too
    ASSERT_NE(cs, engine_step_t::Done);
  }
  EXPECT_TRUE(client_failed) << "the wrong CA must not verify";
}

TEST(TlsEngine, DataRoundTrip) {
  auto pair = make_pair(test::tls::kCaCert, false);
  ASSERT_TRUE(pair.has_value());
  handshake(*pair);

  echo_round(*pair, "hello tls");
  // larger than one record: the write side must chunk quietly
  echo_round(*pair, std::string(50u * 1024u, 'x'));
}

TEST(TlsEngine, ShutdownIsObservedAsClosed) {
  auto pair = make_pair(test::tls::kCaCert, false);
  ASSERT_TRUE(pair.has_value());
  handshake(*pair);

  EXPECT_EQ(pair->client.shutdown(), engine_step_t::Done);
  ASSERT_GT(pump(pair->client, pair->server), 0u);

  // the peer sees close_notify the next time it reads
  char buf[16];
  size_t got = 0;
  EXPECT_EQ(pair->server.read(buf, sizeof(buf), got), engine_step_t::Closed);
}

TEST(TlsEngine, ServerDoesNotEmitSessionTicketsByDefault) {
  // A pool decides liveness with a raw-fd MSG_PEEK; tickets are the only
  // bytes an idle TLS 1.3 connection receives, and they fail that probe. With
  // the default num_tickets{0} the server must queue nothing after the
  // handshake completes.
  auto pair = make_pair(test::tls::kCaCert, false);
  ASSERT_TRUE(pair.has_value());
  handshake(*pair);
  EXPECT_EQ(pair->server.output_pending(), 0u);
}

TEST(TlsEngine, GarbageServerInputFailsHandshake) {
  auto pair = make_pair(test::tls::kCaCert, false);
  ASSERT_TRUE(pair.has_value());

  const char junk[] = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
  ASSERT_TRUE(pair->server.feed_input(junk, sizeof(junk) - 1).has_value());

  EXPECT_EQ(pair->server.handshake(), engine_step_t::Failed);
  EXPECT_EQ(pair->server.error().code, int(tls_error_t::Handshake));
  EXPECT_EQ(pair->server.error().domain, error_domain::Tls);
}

#else

TEST(TlsEngine, DisabledBuildReportsPrecisely) {
  auto cx = tls_context_t::make_client();
  ASSERT_FALSE(cx.has_value());
  EXPECT_EQ(cx.error().code, int(tls_error_t::Disabled));
  EXPECT_EQ(cx.error().domain, error_domain::Tls);
  // and the renderer registered at load time
  EXPECT_STREQ(cx.error().message(), "tls disabled at build time (CORNET_WITH_TLS=OFF)");
}

#endif

} // namespace
} // namespace cornet::tls
