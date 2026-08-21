#include "cornet/websocket/client.h"

#include <cstring>

#include <spdlog/spdlog.h>

#include "cornet/concurrency/combinators.h"
#include "cornet/http/common/parser.h"
#include "cornet/http/common/serializer.h"
#include "cornet/http/common/url.h"
#include "cornet/scheduling/context.h"
#include "cornet/tls/error.h"
#include "cornet/websocket/common/handshake.h"

namespace cornet::websocket {

namespace {

/**
 * @brief the upgrade exchange, reduced to owned values.
 *
 * The parser works zero-copy over the exchange's receive buffer, but that
 * buffer dies with the exchange coroutine — so anything the caller needs is
 * copied out here before the return.
 */
struct handshake_result_t {
  uint16_t    status{0};
  std::string upgrade{};
  std::string connection{};
  std::string accept{};
  std::string protocol{};
  std::string extensions{};
  std::string leftover{};   // bytes read past the blank line: the peer's first frames
};

/**
 * @brief write the request, read and parse the response.
 *
 * Cancelable so with_timeout can own the whole budget: the write and the
 * read are both windows a stuck peer could hold forever.
 */
ccoro_t<expected<handshake_result_t>> http_upgrade_exchange(
    context_t& ctx, tls::transport_t& tr, std::string_view request,
    http::buffer_pool_t& pool) {
  // ── write the request (single gather-write, short-write safe) ──
  struct iovec iov{const_cast<char*>(request.data()), request.size()};
  while (iov.iov_len > 0) {
    expected<size_t> w;
    if (!tr.is_tls()) {
      auto r = co_await tr.plain_writev(ctx, &iov, 1);
      w = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      w = co_await tr.writev(ctx, &iov, 1);
    }
    if (!w) co_return unexpected(w.error());
    if (*w == 0) co_return unexpected(ECONNRESET);
    iov.iov_base = static_cast<char*>(iov.iov_base) + *w;
    iov.iov_len -= *w;
  }

  // ── read until the parser flags the upgrade (or a full response) ──
  http::head_buffer_t in;
  {
    auto lease = pool.acquire(16u << 10);
    in.reset(std::move(lease));
  }
  http::spill_buffer_t spill;
  http::headers_t headers;
  http::parser_t parser(http::parser_t::type_t::Response);
  parser.bind(in, spill, headers);

  auto capture = [&](handshake_result_t& out) {
    out.status = parser.status_code();
    out.upgrade = headers.get(http::field_t::Upgrade);
    out.connection = headers.get(http::field_t::Connection);
    out.accept = headers.get(kSecWebSocketAccept);
    out.protocol = headers.get(kSecWebSocketProtocol);
    out.extensions = headers.get(kSecWebSocketExtensions);
  };

  while (true) {
    auto w = in.writable();
    if (w.empty()) {
      co_return http_unexpected(http::http_error_t::HeaderTooLarge);
    }
    expected<size_t> n;
    if (!tr.is_tls()) {
      auto r = co_await tr.plain_recv(ctx, w.data(), w.size());
      n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      n = co_await tr.recv(ctx, w.data(), w.size());
    }
    if (!n) co_return unexpected(n.error());
    if (*n == 0) co_return unexpected(ECONNRESET);
    in.commit(uint32_t(*n));

    auto r = parser.execute(in.write_pos() - uint32_t(*n), uint32_t(*n));
    for (;;) {
      switch (r) {
        case http::parser_t::result_t::Upgrade: {
          handshake_result_t out;
          capture(out);
          out.leftover.assign(
              in.view(parser.consumed_offset(),
                      in.write_pos() - parser.consumed_offset()));
          co_return out;
        }
        case http::parser_t::result_t::HeadersReady:
        case http::parser_t::result_t::BodyPaused:
          r = parser.resume();
          continue;
        case http::parser_t::result_t::MessageReady:
          // a complete non-upgrade answer: the handshake was refused. The
          // status is reported so the caller can log why
          SPDLOG_DEBUG("ws: handshake refused with status {}", parser.status_code());
          co_return websocket_unexpected(websocket_error_t::HandshakeFailed);
        case http::parser_t::result_t::Error:
          co_return websocket_unexpected(websocket_error_t::HandshakeFailed);
        case http::parser_t::result_t::NeedMore:
          break;
      }
      break;
    }
  }
}

/**
 * @brief validate the 101 the server answered, per RFC 6455 §4.2.2/§4.1.
 * @return the accepted subprotocol (may be empty), or HandshakeFailed
 */
expected<std::string_view> validate_handshake_response(
    const handshake_result_t& res, std::string_view key,
    const std::vector<std::string>& offered) {
  if (res.status != 101) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  if (!http::iequals(res.upgrade, "websocket")) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  // Connection is a token list; case-insensitive containment, not equality
  bool token = false;
  for (size_t pos = 0; pos <= res.connection.size() && !token;) {
    size_t end = res.connection.find(',', pos);
    if (end == std::string::npos) end = res.connection.size();
    auto t = std::string_view(res.connection).substr(pos, end - pos);
    while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) t.remove_prefix(1);
    while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) t.remove_suffix(1);
    token = http::iequals(t, "upgrade");
    pos = end + 1;
  }
  if (!token) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }

  char accept[kAcceptLen];
  accept_key(key, accept);
  if (res.accept != std::string_view(accept, kAcceptLen)) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  // we offer no extensions, so the server must select none (RFC 6455 §9.1)
  if (!res.extensions.empty()) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  if (res.protocol.empty()) return std::string_view{};
  for (const auto& o : offered) {
    if (res.protocol == o) return std::string_view(res.protocol);
  }
  // the server picked a subprotocol we never offered
  return websocket_unexpected(websocket_error_t::HandshakeFailed);
}

/**
 * @brief a connected-family socket wrapped in its transport.
 * The family is known only after resolve, so construction cannot be a
 * declaration at the top of connect().
 */
tls::transport_t make_transport(sa_family_t family, int& fd) {
  if (family == AF_INET6) {
    tcp::v6::socket_t sock;
    fd = sock.native_fd();
    return tls::transport_t(std::move(sock));
  }
  tcp::v4::socket_t sock;
  fd = sock.native_fd();
  return tls::transport_t(std::move(sock));
}

} // namespace

coro_t<expected<std::unique_ptr<session_t>>> connect(context_t& ctx,
                                                     std::string_view url,
                                                     client_options_t opt) {
  // ws/wss map onto the http url machinery: the schemes differ, everything
  // else (authority, path, query, ports 80/443) is identical
  bool secure;
  std::string http_url;
  if (url.substr(0, 5) == "ws://") {
    secure = false;
    http_url = "http://" + std::string(url.substr(5));
  } else if (url.substr(0, 6) == "wss://") {
    secure = true;
    http_url = "https://" + std::string(url.substr(6));
  } else {
    co_return http_unexpected(http::http_error_t::UnsupportedScheme);
  }

  auto u = http::url_t::parse(http_url);
  if (!u) co_return unexpected(u.error());
  if (!u->userinfo().empty()) {
    // ws has no use for credentials in the authority, and echoing them into
    // the Host header is how they leak
    co_return http_unexpected(http::http_error_t::BadUrl);
  }
  if (secure && !opt.tls) {
    co_return tls_unexpected(tls::tls_error_t::Disabled);
  }

  // ── resolve + connect, all inside the handshake budget ──
  auto resolved = co_await cornet::resolve(ctx, u->host(), u->port());
  if (!resolved) co_return unexpected(resolved.error());

  int fd = -1;
  tls::transport_t tr = make_transport(resolved->addr.ss_family, fd);
  if (fd < 0) co_return unexpected(errno);

  auto c = co_await with_timeout(ctx, tr.connect(ctx, *resolved), opt.handshake_timeout);
  if (!c) {
    tr.abandon(ctx);
    co_return unexpected(c.error());
  }

  if (secure) {
    auto hs = co_await with_timeout(
        ctx, tr.start_tls(ctx, opt.tls, tls::engine_mode_t::Client, u->host()),
        opt.handshake_timeout);
    if (!hs) {
      tr.abandon(ctx);
      co_return unexpected(hs.error());
    }
  }

  // ── frame and exchange the upgrade request, same budget ──
  char key[kKeyLen];
  make_key(key);
  std::string offered;
  for (size_t i = 0; i < opt.subprotocols.size(); ++i) {
    if (i) offered += ", ";
    offered += opt.subprotocols[i];
  }

  http::buffer_pool_t& pool = http::buffer_pool_t::local();
  http::out_buffer_t out;
  {
    auto lease = pool.acquire(4u << 10);
    out.reset(std::move(lease));
  }
  frame_handshake_request(out, u->authority(), u->path(), u->query(),
                          std::string_view(key, kKeyLen), offered);
  if (out.failed()) {
    tr.abandon(ctx);
    co_return unexpected(out.error());
  }

  auto exch = co_await with_timeout(
      ctx, http_upgrade_exchange(ctx, tr, out.view(), pool), opt.handshake_timeout);
  if (!exch) {
    tr.abandon(ctx);
    co_return unexpected(exch.error());
  }

  auto proto = validate_handshake_response(*exch, std::string_view(key, kKeyLen),
                                           opt.subprotocols);
  if (!proto) {
    tr.abandon(ctx);
    co_return unexpected(proto.error());
  }

  auto ws = std::make_unique<session_t>(ctx, std::move(tr), role_t::Client, pool,
                                        ctx.timeout_wheel(), opt.session,
                                        exch->leftover, nullptr);
  if (!proto->empty()) ws->set_subprotocol(*proto);
  SPDLOG_DEBUG("ws: connected to {}{}", u->authority(),
               u->path().empty() ? "/" : u->path());
  co_return std::move(ws);
}

} // namespace cornet::websocket
