#include "cornet/tls/dial.h"

#include <cerrno>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

namespace cornet::tls {

coro_t<expected<transport_t>> dial(context_t& ctx, deadline_t& deadline,
                                   const resolved_address& addr, const dial_options_t& opt) {
  transport_t tr = [&]() {
    // The family is known only after resolve, so socket construction happens
    // here, not at the caller's top
    if (addr.addr.ss_family == AF_INET6) {
      return transport_t(tcp::v6::socket_t{});
    }
    return transport_t(tcp::v4::socket_t{});
  }();
  if (tr.native_fd() < 0) co_return unexpected(errno);
  if (opt.tcp_nodelay) {
    int on = 1;
    ::setsockopt(tr.native_fd(), IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  }

  deadline.cap(opt.connect_timeout);
  auto c = co_await with_deadline(ctx, tr.connect(ctx, addr), deadline);
  if (!c) {
    tr.abandon(ctx);
    co_return unexpected(deadline.map(c.error()));
  }

  if (opt.tls_cx) {
    deadline.cap(opt.handshake_timeout);
    auto hs = co_await with_deadline(
        ctx, tr.start_tls(ctx, opt.tls_cx, engine_mode_t::Client, opt.server_name),
        deadline);
    if (!hs) {
      tr.abandon(ctx);
      co_return unexpected(deadline.map(hs.error()));
    }
  }

  co_return std::move(tr);
}

} // namespace cornet::tls
