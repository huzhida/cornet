#ifndef CORNET_TLS_TRANSPORT_H
#define CORNET_TLS_TRANSPORT_H

#include <memory>
#include <string_view>
#include <sys/uio.h>
#include <variant>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/concurrency/deadline.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/coro.h"
#include "cornet/net/socket.h"
#include "cornet/tls/context.h"
#include "cornet/tls/stream.h"

namespace cornet::tls {

/**
 * @brief one byte-stream transport for protocol code: plain TCP or TLS.
 *
 * The unification point the http server/client are written against. Every op
 * is a ccoro_t with the same signature in both modes, so callers co_await one
 * shape and keep their existing with_cancel/with_timeout wrapping; the extra
 * coroutine frame per op on the plain path is what frame_pool is for.
 *
 * When CORNET_WITH_TLS is off the type still exists (protocol code compiles
 * unchanged); start_tls() then fails with tls_error_t::Disabled instead of
 * taking the socket away.
 */
class transport_t {
 public:
  transport_t() = default;
  explicit transport_t(tcp::socket_t sock);
  ~transport_t();

  transport_t(const transport_t&) = delete;
  transport_t& operator=(const transport_t&) = delete;
  transport_t(transport_t&&) noexcept;
  transport_t& operator=(transport_t&&) noexcept;

  CORNET_NODISCARD bool is_tls() const;
  CORNET_NODISCARD int native_fd() const;

  // Plain-mode only: TLS always starts from an already connected socket, so
  // this is never meaningful after start_tls().
  CORNET_NODISCARD socket_t::connect_awaiter connect(context_t& ctx,
                                                     const resolved_address& resolved);

  CORNET_NODISCARD ccoro_t<expected<size_t>> recv(context_t& ctx, void* buf, size_t len);
  CORNET_NODISCARD ccoro_t<expected<size_t>> writev(context_t& ctx, const struct iovec* iov,
                                                    size_t iov_len);

  /**
   * @brief half-close, then a bounded drain.
   * Half-closing first lets queued writes flush so the peer sees a clean FIN;
   * a short read window then mops up our receive buffer so the close() that
   * follows cannot turn into an RST that would eat already-sent bytes (the
   * peer may discard them once reset). Protocol-agnostic courtesy: the http
   * server calls it with a dedicated drain canceler, websocket with its read
   * canceler — the helper neither knows nor cares.
   */
  CORNET_NODISCARD coro_t<void> close_gracefully(
      context_t& ctx, deadline_t& deadline, canceler_t& canceler,
      std::chrono::milliseconds window = std::chrono::milliseconds(200),
      uint32_t max_reads = 4);

  /**
   * @brief per-syscall counters of one writev_all() call, when the caller
   * reports transport io metrics of its own.
   */
  struct writev_stat_t {
    uint32_t calls{0};    // writev syscalls issued
    uint32_t partial{0};  // syscalls that left bytes unsent
  };

  /**
   * @brief write a whole iovec span, looping over short writes.
   * The single place the plain/TLS dispatch and the advance loop live: callers
   * used to hand-copy both, and the copies drifted (partial counters counted
   * three different things; one advance helper was dead code). The plain
   * branch awaits the socket's leaf awaiter inside this frame — zero extra
   * frames per iteration, the same cost the old hand-copied loops paid.
   * The iovec array is advanced in place; a zero-byte write is ECONNRESET.
   * Cancellation composes the usual way — wrap in with_cancel().
   */
  CORNET_NODISCARD ccoro_t<expected<void>> writev_all(context_t& ctx, struct iovec* iov,
                                                      size_t iov_len,
                                                      writev_stat_t* stat = nullptr);

  /**
   * @brief upgrade the connection to TLS and run the handshake.
   *
   * On failure the transport stays plain (a stale fd is never lost: the caller
   * can still abandon() it), and the error says why. server_name matters only
   * in Client mode (SNI + hostname verification). OpenSSL up-refs the context,
   * so ctx may be dropped after this returns.
   */
  CORNET_NODISCARD ccoro_t<expected<void>> start_tls(context_t& ctx,
                                                     std::shared_ptr<tls_context_t> cx,
                                                     engine_mode_t mode,
                                                     std::string_view server_name = {});

  /**
   * @brief half-close: SHUT_WR plain, close_notify over TLS.
   * The peer's own close arrives as a later recv() returning 0 in both modes,
   * so drain code is shared. Any timeout belongs to the caller.
   */
  CORNET_NODISCARD ccoro_t<expected<void>> shutdown_write(context_t& ctx);

  /**
   * @brief hand the fd to a fire-and-forget io_uring close, engine included.
   * Used on error paths where close_notify ceremony is beside the point.
   */
  void abandon(context_t& ctx);

  // ── test/trace accessors; empty on the plain path ──

  CORNET_NODISCARD std::string_view tls_version() const;
  CORNET_NODISCARD std::string_view tls_cipher() const;

 private:
  std::variant<tcp::socket_t, tls_stream_t> io_;
};

} // namespace cornet::tls

#endif // CORNET_TLS_TRANSPORT_H
