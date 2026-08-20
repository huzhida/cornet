#ifndef CORNET_TLS_STREAM_H
#define CORNET_TLS_STREAM_H

#include <memory>
#include <string_view>
#include <sys/uio.h>

#include "cornet/base/expected.h"
#include "cornet/coroutine/coro.h"
#include "cornet/net/socket.h"
#include "cornet/tls/engine.h"

namespace cornet::tls {

/**
 * @brief a TLS connection: a tcp socket plus the engine that encrypts it.
 *
 * Every operation is a coroutine (ccoro_t) rather than a leaf awaiter, on
 * purpose: one logical TLS read is several socket operations underneath (drain
 * output, fill input, retry), and making it a coroutine means the framework's
 * existing cancellation machine covers the whole thing. Inject a canceler at
 * the call site (with_cancel) and the pump's innermost socket wait is what
 * actually gets cancelled — no TLS-specific cancel code exists anywhere.
 *
 * The shared-nothing rule applies with teeth: the embedded SSL object is not
 * thread-safe, so a stream must never migrate between contexts once engaged.
 * Hand it over (spawn_remote) only before handshake().
 */
class tls_stream_t {
 public:
  // no default ctor: there is no such thing as a socket-less stream
  // (tcp::socket_t itself has none either)
  tls_stream_t(tcp::socket_t sock, tls_engine_t engine);
  ~tls_stream_t();

  tls_stream_t(const tls_stream_t&) = delete;
  tls_stream_t& operator=(const tls_stream_t&) = delete;
  tls_stream_t(tls_stream_t&&) noexcept;
  tls_stream_t& operator=(tls_stream_t&&) noexcept;

  CORNET_NODISCARD bool valid() const;
  CORNET_NODISCARD int native_fd() const { return sock_.native_fd(); }

  /**
   * @brief drive the handshake to completion, shuffling flight data over the
   * socket as needed.
   */
  CORNET_NODISCARD ccoro_t<expected<void>> handshake(context_t& ctx);

  /**
   * @brief read decrypted bytes.
   * Returns 0 on a clean close_notify, matching recv()'s EOF convention.
   */
  CORNET_NODISCARD ccoro_t<expected<size_t>> recv(context_t& ctx, void* buf, size_t len);

  /**
   * @brief gather-write through the record layer.
   * Plaintext from every iovec is appended into a staging buffer and encrypted
   * per record — the one copy a software TLS cannot avoid. The ciphertext then
   * leaves in one socket write at the end. The plain transport keeps its
   * native writev; this asymmetry is documented in docs/tls.md.
   */
  CORNET_NODISCARD ccoro_t<expected<size_t>> writev(context_t& ctx, const struct iovec* iov,
                                                    size_t iov_len);

  /**
   * @brief send close_notify (the TLS half-close equivalent).
   * Does not wait for the peer's own close_notify: that arrives through a
   * later recv() returning 0, keeping drain logic identical to the plain
   * transport's SHUT_WR-then-drain. Any timeout is the caller's to impose.
   */
  CORNET_NODISCARD ccoro_t<expected<void>> shutdown_write(context_t& ctx);

  /** @brief recover ownership of the socket, e.g. after a failed handshake. */
  tcp::socket_t release_socket();

  /** @brief give up the descriptor without touching the engine. */
  int release();
  CORNET_NODISCARD std::string_view version() const { return engine_.version(); }
  CORNET_NODISCARD std::string_view cipher() const { return engine_.cipher(); }
  CORNET_NODISCARD const tls_engine_t& engine() const { return engine_; }

 private:
  CORNET_NODISCARD ccoro_t<expected<void>> flush_output(context_t& ctx);
  CORNET_NODISCARD ccoro_t<expected<void>> fill_input(context_t& ctx);

  void ensure_io_bite() {
    if (!io_bite_) io_bite_ = std::make_unique<char[]>(io_bite_size_);
  }
  // a full pull means the next one wants to be bigger; double to the cap
  void maybe_grow_io_bite() {
    if (io_bite_size_ < kMaxIoBiteSize) {
      io_bite_size_ = kMaxIoBiteSize;
      io_bite_ = std::make_unique<char[]>(io_bite_size_);
    }
  }

  // one TLS record carries at most 16KiB of payload, but that is a protocol
  // number, not an io stride: reading/writing in bigger bites cuts the io op
  // count per message 4x. Connections that never see big messages must not pay
  // for it — the scratch starts at one-record size and doubles to kMax after
  // the wire itself proves bigger windows are coming.
  static constexpr size_t kMinIoBiteSize = 16u * 1024u;
  static constexpr size_t kMaxIoBiteSize = 64u * 1024u;

  tcp::socket_t      sock_;
  tls_engine_t       engine_;
  // staging for writev aggregation, allocated on first use
  std::unique_ptr<char[]> stage_;
  // shared fill/flush scratch: never needed for both directions at once
  std::unique_ptr<char[]> io_bite_;
  size_t                 io_bite_size_{kMinIoBiteSize};
};

} // namespace cornet::tls

#endif // CORNET_TLS_STREAM_H
