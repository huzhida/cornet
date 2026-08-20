#ifndef CORNET_TLS_ENGINE_H
#define CORNET_TLS_ENGINE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "cornet/base/expected.h"
#include "cornet/tls/error.h"

namespace cornet::tls {

class tls_context_t;

enum class engine_mode_t : uint8_t { Client, Server };

/**
 * @brief result of driving one step of the TLS state machine.
 *
 * The engine is a pure state machine over two memory BIOs: it never touches a
 * socket. The caller (tls_stream_t, or a test) shuffles ciphertext between the
 * wire and feed_input()/take_output(). That split is what makes the whole
 * handshake/read/write/shutdown logic testable without io_uring or even a
 * kernel that knows what io_uring is.
 */
enum class engine_step_t : uint8_t {
  Done,       // the step completed
  WantRead,   // cannot progress until more ciphertext is fed
  WantWrite,  // cannot progress until pending output is drained
  Closed,     // orderly close (close_notify) observed
  Failed,     // error() carries the reason
};

class tls_engine_t {
 public:
  tls_engine_t();
  ~tls_engine_t();

  tls_engine_t(const tls_engine_t&) = delete;
  tls_engine_t& operator=(const tls_engine_t&) = delete;
  tls_engine_t(tls_engine_t&&) noexcept;
  tls_engine_t& operator=(tls_engine_t&&) noexcept;

  /**
   * @brief build an engine around one connection.
   * @param server_name client mode: SNI and the name the certificate is checked
   *        against; ignored in server mode
   */
  CORNET_NODISCARD static expected<tls_engine_t> create(const tls_context_t& ctx,
                                                        engine_mode_t mode,
                                                        std::string_view server_name = {});

  // ── state machine steps; call until Done/Closed/Failed ──

  engine_step_t handshake();

  /**
   * @brief decrypt into buf.
   * Done with got>0, WantRead/WantWrite to stall, Closed on close_notify
   * (the caller maps this to a clean 0-byte read).
   */
  engine_step_t read(void* buf, size_t len, size_t& got);

  /**
   * @brief encrypt one record; len must not exceed kRecordPayload.
   * Done means the ciphertext sits in output. WantRead is legal even on a
   * write: a KeyUpdate reply may have to be processed first.
   */
  engine_step_t write(const void* buf, size_t len);

  /**
   * @brief queue a close_notify.
   * Done means the alert is in output; this side is then half-closed. The
   * peer's own close_notify shows up later as read() -> Closed, which keeps
   * drain semantics identical to TCP half-close.
   */
  engine_step_t shutdown();

  // ── ciphertext shuffling ──

  CORNET_NODISCARD size_t output_pending() const;
  size_t take_output(void* buf, size_t len);
  CORNET_NODISCARD expected<void> feed_input(const void* buf, size_t len);

  // ── introspection (tests and logging) ──

  CORNET_NODISCARD error_t error() const { return err_; }
  CORNET_NODISCARD bool valid_for_io() const;
  CORNET_NODISCARD std::string_view version() const;
  CORNET_NODISCARD std::string_view cipher() const;

  // one TLS record carries at most 2^14 bytes of payload
  static constexpr size_t kRecordPayload = 16u * 1024u;

 private:
  engine_step_t classify(int ret, bool in_handshake);

  struct impl;
  std::unique_ptr<impl> impl_;
  error_t err_{};
  bool handshaken_{false};
};

} // namespace cornet::tls

#endif // CORNET_TLS_ENGINE_H
