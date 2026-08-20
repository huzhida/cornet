#include "cornet/tls/stream.h"

#include <cstring>

#include "cornet/coroutine/cancel.h"
#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"

namespace cornet::tls {

tls_stream_t::tls_stream_t(tcp::socket_t sock, tls_engine_t engine)
  : sock_(std::move(sock)), engine_(std::move(engine)) {}

tls_stream_t::~tls_stream_t() = default;
tls_stream_t::tls_stream_t(tls_stream_t&&) noexcept = default;
tls_stream_t& tls_stream_t::operator=(tls_stream_t&&) noexcept = default;

bool tls_stream_t::valid() const {
  return engine_.valid_for_io();
}

tcp::socket_t tls_stream_t::release_socket() { return std::move(sock_); }

int tls_stream_t::release() { return sock_.release(); }

ccoro_t<expected<void>> tls_stream_t::flush_output(context_t& ctx) {
  ensure_io_bite();
  char* buf = io_bite_.get();
  while (engine_.output_pending() > 0) {
    size_t n = engine_.take_output(buf, io_bite_size_);
    size_t off = 0;
    while (off < n) {
      auto w = co_await sock_.send(ctx, buf + off, n - off);
      if (!w) co_return unexpected(w.error());
      if (*w == 0) co_return unexpected(EPIPE);
      off += *w;
    }
    // grow only after the piece this buffer holds has fully left — realloc
    // frees the old scratch
    if (n == io_bite_size_) {
      maybe_grow_io_bite();
      buf = io_bite_.get();
    }
  }
  co_return {};
}

ccoro_t<expected<void>> tls_stream_t::fill_input(context_t& ctx) {
  ensure_io_bite();
  auto n = co_await sock_.recv(ctx, io_bite_.get(), io_bite_size_);
  if (!n) co_return unexpected(n.error());
  if (*n == 0) {
    // TCP EOF mid-records: an unclean shutdown, not a close_notify.
    co_return tls_unexpected(tls_error_t::UnexpectedEof);
  }
  if (auto ok = engine_.feed_input(io_bite_.get(), *n); !ok) co_return unexpected(ok.error());
  if (*n == io_bite_size_) maybe_grow_io_bite();
  co_return {};
}

ccoro_t<expected<void>> tls_stream_t::handshake(context_t& ctx) {
  for (;;) {
    // Flights always end with output to flush; doing it unconditionally keeps
    // the loop two lines shorter and costs nothing when the wbio is empty.
    if (auto ok = co_await flush_output(ctx); !ok) co_return ok;
    switch (engine_.handshake()) {
      case engine_step_t::Done:
        co_return {};
      case engine_step_t::WantRead:
        // The step just queued its flight (ClientHello etc.); waiting for the
        // peer without sending it first is a mutual-recv deadlock.
        if (auto ok = co_await flush_output(ctx); !ok) co_return ok;
        if (auto ok = co_await fill_input(ctx); !ok) co_return ok;
        break;
      case engine_step_t::WantWrite:
        break;  // next iteration flushes
      case engine_step_t::Closed:
        co_return tls_unexpected(tls_error_t::Handshake);
      case engine_step_t::Failed:
        co_return unexpected(engine_.error());
    }
  }
}

ccoro_t<expected<size_t>> tls_stream_t::recv(context_t& ctx, void* buf, size_t len) {
  for (;;) {
    size_t got = 0;
    switch (engine_.read(buf, len, got)) {
      case engine_step_t::Done:
        co_return got;
      case engine_step_t::Closed:
        // close_notify is the TLS EOF: same contract as recv() returning 0.
        co_return size_t(0);
      case engine_step_t::WantRead:
        // Output first: a pending ticket or KeyUpdate reply must not stall
        // behind a read that needs the peer to see them.
        if (auto ok = co_await flush_output(ctx); !ok) co_return unexpected(ok.error());
        if (auto ok = co_await fill_input(ctx); !ok) co_return unexpected(ok.error());
        break;
      case engine_step_t::WantWrite:
        if (auto ok = co_await flush_output(ctx); !ok) co_return unexpected(ok.error());
        break;
      case engine_step_t::Failed:
        co_return unexpected(engine_.error());
    }
  }
}

ccoro_t<expected<size_t>> tls_stream_t::writev(context_t& ctx, const struct iovec* iov,
                                               size_t iov_len) {
  if (!stage_) stage_ = std::make_unique<char[]>(16u * 1024u);
  size_t total = 0;
  size_t staged = 0;

  auto flush_stage = [&]() -> engine_step_t {
    if (staged == 0) return engine_step_t::Done;
    return engine_.write(stage_.get(), staged);
  };

  for (size_t i = 0; i < iov_len; ++i) {
    const char* p = static_cast<const char*>(iov[i].iov_base);
    size_t left = iov[i].iov_len;
    while (left > 0) {
      size_t room = tls_engine_t::kRecordPayload - staged;
      size_t take = left < room ? left : room;
      std::memcpy(stage_.get() + staged, p, take);
      staged += take;
      p += take;
      left -= take;
      total += take;
      if (staged < tls_engine_t::kRecordPayload) continue;
      // a full record: encrypt and queue it
      for (;;) {
        switch (flush_stage()) {
          case engine_step_t::Done:
            staged = 0;
            goto next_slice;
          case engine_step_t::WantWrite:
          case engine_step_t::WantRead:
            if (auto ok = co_await flush_output(ctx); !ok) co_return unexpected(ok.error());
            // WantRead from SSL_write means a pending handshake message needs wire
            // input before ours can go out (TLS 1.3 re-key, renegotiation).
            if (engine_.output_pending() == 0) {
              if (auto ok = co_await fill_input(ctx); !ok) co_return unexpected(ok.error());
            }
            break;
          case engine_step_t::Closed:
            co_return unexpected(EPIPE);
          case engine_step_t::Failed:
            co_return unexpected(engine_.error());
        }
      }
    next_slice:;
    }
  }

  // the tail record
  if (staged > 0) {
    for (;;) {
      switch (flush_stage()) {
        case engine_step_t::Done:
          staged = 0;
          goto tail_done;
        case engine_step_t::WantWrite:
        case engine_step_t::WantRead:
          if (auto ok = co_await flush_output(ctx); !ok) co_return unexpected(ok.error());
          if (engine_.output_pending() == 0) {
            if (auto ok = co_await fill_input(ctx); !ok) co_return unexpected(ok.error());
          }
          break;
        case engine_step_t::Closed:
          co_return unexpected(EPIPE);
        case engine_step_t::Failed:
          co_return unexpected(engine_.error());
      }
    }
  tail_done:;
  }

  if (auto ok = co_await flush_output(ctx); !ok) co_return unexpected(ok.error());
  co_return total;
}

ccoro_t<expected<void>> tls_stream_t::shutdown_write(context_t& ctx) {
  for (;;) {
    switch (engine_.shutdown()) {
      case engine_step_t::Done:
      case engine_step_t::Closed:
        if (auto ok = co_await flush_output(ctx); !ok) co_return ok;
        co_return {};
      case engine_step_t::WantWrite:
        if (auto ok = co_await flush_output(ctx); !ok) co_return ok;
        break;
      case engine_step_t::WantRead:
        // The peer owes a reply before our alert can go out; honoring it keeps
        // the alert reliable without turning this into a full-duplex close.
        if (auto ok = co_await flush_output(ctx); !ok) co_return ok;
        if (auto ok = co_await fill_input(ctx); !ok) co_return ok;
        break;
      case engine_step_t::Failed:
        co_return unexpected(engine_.error());
    }
  }
}

} // namespace cornet::tls
