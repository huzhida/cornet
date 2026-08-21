#include "cornet/websocket/session.h"

#include <algorithm>
#include <cstring>

#include <spdlog/spdlog.h>

#include "cornet/concurrency/combinators.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/http/server/connection.h"
#include "cornet/scheduling/context.h"
#include "cornet/utils/config.h"
#include "cornet/websocket/common/handshake.h"

namespace cornet::websocket {

// ─────────────────────────── session_options_t ───────────────────────────

void session_options_t::load(config_t* config, const char* prefix) {
  if (!config) return;
  const std::string base = std::string(prefix) + ".";
  auto at = [&](const char* key) { return config->at_path((base + key).c_str()); };

  max_message_bytes = at("max_message_bytes").value_or(max_message_bytes);
  recv_buffer_bytes = at("recv_buffer_bytes").value_or(recv_buffer_bytes);

  auto duration = [&](const char* key, std::chrono::milliseconds& target) {
    if (auto s = at(key).value<std::string_view>()) {
      target = std::chrono::duration_cast<std::chrono::milliseconds>(parse_time_str(*s));
    } else if (auto ms = at(key).value<int64_t>()) {
      target = std::chrono::milliseconds(*ms);
    }
  };
  duration("idle_timeout", idle_timeout);
  duration("close_timeout", close_timeout);
}

// ───────────────────────────── message_t ─────────────────────────────

close_code_t message_t::close_code() const {
  if (opcode != opcode_t::Close) return close_code_t::NoStatus;
  if (payload.size() < 2) return close_code_t::NoStatus;
  return close_code_t(uint16_t(uint8_t(payload[0])) << 8 | uint8_t(payload[1]));
}

std::string_view message_t::close_reason() const {
  if (opcode != opcode_t::Close || payload.size() <= 2) return {};
  return payload.substr(2);
}

// ───────────────────────────── session_t ─────────────────────────────

session_t::session_t(context_t& ctx, tls::transport_t tr, role_t role,
                     http::buffer_pool_t& pool, timer_wheel_t& wheel,
                     const session_options_t& opt, std::string_view leftover,
                     http::connection_metrics_t* metrics)
  : ctx_(ctx), tr_(std::move(tr)), role_(role), pool_(pool), wheel_(wheel),
    opt_(opt), metrics_(metrics), decoder_(role), read_canceler_(ctx) {
  // The leftover must fit: it arrived in the handshake buffer, which the
  // receive window normally exceeds — but nothing forces that, so size for it.
  auto lease = pool_.acquire(
      uint32_t(std::max<uint64_t>(opt_.recv_buffer_bytes, leftover.size())));
  in_.reset(std::move(lease));
  if (!leftover.empty()) {
    std::memcpy(in_.writable().data(), leftover.data(), leftover.size());
    in_.commit(uint32_t(leftover.size()));
  }

  timer_.owner = this;
  timer_.on_expire = [](void* owner) {
    auto* self = static_cast<session_t*>(owner);
    self->timed_out_ = true;
    // reads only, the same discipline as the http connection: an interrupted
    // writev is a truncated frame, and a truncated frame desynchronizes the
    // stream far worse than a late timeout
    self->read_canceler_.cancel();
  };

  if (role_ == role_t::Client) {
    // xorshift seeds the frame masks; zero is a degenerate state the generator
    // never leaves, so re-draw on the one value that would also read as
    // "unmasked" to write_frame_header
    do {
      random_bytes(&mask_seed_, sizeof(mask_seed_));
    } while (mask_seed_ == 0);
  }
}

session_t::~session_t() {
  wheel_.cancel(timer_);
  // finish() closes politely; reaching the destructor unfinished means the
  // frame was destroyed mid-flight, where ceremony no longer matters
  if (!finished_ && tr_.native_fd() >= 0) tr_.abandon(ctx_);
}

bool session_t::is_open() const {
  return state_ == state_t::Open && !finished_;
}

void session_t::request_close() {
  closing_ = true;
  read_canceler_.cancel();
}

// ─────────────────────────────── io helpers ───────────────────────────────

coro_t<expected<uint32_t>> session_t::fill() {
  auto w = in_.writable();
  if (w.empty()) {
    // Legal here because nothing in in_ outlives the fill: frames are copied
    // out (masked frames) or handed to the caller before the next read, and a
    // delivered message's "valid until the next recv" window closed at step
    // entry. Mid-fragment bytes live in msg_, not here.
    in_.compact();
    w = in_.writable();
    if (w.empty()) co_return unexpected(E2BIG);  // unreachable; the window is never pinned
  }
  if (!close_wait_ && opt_.idle_timeout.count() > 0 && !timed_out_) {
    // the wheel owns the deadline — one SQE per read, not two
    wheel_.arm(timer_, opt_.idle_timeout);
  }
  expected<size_t> n;
  if (!tr_.is_tls()) {
    auto r = co_await with_cancel(ctx_, tr_.plain_recv(ctx_, w.data(), w.size()),
                                  read_canceler_);
    n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
  } else {
    n = co_await with_cancel(ctx_, tr_.recv(ctx_, w.data(), w.size()), read_canceler_);
  }
  if (!n) {
    if (timed_out_) co_return unexpected(ETIMEDOUT);
    co_return unexpected(n.error());
  }
  in_.commit(uint32_t(*n));
  co_return uint32_t(*n);
}

coro_t<expected<void>> session_t::writev_span(struct iovec* iov, uint32_t n) {
  uint32_t done = 0;
  while (done < n) {
    if (metrics_) ++metrics_->writev_calls;
    // no cancellation on the write path: a partial frame on the wire is a
    // desynchronized stream (see the timer comment)
    expected<size_t> w;
    if (!tr_.is_tls()) {
      auto r = co_await tr_.plain_writev(ctx_, iov + done, n - done);
      w = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
    } else {
      w = co_await tr_.writev(ctx_, iov + done, n - done);
    }
    if (!w) co_return unexpected(w.error());
    if (*w == 0) co_return unexpected(ECONNRESET);
    uint32_t remaining = uint32_t(*w);
    for (size_t i = done; i < n && remaining > 0; ++i) {
      uint32_t take = remaining < iov[i].iov_len ? remaining : iov[i].iov_len;
      iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + take;
      iov[i].iov_len -= take;
      remaining -= take;
    }
    while (done < n && iov[done].iov_len == 0) ++done;
    if (done < n && metrics_) ++metrics_->writev_partial;
  }
  co_return expected<void>{};
}

uint32_t session_t::next_mask_key() {
  // xorshift32: full-period on a nonzero state, which the constructor
  // guarantees; the loop only guards the one value that would serialize as
  // "unmasked"
  uint32_t x = mask_seed_;
  do {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
  } while (x == 0);
  mask_seed_ = x;
  return x;
}

// ─────────────────────────────── sending ───────────────────────────────

coro_t<expected<void>> session_t::send(std::string_view payload, opcode_t op) {
  if (!opcode_is_data(op)) {
    co_return websocket_unexpected(websocket_error_t::InvalidOpcode);
  }
  if (state_ != state_t::Open) {
    co_return websocket_unexpected(websocket_error_t::Closed);
  }

  char hdr[kMaxFrameHeaderLen];
  struct iovec iov[2];
  iov[0] = {hdr, 0};
  if (role_ == role_t::Client) {
    // Masking transforms the payload bytes, so the caller's buffer cannot be
    // the one on the wire; out_ is the per-session scratch that absorbs that.
    uint32_t key = next_mask_key();
    iov[0].iov_len = write_frame_header(hdr, true, op, payload.size(), key);
    if (!payload.empty()) {
      auto ok = out_.grow(pool_, uint32_t(payload.size()));
      if (!ok) co_return unexpected(ok.error());
      std::memcpy(out_.data(), payload.data(), payload.size());
      apply_mask(key, out_.data(), payload.size());
      iov[1] = {out_.data(), payload.size()};
      co_return co_await writev_span(iov, 2);
    }
    co_return co_await writev_span(iov, 1);
  }

  // Server: the payload travels as its own iovec, zero copies
  iov[0].iov_len = write_frame_header(hdr, true, op, payload.size(), 0);
  if (payload.empty()) co_return co_await writev_span(iov, 1);
  iov[1] = {const_cast<char*>(payload.data()), payload.size()};
  co_return co_await writev_span(iov, 2);
}

coro_t<expected<void>> session_t::send_control(opcode_t op, std::string_view payload) {
  payload = payload.substr(0, kMaxControlPayload);
  char hdr[kMaxFrameHeaderLen];
  struct iovec iov[2];
  iov[0] = {hdr, 0};
  if (role_ == role_t::Client) {
    uint32_t key = next_mask_key();
    iov[0].iov_len = write_frame_header(hdr, true, op, payload.size(), key);
    // masked bytes need a mutable copy; a control payload is at most 125, so
    // scratch space lives on the frame (valid across suspension)
    char body[kMaxControlPayload];
    std::memcpy(body, payload.data(), payload.size());
    apply_mask(key, body, payload.size());
    iov[1] = {body, payload.size()};
  } else {
    iov[0].iov_len = write_frame_header(hdr, true, op, payload.size(), 0);
    iov[1] = {const_cast<char*>(payload.data()), payload.size()};
  }
  co_return co_await writev_span(iov, payload.empty() ? 1 : 2);
}

coro_t<expected<void>> session_t::ping(std::string_view payload) {
  if (state_ != state_t::Open) {
    co_return websocket_unexpected(websocket_error_t::Closed);
  }
  co_return co_await send_control(opcode_t::Ping, payload);
}

coro_t<expected<void>> session_t::close(close_code_t code, std::string_view reason) {
  if (state_ != state_t::Open) co_return expected<void>{};  // idempotent by design
  if (code == close_code_t::NoStatus || code == close_code_t::Abnormal ||
      !close_code_valid(uint16_t(code))) {
    co_return websocket_unexpected(websocket_error_t::InvalidCloseCode);
  }

  char body[kMaxControlPayload];
  body[0] = char(uint16_t(code) >> 8);
  body[1] = char(uint16_t(code));
  reason = reason.substr(0, kMaxControlPayload - 2);
  std::memcpy(body + 2, reason.data(), reason.size());
  auto ok = co_await send_control(opcode_t::Close, std::string_view(body, 2 + reason.size()));
  if (!ok) co_return ok;
  state_ = state_t::CloseSent;
  co_return expected<void>{};
}

coro_t<expected<message_t>> session_t::fail_protocol(websocket_error_t err,
                                                     close_code_t code) {
  if (metrics_) ++metrics_->protocol_errors;
  SPDLOG_DEBUG("ws: protocol violation ({}), closing with {}",
               websocket_error_name(int(err)), uint16_t(code));
  if (state_ == state_t::Open && !timed_out_) {
    co_await close(code);
    // the peer knows it erred now; nothing further will be read
  }
  co_return websocket_unexpected(err);
}

// ─────────────────────────────── receiving ───────────────────────────────

// one step of the receive machinery: consume exactly one interesting event.
// Data completes a message only when `deliver` (finish() drains instead);
// Close always produces its message, since both recv() and finish() key on it.
coro_t<expected<message_t>> session_t::step(bool deliver) {
  while (true) {
    // ── parse the next frame header ──
    if (!frame_open_) {
      auto window = in_.view(in_.read_pos(), in_.readable());
      frame_t f;
      auto r = decoder_.parse(window, f);
      switch (r) {
        case frame_decoder_t::result_t::Error: {
          auto err = decoder_.error();
          if (err.domain == error_domain::Websocket) {
            if (websocket_error_t(err.code) == websocket_error_t::MessageTooLarge) {
              co_return co_await fail_protocol(websocket_error_t(err.code),
                                               close_code_t::MessageTooBig);
            }
            co_return co_await fail_protocol(websocket_error_t(err.code),
                                             close_code_t::ProtocolError);
          }
          co_return unexpected(err);
        }
        case frame_decoder_t::result_t::NeedMore: {
          auto got = co_await fill();
          if (!got) co_return unexpected(got.error());
          if (*got == 0) {
            // EOF mid-bookkeeping is an abnormal closure (RFC 6455 §7.1.7);
            // there is no Close to deliver
            co_return unexpected(ECONNRESET);
          }
          continue;
        }
        case frame_decoder_t::result_t::Frame:
          break;
      }

      // message-level rules the stateless decoder cannot know (RFC 6455 §5.4)
      if (opcode_is_data(f.opcode)) {
        if (fragment_open_) {
          co_return co_await fail_protocol(websocket_error_t::ContinuationExpected,
                                           close_code_t::ProtocolError);
        }
        if (f.payload_len > opt_.max_message_bytes) {
          co_return co_await fail_protocol(websocket_error_t::MessageTooLarge,
                                           close_code_t::MessageTooBig);
        }
        // a new message starts: previous contents are either delivered (the
        // view expired with that step) or never existed
        msg_.clear();
      } else if (f.opcode == opcode_t::Continue) {
        if (!fragment_open_) {
          co_return co_await fail_protocol(websocket_error_t::ContinuationUnexpected,
                                           close_code_t::ProtocolError);
        }
        if (msg_.size() + f.payload_len > opt_.max_message_bytes) {
          co_return co_await fail_protocol(websocket_error_t::MessageTooLarge,
                                           close_code_t::MessageTooBig);
        }
      }

      in_.consume(f.header_len);
      cur_ = f;
      cur_remaining_ = f.payload_len;
      frame_open_ = true;
    }

    // ── consume the payload ──
    const uint64_t consumed = cur_.payload_len - cur_remaining_;
    std::string_view window = in_.view(in_.read_pos(), in_.readable());

    if (opcode_is_control(cur_.opcode)) {
      // a control payload is at most 125 bytes: never fed in runs, so a short
      // window means wait for the rest
      if (window.size() < cur_remaining_) {
        auto got = co_await fill();
        if (!got) co_return unexpected(got.error());
        if (*got == 0) co_return unexpected(ECONNRESET);
        continue;
      }
      std::string_view payload = window.substr(0, size_t(cur_remaining_));
      if (cur_.masked) {
        apply_mask(cur_.mask_key, const_cast<char*>(payload.data()), payload.size(),
                   consumed);
      }
      in_.consume(uint32_t(cur_remaining_));
      cur_remaining_ = 0;
      frame_open_ = false;

      switch (cur_.opcode) {
        case opcode_t::Ping: {
          // the Pong is the same payload; it must leave this buffer before
          // the next frame overwrites it
          char echo[kMaxControlPayload];
          std::memcpy(echo, payload.data(), payload.size());
          auto ok = co_await send_control(opcode_t::Pong,
                                          std::string_view(echo, payload.size()));
          if (!ok) co_return unexpected(ok.error());
          continue;
        }
        case opcode_t::Pong:
          continue;   // no bookkeeping: pings are one-way, on-demand
        case opcode_t::Close: {
          if (payload.size() == 1) {
            // a Close body is empty or code+reason, never a bare code byte
            co_return co_await fail_protocol(websocket_error_t::InvalidCloseCode,
                                             close_code_t::ProtocolError);
          }
          if (payload.size() >= 2) {
            uint16_t code = uint16_t(uint8_t(payload[0])) << 8 | uint8_t(payload[1]);
            if (!close_code_valid(code)) {
              co_return co_await fail_protocol(websocket_error_t::InvalidCloseCode,
                                               close_code_t::ProtocolError);
            }
          }
          bool echo_needed = state_ == state_t::Open;
          state_ = state_t::CloseReceived;
          if (echo_needed) {
            // RFC 6455 §5.5.1: echo the frame verbatim; payload echoes ride
            // the same stack-copy discipline as the pong above
            char echo[kMaxControlPayload];
            std::memcpy(echo, payload.data(), payload.size());
            auto ok = co_await send_control(opcode_t::Close,
                                            std::string_view(echo, payload.size()));
            if (!ok) co_return unexpected(ok.error());
          }
          co_return message_t{opcode_t::Close, payload};
        }
        default:
          co_return websocket_unexpected(websocket_error_t::InvalidOpcode);
      }
    }

    // data frame: consume what is buffered, in runs when the window is short
    if (window.empty()) {
      auto got = co_await fill();
      if (!got) co_return unexpected(got.error());
      if (*got == 0) co_return unexpected(ECONNRESET);
      continue;
    }
    const uint32_t run = uint32_t(std::min<uint64_t>(window.size(), cur_remaining_));
    std::string_view part = window.substr(0, run);
    if (cur_.masked) {
      apply_mask(cur_.mask_key, const_cast<char*>(part.data()), part.size(), consumed);
    }

    // zero-copy fast path: the frame's whole payload arrived as this single
    // run and no fragment is open — deliver a view into the receive window
    // without touching the aggregation buffer. Demanding the payload be
    // unconsumed so far (run == payload_len, not merely cur_remaining_ ==
    // run) is what keeps the final run of a multi-run message off this path:
    // everything before it is already in msg_.
    const bool direct = !fragment_open_ && run == cur_.payload_len;
    if (!direct) {
      auto ok = msg_.grow(pool_, msg_.size() + run);
      if (!ok) co_return unexpected(ok.error());
      auto appended = msg_.append(part.data(), run);
      if (!appended) co_return unexpected(appended.error());
    }
    in_.consume(run);
    cur_remaining_ -= run;
    if (cur_remaining_ > 0) continue;

    // message complete
    frame_open_ = false;
    const opcode_t op = fragment_open_ ? frag_opcode_ : cur_.opcode;
    message_t msg;
    msg.opcode = op;
    if (direct) {
      msg.payload = part;
    } else {
      msg.payload = msg_.view();
      fragment_open_ = false;
    }
    if (!cur_.fin) {
      // fragmented message starts or continues; remember the kind for the
      // closing frame and keep aggregating
      if (!fragment_open_) {
        fragment_open_ = true;
        frag_opcode_ = cur_.opcode;
      }
      if (direct) {
        // a non-final single frame cannot ride the fast path after all:
        // its bytes must outlive the receive window
        auto ok = msg_.grow(pool_, run);
        if (!ok) co_return unexpected(ok.error());
        auto appended = msg_.append(part.data(), run);
        if (!appended) co_return unexpected(appended.error());
      }
      continue;
    }
    if (!deliver) continue;
    co_return msg;
  }
}

coro_t<expected<message_t>> session_t::recv() {
  if (state_ == state_t::CloseReceived || finished_) {
    co_return websocket_unexpected(websocket_error_t::Closed);
  }
  co_return co_await step(true);
}

// ─────────────────────────────── finishing ───────────────────────────────

coro_t<void> session_t::finish() {
  if (finished_ || tr_.native_fd() < 0) {
    finished_ = true;
    co_return;
  }

  // 1. get our Close out unless the transport already failed us
  if (state_ == state_t::Open && !timed_out_) {
    auto code = closing_ ? close_code_t::GoingAway : close_code_t::Normal;
    auto ok = co_await close(code);
    (void)ok;  // a failed close write still ends here; the peer is already gone
  }

  // 2. wait out the peer's Close, bounded; a peer that never answers must not
  //    hold the connection slot (idle_timeout stays out of the way: this
  //    window has its own budget, marked by close_wait_)
  if (state_ != state_t::CloseReceived && !timed_out_) {
    close_wait_ = true;
    timed_out_ = false;
    wheel_.arm(timer_, opt_.close_timeout);
    while (state_ != state_t::CloseReceived && !timed_out_) {
      auto msg = co_await step(false);
      if (!msg) break;   // transport, timeout or protocol error: stop waiting
    }
    close_wait_ = false;
    wheel_.cancel(timer_);
  }

  // 3. half-close and drain briefly. An outright close can surface as RST at
  //    the peer and eat the Close we just sent; a few short reads give the
  //    peer's FIN time to arrive (the http connection's shutdown_gracefully
  //    makes the same trade).
  if (!timed_out_) {
    auto sd = co_await tr_.shutdown_write(ctx_);
    (void)sd;
    char scratch[512];
    wheel_.arm(timer_, std::chrono::milliseconds(200));
    for (int i = 0; i < 4 && !timed_out_; ++i) {
      expected<size_t> n;
      if (!tr_.is_tls()) {
        auto r = co_await with_cancel(ctx_, tr_.plain_recv(ctx_, scratch, sizeof(scratch)),
                                      read_canceler_);
        n = r ? expected<size_t>(static_cast<size_t>(*r)) : unexpected(r.error());
      } else {
        n = co_await with_cancel(ctx_, tr_.recv(ctx_, scratch, sizeof(scratch)),
                                 read_canceler_);
      }
      if (!n || *n == 0) break;
    }
    wheel_.cancel(timer_);
  }

  tr_.abandon(ctx_);
  finished_ = true;
  co_return;
}

} // namespace cornet::websocket
