#include "cornet/http/common/parser.h"

#include <llhttp.h>

#include <spdlog/spdlog.h>

namespace cornet::http {

// The inline byte array must be able to hold a real llhttp_t. Checking it here
// turns a library upgrade that grows the struct into a compile error instead of
// silent memory corruption. 96 bytes today; the slack is deliberate.
static_assert(sizeof(llhttp_t) <= parser_t::kStateSize,
              "llhttp_t outgrew parser_t::kStateSize; raise it");
static_assert(alignof(llhttp_t) <= 16, "llhttp_t needs stricter alignment than state_ provides");

namespace {

/**
 * @brief whether llhttp is currently in the trailer section of a chunked message.
 *
 * Trailers arrive through the same header callbacks as the real header section, but
 * their bytes live in the body region of the receive buffer — which is rewound and
 * refilled as the body streams past. Recording them as offsets into that region is
 * not merely useless, it is unsound: a trailer split across two reads has its first
 * run overwritten by the second, and the resulting entry is a name and value made of
 * body bytes. That is a header-injection primitive handed to whoever writes the body,
 * so trailers are dropped instead of recorded.
 *
 * Nothing in cornet consumes trailers, and neither side advertises `TE: trailers`, so
 * ignoring them is also the correct protocol answer. Supporting them would mean
 * storing them out of the body region and keeping them in their own namespace rather
 * than merged into the header table — a feature, not a fix.
 */
inline bool in_trailer(const llhttp_t* p) { return (p->flags & F_TRAILING) != 0; }

inline llhttp_t* st(unsigned char* storage) {
  return reinterpret_cast<llhttp_t*>(storage);
}
inline const llhttp_t* st(const unsigned char* storage) {
  return reinterpret_cast<const llhttp_t*>(storage);
}

} // namespace

/**
 * @brief llhttp callback bridge.
 *
 * A separate struct so the callbacks can name llhttp's types (which the header
 * must not expose) while still reaching parser_t's privates through friendship.
 * All of them are plain statics, which lets the settings table below be a single
 * shared constant: llhttp only requires that settings outlive the parser, and
 * copying 200 bytes of function pointers into every connection would waste both
 * memory and cache for no benefit.
 */
struct parser_callbacks_t {
  static parser_t* self(llhttp_t* p) { return static_cast<parser_t*>(p->data); }

  static int on_message_begin(llhttp_t* p) {
    auto* s = self(p);
    s->headers_->clear();
    s->pending_ = {};
    s->pending_valid_ = false;
    s->skip_trailer_ = false;
    s->target_off_ = 0;
    s->target_len_ = 0;
    s->target_spilled_ = false;
    s->body_received_ = 0;
    s->headers_signal_ = false;
    s->message_done_ = false;
    if (s->head_) s->head_->begin_message();
    return 0;
  }

  static int on_url(llhttp_t* p, const char* at, size_t len) {
    auto* s = self(p);
    auto ok = s->accumulate(s->target_off_, s->target_len_, s->target_spilled_,
                            at, uint32_t(len), true);
    if (!ok) {
      s->error_ = ok.error();
      return -1;
    }
    return 0;
  }

  static int on_status(llhttp_t* p, const char* at, size_t len) {
    (void)p; (void)at; (void)len;
    return 0;
  }

  static int on_header_field(llhttp_t* p, const char* at, size_t len) {
    auto* s = self(p);
    bool trailer = in_trailer(p);
    if (trailer && (s->limits_.max_trailers == 0 || s->skip_trailer_)) return 0;

    // A header name is never folded, so within the header section its runs are always
    // adjacent in the header buffer, and spilling is not allowed there so that a
    // broken assumption fails loudly instead of producing a silently wrong name. A
    // trailer's bytes live in the body region, which the reader rewinds, so those are
    // copied out from the first run instead.
    auto ok = s->accumulate(s->pending_.name_off, s->pending_.name_len,
                            s->pending_.name_spilled, at, uint32_t(len),
                            /*allow_spill=*/trailer, /*must_copy=*/trailer);
    if (!ok) {
      if (trailer) {
        // No room to copy it: drop this trailer rather than fail a message that is
        // otherwise complete.
        s->skip_trailer_ = true;
        s->pending_ = {};
        s->pending_valid_ = false;
        return 0;
      }
      s->error_ = ok.error();
      return -1;
    }
    s->pending_.trailer = trailer;
    s->pending_valid_ = true;
    return 0;
  }

  static int on_header_field_complete(llhttp_t* p) {
    auto* s = self(p);
    if (!s->pending_valid_) return 0;   // a dropped trailer
    auto name = s->pending_.name_spilled
                    ? s->spill_->view(s->pending_.name_off, s->pending_.name_len)
                    : s->head_->view(s->pending_.name_off, s->pending_.name_len);
    s->pending_.field = field_from_name(name);
    return 0;
  }

  static int on_header_value(llhttp_t* p, const char* at, size_t len) {
    auto* s = self(p);
    bool trailer = in_trailer(p);
    if (trailer && (s->limits_.max_trailers == 0 || s->skip_trailer_)) return 0;

    auto ok = s->accumulate(s->pending_.value_off, s->pending_.value_len,
                            s->pending_.spilled, at, uint32_t(len),
                            /*allow_spill=*/true, /*must_copy=*/trailer);
    if (!ok) {
      if (trailer) {
        s->skip_trailer_ = true;
        s->pending_ = {};
        s->pending_valid_ = false;
        return 0;
      }
      s->error_ = ok.error();
      return -1;
    }
    return 0;
  }

  static int on_header_value_complete(llhttp_t* p) {
    auto* s = self(p);
    // Whatever happened to the trailer we were skipping, the next one starts clean.
    s->skip_trailer_ = false;
    auto ok = s->finish_header();
    if (!ok) {
      s->error_ = ok.error();
      return -1;
    }
    return 0;
  }

  static int on_headers_complete(llhttp_t* p) {
    auto* s = self(p);

    s->method_ = method_from_raw(llhttp_get_method(p));
    s->status_code_ = uint16_t(llhttp_get_status_code(p));
    uint8_t major = llhttp_get_http_major(p);
    uint8_t minor = llhttp_get_http_minor(p);
    s->version_ = (major == 1 && minor == 1) ? version_t::Http11
                : (major == 1 && minor == 0) ? version_t::Http10
                                             : version_t::Unknown;
    s->keep_alive_ = llhttp_should_keep_alive(p) != 0;
    s->upgrade_ = llhttp_get_upgrade(p) != 0;

    s->has_content_length_ = s->headers_->has(field_t::ContentLength);
    s->content_length_ = p->content_length;
    s->chunked_ = s->headers_->contains_token(field_t::TransferEncoding, "chunked");
    s->expects_continue_ = s->headers_->contains_token(field_t::Expect, "100-continue");

    if (s->has_content_length_ && s->content_length_ > s->limits_.max_body_bytes) {
      s->error_ = http_error(http_error_t::BodyTooLarge);
      return -1;
    }

    if (s->headers_end_message()) {
      // llhttp reads 1 as "no body follows": it fires on_message_complete right
      // away instead of waiting for bytes that will never come. Pausing here
      // instead — the request-side behaviour below — would leave llhttp expecting
      // a body, and on a keep-alive connection the next response would be
      // swallowed as this one's body.
      s->headers_signal_ = true;
      return 1;
    }

    // Hand control back before any body byte is consumed. This is the only
    // moment at which the caller can still route the request, answer
    // 100-continue, refuse an oversized body without reading it, or start a
    // handler that will consume the body itself.
    s->headers_signal_ = true;
    return HPE_PAUSED;
  }

  static int on_body(llhttp_t* p, const char* at, size_t len) {
    auto* s = self(p);
    s->body_received_ += len;
    if (s->body_received_ > s->limits_.max_body_bytes) {
      s->error_ = http_error(http_error_t::BodyTooLarge);
      return -1;
    }

    if (s->body_sink_) {
      // Aggregating: copy the run to the write cursor. For a chunked body the
      // runs are separated by chunk-size lines, so this also squeezes those gaps
      // out and keeps body() contiguous.
      auto ok = s->body_sink_->append(at, uint32_t(len));
      if (!ok) {
        s->error_ = ok.error();
        return -1;
      }
      return 0;
    }

    // Streaming: publish the run and stop, so the reader can consume it before
    // the buffer is reused.
    s->body_run_ = at;
    s->body_run_len_ = uint32_t(len);
    s->body_paused_ = true;
    return HPE_PAUSED;
  }

  static int on_message_complete(llhttp_t* p) {
    auto* s = self(p);
    s->message_done_ = true;
    // Pausing here is what makes pipelining exact: llhttp_get_error_pos() then
    // points at the first byte after this message, which is where the next
    // request begins.
    return HPE_PAUSED;
  }
};

namespace {

/**
 * @brief the one and only settings table.
 * Static and const: llhttp only needs settings to outlive the parser, so every
 * connection can share this.
 */
const llhttp_settings_t& shared_settings() {
  static const llhttp_settings_t settings = [] {
    llhttp_settings_t s{};
    llhttp_settings_init(&s);
    s.on_message_begin = &parser_callbacks_t::on_message_begin;
    s.on_url = &parser_callbacks_t::on_url;
    s.on_status = &parser_callbacks_t::on_status;
    s.on_header_field = &parser_callbacks_t::on_header_field;
    s.on_header_field_complete = &parser_callbacks_t::on_header_field_complete;
    s.on_header_value = &parser_callbacks_t::on_header_value;
    s.on_header_value_complete = &parser_callbacks_t::on_header_value_complete;
    s.on_headers_complete = &parser_callbacks_t::on_headers_complete;
    s.on_body = &parser_callbacks_t::on_body;
    s.on_message_complete = &parser_callbacks_t::on_message_complete;
    return s;
  }();
  return settings;
}

} // namespace

// ───────────────────────────── lifecycle ─────────────────────────────

const char* parser_t::to_string(result_t r) {
  switch (r) {
    case result_t::NeedMore:     return "NeedMore";
    case result_t::HeadersReady: return "HeadersReady";
    case result_t::MessageReady: return "MessageReady";
    case result_t::BodyPaused:   return "BodyPaused";
    case result_t::Upgrade:      return "Upgrade";
    case result_t::Error:        return "Error";
  }
  return "?";
}

parser_t::parser_t(type_t type) : type_(type) {
  auto* p = st(state_);
  llhttp_init(p, type == type_t::Request ? HTTP_REQUEST : HTTP_RESPONSE, &shared_settings());
  p->data = this;
  set_limits(limits_);
}

parser_t::~parser_t() = default;

void parser_t::bind(head_buffer_t& head, spill_buffer_t& spill, headers_t& headers) {
  head_ = &head;
  spill_ = &spill;
  headers_ = &headers;
  headers.bind(&head, &spill);
  // Any feed window still recorded points into the buffers being replaced. The
  // client rebinds per exchange (it hands the old buffers to the response it just
  // finished), so leaving the window behind would make has_pending_input() compare
  // pointers into memory that is no longer ours.
  feed_pos_ = nullptr;
  feed_end_ = nullptr;
  consumed_offset_ = 0;
}

void parser_t::set_limits(const parser_limits_t& limits) {
  limits_ = limits;
  auto* p = st(state_);
  // Everything lenient is off by default. Each of these switches re-admits a
  // request-smuggling variant: disagreeing Content-Length and Transfer-Encoding,
  // duplicate Content-Length, sloppy chunk lengths. A proxy in front of this
  // server may interpret such a message differently than we would, which is
  // exactly how a smuggled request slips through.
  llhttp_set_lenient_headers(p, limits_.lenient_headers ? 1 : 0);
  llhttp_set_lenient_chunked_length(p, limits_.lenient_chunked_length ? 1 : 0);
  llhttp_set_lenient_keep_alive(p, limits_.lenient_keep_alive ? 1 : 0);
}

void parser_t::reset() {
  auto* p = st(state_);
  llhttp_reset(p);
  p->data = this;

  if (head_) head_->end_message();

  pending_ = {};
  pending_valid_ = false;
  skip_trailer_ = false;
  target_off_ = 0;
  target_len_ = 0;
  target_spilled_ = false;
  body_run_ = nullptr;
  body_run_len_ = 0;
  method_ = method_t::Unknown;
  version_ = version_t::Unknown;
  status_code_ = 0;
  content_length_ = 0;
  body_received_ = 0;
  consumed_ = 0;
  has_content_length_ = false;
  chunked_ = false;
  keep_alive_ = true;
  upgrade_ = false;
  expects_continue_ = false;
  headers_signal_ = false;
  message_done_ = false;
  body_paused_ = false;
  zero_fed_ = false;
  error_ = {};
  if (spill_) spill_->clear();
  // feed_pos_/feed_end_ survive on purpose: whatever the peer pipelined behind
  // this message is still sitting in the window and must be parsed next.
}

bool parser_t::headers_end_message() const {
  if (type_ != type_t::Response) return false;
  if (status_forbids_body(status_t(status_code_))) return true;
  return response_to_ == method_t::Head;
}

std::string_view parser_t::target() const {
  if (target_len_ == 0) return {};
  if (target_spilled_) return spill_ ? spill_->view(target_off_, target_len_) : std::string_view{};
  return head_ ? head_->view(target_off_, target_len_) : std::string_view{};
}

// ─────────────────────── header accumulation ───────────────────────

expected<void> parser_t::accumulate(uint32_t& off, uint16_t& len, bool& spilled,
                                    const char* at, uint32_t n, bool allow_spill,
                                    bool must_copy) {
  if (n == 0) return {};

  if (len == 0) {
    // first run
    if (spilled || must_copy) {
      spilled = true;
      auto o = spill_->put(at, n);
      if (!o) return unexpected(o.error());
      off = *o;
    } else {
      off = head_->offset_of(at);
    }
    len = uint16_t(n);
    return {};
  }

  if (!spilled) {
    // llhttp splits a value when it straddles a recv boundary. Because the whole
    // header section lands in one buffer that never moves during a message, the
    // two runs are adjacent and the view simply gets longer — no copy.
    uint32_t here = head_->offset_of(at);
    if (off + len == here) {
      if (uint32_t(len) + n > 0xffff) return http_unexpected(http_error_t::HeaderTooLarge);
      len = uint16_t(len + n);
      return {};
    }
    if (!allow_spill) {
      // Only reachable if the no-move invariant were broken.
      return http_unexpected(http_error_t::HeaderTooLarge);
    }
    // Not adjacent: obsolete line folding and similar oddities. Copy what we have
    // so far into the spill buffer, then continue there.
    auto existing = head_->view(off, len);
    auto o = spill_->put(existing.data(), uint32_t(existing.size()));
    if (!o) return unexpected(o.error());
    off = *o;
    spilled = true;
  }

  auto ok = spill_->extend(at, n);
  if (!ok) return unexpected(ok.error());
  if (uint32_t(len) + n > 0xffff) return http_unexpected(http_error_t::HeaderTooLarge);
  len = uint16_t(len + n);
  return {};
}

expected<void> parser_t::finish_header() {
  if (!pending_valid_) return {};
  auto entry = pending_;
  pending_ = {};
  pending_valid_ = false;

  if (entry.trailer) {
    // Trailers are best effort: one we will not keep is dropped, never an error. A
    // peer being sloppy about them should not cost a message that is otherwise fine.
    if (headers_->trailer_count() >= limits_.max_trailers) return {};
    // RFC 9110 §6.5.1: framing, routing, request modifiers, authentication and
    // response control data were all interpreted long before this arrived.
    if (field_forbidden_in_trailer(entry.field)) return {};
    (void)headers_->add(entry, limits_.max_trailers);
    return {};
  }

  return headers_->add(entry, limits_.max_headers);
}

// ────────────────────────────── driving ──────────────────────────────

parser_t::result_t parser_t::pump() {
  auto* p = st(state_);

  if (feed_pos_ >= feed_end_) {
    // The window is empty, but the message may still be one state transition from
    // done: a request without a body reaches on_message_complete only after the
    // headers pause is resumed, and there are no bytes left to hand over. llhttp
    // accepts a zero-length execute for exactly this, and zero_fed_ keeps it from
    // happening twice in a row.
    if (zero_fed_) {
      consumed_offset_ = head_ ? head_->offset_of(feed_pos_) : 0;
      return result_t::NeedMore;
    }
    zero_fed_ = true;
  } else {
    zero_fed_ = false;
  }

  headers_signal_ = false;
  body_paused_ = false;
  message_done_ = false;

  auto err = llhttp_execute(p, feed_pos_, size_t(feed_end_ - feed_pos_));

  if (err == HPE_OK) {
    consumed_ += uint32_t(feed_end_ - feed_pos_);
    feed_pos_ = feed_end_;
    consumed_offset_ = head_->offset_of(feed_pos_);
    return result_t::NeedMore;
  }

  if (err == HPE_PAUSED || err == HPE_PAUSED_UPGRADE) {
    const char* stop = llhttp_get_error_pos(p);
    if (stop) {
      consumed_ += uint32_t(stop - feed_pos_);
      feed_pos_ = stop;
    }
    consumed_offset_ = head_->offset_of(feed_pos_);

    if (err == HPE_PAUSED_UPGRADE) {
      llhttp_resume_after_upgrade(p);
      return result_t::Upgrade;
    }
    // Clear the pause now; parsing only advances on the next llhttp_execute, so
    // this does not consume anything the caller has not asked for yet.
    llhttp_resume(p);

    if (message_done_) {
      if (head_) head_->end_message();
      return upgrade_ ? result_t::Upgrade : result_t::MessageReady;
    }
    if (headers_signal_) return result_t::HeadersReady;
    if (body_paused_) return result_t::BodyPaused;
    // A pause we did not ask for should not happen; treat it as a protocol error
    // rather than looping forever.
    error_ = error_t{int(HPE_PAUSED), error_domain::Http};
    return result_t::Error;
  }

  // error_ may already hold a more specific reason set by a callback
  if (!error_) {
    error_ = error_t{int(err), error_domain::Http};
  }
  SPDLOG_DEBUG("http parse error: {} ({})", llhttp_errno_name(err), llhttp_get_error_reason(p));
  return result_t::Error;
}

parser_t::result_t parser_t::execute(uint32_t off, uint32_t len) {
  if (!head_ || !headers_) {
    error_ = http_error(http_error_t::InvalidState);
    return result_t::Error;
  }
  feed_pos_ = head_->base() + off;
  feed_end_ = feed_pos_ + len;
  zero_fed_ = false;
  return pump();
}

parser_t::result_t parser_t::resume() {
  if (!head_ || !headers_) {
    error_ = http_error(http_error_t::InvalidState);
    return result_t::Error;
  }
  return pump();
}

parser_t::result_t parser_t::finish() {
  auto* p = st(state_);
  auto err = llhttp_finish(p);
  if (err == HPE_OK) {
    if (head_) head_->end_message();
    return result_t::MessageReady;
  }
  if (err == HPE_PAUSED && message_done_) {
    llhttp_resume(p);
    if (head_) head_->end_message();
    return result_t::MessageReady;
  }
  if (!error_) error_ = error_t{int(err), error_domain::Http};
  return result_t::Error;
}

} // namespace cornet::http
