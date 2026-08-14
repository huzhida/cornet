#include "cornet/http/message.h"

namespace cornet::http {

// ───────────────────────────── query_t ─────────────────────────────

void query_t::iterator::advance() {
  if (pos_ > raw_.size()) {
    cur_ = {};
    next_ = pos_;
    return;
  }
  if (pos_ == raw_.size()) {
    // one past the last pair: become end()
    cur_ = {};
    pos_ = raw_.size() + 1;
    next_ = pos_;
    return;
  }
  size_t amp = raw_.find('&', pos_);
  auto pair = raw_.substr(pos_, amp == std::string_view::npos ? std::string_view::npos : amp - pos_);
  next_ = amp == std::string_view::npos ? raw_.size() + 1 : amp + 1;

  size_t eq = pair.find('=');
  if (eq == std::string_view::npos) {
    cur_ = {pair, {}};
  } else {
    cur_ = {pair.substr(0, eq), pair.substr(eq + 1)};
  }
}

std::string_view query_t::get(std::string_view key) const {
  for (auto e : *this) {
    if (e.key == key) return e.value;
  }
  return {};
}

// ──────────────────────────── request_t ────────────────────────────

std::string_view request_t::path() const {
  if (path_cached_) return path_cache_;
  auto t = target();
  auto q = t.find('?');
  path_cache_ = q == std::string_view::npos ? t : t.substr(0, q);
  path_cached_ = true;
  return path_cache_;
}

query_t request_t::query() const {
  auto t = target();
  auto q = t.find('?');
  if (q == std::string_view::npos) return query_t{};
  auto rest = t.substr(q + 1);
  // a fragment is not supposed to reach the server, but strip it defensively
  if (auto hash = rest.find('#'); hash != std::string_view::npos) {
    rest = rest.substr(0, hash);
  }
  return query_t{rest};
}

// ─────────────────────────── response_t ───────────────────────────

void response_t::begin() {
  release_owned();
  status_ = status_t::Ok;
  source_ = body_source_t::None;
  hdr_off_ = hdr_ ? hdr_->size() : 0;
  hdr_end_ = hdr_off_;
  body_off_ = body_out_ ? body_out_->size() : 0;
  body_len_ = 0;
  external_ = {};
  saw_content_length_ = false;
  saw_connection_ = false;
  saw_date_ = false;
  saw_transfer_encoding_ = false;
  err_ = {};
}

error_t response_t::error() const {
  if (err_) return err_;
  if (hdr_ && hdr_->failed()) return hdr_->error();
  if (body_out_ && body_out_->failed()) return body_out_->error();
  return {};
}

uint32_t response_t::hdr_length() const {
  return hdr_end_ > hdr_off_ ? hdr_end_ - hdr_off_ : 0;
}

response_t& response_t::header(field_t f, std::string_view value) {
  if (!hdr_) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  note_framing_header(f);
  serializer_t::header(*hdr_, f, value);
  hdr_end_ = hdr_->size();
  return *this;
}

response_t& response_t::header(std::string_view name, std::string_view value) {
  if (!hdr_) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  // Route through the enum so that a handler writing "content-length" by hand is
  // recognised as having set the framing header.
  note_framing_header(field_from_name(name));
  serializer_t::header(*hdr_, name, value);
  hdr_end_ = hdr_->size();
  return *this;
}

response_t& response_t::header(field_t f, uint64_t value) {
  if (!hdr_) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  note_framing_header(f);
  serializer_t::header_u64(*hdr_, f, value);
  hdr_end_ = hdr_->size();
  return *this;
}

void response_t::note_framing_header(field_t f) {
  switch (f) {
    case field_t::ContentLength:    saw_content_length_ = true; break;
    case field_t::Connection:       saw_connection_ = true; break;
    case field_t::Date:             saw_date_ = true; break;
    case field_t::TransferEncoding: saw_transfer_encoding_ = true; break;
    default: break;
  }
}

response_t& response_t::body(std::string_view data) {
  if (!body_out_) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  if (source_ != body_source_t::None) {
    // Two bodies would produce a response whose framing disagrees with its
    // content; refuse rather than send something ambiguous.
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  body_off_ = body_out_->size();
  body_out_->put(data);
  if (body_out_->failed()) {
    fail(body_out_->error());
    return *this;
  }
  body_len_ = data.size();
  source_ = body_source_t::Inline;
  return *this;
}

response_t& response_t::body_static(std::string_view data) {
  if (source_ != body_source_t::None) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  external_ = data;
  body_len_ = data.size();
  source_ = body_source_t::External;
  return *this;
}

response_t& response_t::body_owned(buffer_lease_t lease, uint32_t len) {
  if (source_ != body_source_t::None) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  if (!lease || len > lease.capacity()) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  external_ = std::string_view(lease.data(), len);
  owned_ = std::move(lease);
  body_len_ = len;
  source_ = body_source_t::External;
  return *this;
}

void response_t::seal_headers() {
  if (!hdr_) return;
  // The blank line that ends the header section. Appending it here — rather than
  // making it a separate iovec — costs two bytes in a buffer we already own and
  // keeps one segment out of every writev.
  hdr_->put_crlf();
  hdr_end_ = hdr_->size();
}

void response_t::release_owned() {
  for (auto& p : pinned_) {
    p.destroy(p.ptr);
  }
  pinned_.clear();
  owned_.release();
  external_ = {};
}

} // namespace cornet::http
