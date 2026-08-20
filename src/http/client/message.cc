#include "cornet/http/client/message.h"

#include "cornet/http/client/client.h"

#include <cstring>
#include <new>

namespace cornet::http {

// ──────────────────────────── inbound_t ────────────────────────────

inbound_t* inbound_t::create(buffer_pool_t& pool, uint32_t head_bytes) {
  auto block = pool.acquire(uint32_t(sizeof(inbound_t)));
  if (!block) return nullptr;
  auto head = pool.acquire(head_bytes);
  if (!head) return nullptr;   // the block goes back with the lease

  auto* node = new (block.data()) inbound_t();
  node->self_ = std::move(block);
  node->head.reset(std::move(head));
  node->headers.bind(&node->head, &node->spill);
  return node;
}

void inbound_t::destroy() {
  // The object lives inside the block this lease owns, so the lease has to outlive
  // the destructor call: move it out first and let the local release it last.
  buffer_lease_t self = std::move(self_);
  this->~inbound_t();
  (void)self;
}

// ───────────────────────── client_response_t ─────────────────────────

namespace {

/**
 * @brief the headers of a response that does not exist.
 * Empty and unbound, so size() is 0 and nothing dereferences a buffer.
 */
const headers_t& no_headers() {
  static const headers_t empty;
  return empty;
}

} // namespace

void client_response_t::reset() {
  if (node_) {
    node_->destroy();
    node_ = nullptr;
  }
}

status_t client_response_t::status() const {
  // 0 is not a real status: an invalid response reports one that cannot be mistaken
  // for an answer.
  return node_ ? node_->status : status_t(0);
}

bool client_response_t::ok() const {
  auto code = uint16_t(status());
  return code >= 200 && code < 300;
}

version_t client_response_t::version() const {
  return node_ ? node_->version : version_t::Unknown;
}

const headers_t& client_response_t::headers() const {
  return node_ ? node_->headers : no_headers();
}

std::string_view client_response_t::body() const {
  return node_ ? node_->body.view() : std::string_view{};
}

bool client_response_t::has_content_length() const {
  return node_ && node_->has_content_length;
}

uint64_t client_response_t::content_length() const {
  return node_ ? node_->content_length : 0;
}

bool client_response_t::chunked() const { return node_ && node_->chunked; }

bool client_response_t::keep_alive() const { return node_ && node_->keep_alive; }

// ───────────────────────── client_request_t ─────────────────────────

expected<client_request_t> client_request_t::make(buffer_pool_t& pool, method_t m,
                                                 std::string_view url, uint32_t hdr_bytes,
                                                 client_t* owner) {
  client_request_t req;
  // owner must be set before init(): retarget() below asks the owner's parse
  // cache when one exists; wiring it up afterwards leaves the cache unused
  req.owner_ = owner;
  if (auto ok = req.init(pool, m, url, hdr_bytes); !ok) return unexpected(ok.error());
  return req;
}

expected<void> client_request_t::init(buffer_pool_t& pool, method_t m, std::string_view url,
                                      uint32_t hdr_bytes) {
  pool_ = &pool;
  method_ = m;
  auto hdr = pool.acquire(hdr_bytes);
  if (!hdr) return unexpected(ENOMEM);
  hdr_.reset(std::move(hdr));
  return retarget(url);
}

expected<void> client_request_t::retarget(std::string_view url) {
  if (!pool_) return http_unexpected(http_error_t::InvalidState);
  if (url.empty()) return http_unexpected(http_error_t::BadUrl);

  // Own the bytes: the caller's string may be a temporary, and a retry or a redirect
  // needs the url long after send() was called.
  if (!url_lease_ || url_lease_.capacity() < url.size()) {
    auto lease = pool_->acquire(uint32_t(url.size()));
    if (!lease) return unexpected(ENOMEM);
    url_lease_ = std::move(lease);
  }
  std::memcpy(url_lease_.data(), url.data(), url.size());
  url_len_ = uint32_t(url.size());

  std::string_view owned(url_lease_.data(), url_len_);
  if (owner_) {
    // hit the client's parse cache: the view still anchors onto this request's
    // own lease, only the scan itself is skipped
    auto cached = owner_->parse_cached(url);
    if (!cached) return unexpected(cached.error());
    url_ = (*cached)->rebase(owned);
  } else {
    auto parsed = url_t::parse(owned);
    if (!parsed) return unexpected(parsed.error());
    url_ = *parsed;
  }
  return {};
}

void client_request_t::note_framing_header(field_t f) {
  switch (f) {
    case field_t::Host:             saw_host_ = true; break;
    case field_t::ContentLength:    saw_content_length_ = true; break;
    case field_t::Connection:       saw_connection_ = true; break;
    case field_t::TransferEncoding: saw_transfer_encoding_ = true; break;
    case field_t::UserAgent:        saw_user_agent_ = true; break;
    case field_t::Accept:           saw_accept_ = true; break;
    case field_t::Expect:           saw_expect_ = true; break;
    case field_t::Authorization:    saw_authorization_ = true; break;
    default: break;
  }
}

void client_request_t::drop_body() {
  source_ = body_source_t::None;
  body_len_ = 0;
  external_ = {};
  owned_.release();
  body_out_.clear();
}

client_request_t& client_request_t::header(field_t f, std::string_view value) {
  serializer_t::header(hdr_, f, value);
  note_framing_header(f);
  return *this;
}

client_request_t& client_request_t::header(std::string_view name, std::string_view value) {
  serializer_t::header(hdr_, name, value);
  // A framing header written by name must count too, or the connection would emit a
  // second one — duplicate Content-Length is a framing error, not a cosmetic one.
  note_framing_header(field_from_name(name));
  return *this;
}

client_request_t& client_request_t::header(field_t f, uint64_t value) {
  serializer_t::header_u64(hdr_, f, value);
  note_framing_header(f);
  return *this;
}

expected<void> client_request_t::ensure_body_buffer(uint32_t bytes) {
  if (!pool_) return http_unexpected(http_error_t::InvalidState);
  if (body_out_.attached() && body_out_.capacity() >= bytes) return {};
  auto lease = pool_->acquire(bytes);
  if (!lease) return unexpected(ENOMEM);
  body_out_.reset(std::move(lease));
  return {};
}

client_request_t& client_request_t::body(std::string_view data) {
  if (source_ != body_source_t::None) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  if (data.empty()) return *this;

  if (auto ok = ensure_body_buffer(uint32_t(data.size())); !ok) {
    fail(ok.error());
    return *this;
  }
  body_out_.put(data);
  if (body_out_.failed()) {
    fail(body_out_.error());
    return *this;
  }
  source_ = body_source_t::Inline;
  body_len_ = data.size();
  return *this;
}

client_request_t& client_request_t::body_static(std::string_view data) {
  if (source_ != body_source_t::None) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  if (data.empty()) return *this;
  source_ = body_source_t::External;
  external_ = data;
  body_len_ = data.size();
  return *this;
}

client_request_t& client_request_t::body_owned(buffer_lease_t lease, uint32_t len) {
  if (source_ != body_source_t::None) {
    fail(http_error(http_error_t::InvalidState));
    return *this;
  }
  if (!lease || len == 0) return *this;
  owned_ = std::move(lease);
  external_ = std::string_view(owned_.data(), len);
  source_ = body_source_t::External;
  body_len_ = len;
  return *this;
}

std::string_view client_request_t::inline_body() const {
  return body_out_.attached() ? body_out_.view() : std::string_view{};
}

std::string_view client_request_t::body_view() const {
  switch (source_) {
    case body_source_t::Inline:   return inline_body();
    case body_source_t::External: return external_;
    default:                      return {};
  }
}

error_t client_request_t::error() const {
  if (err_) return err_;
  if (hdr_.failed()) return hdr_.error();
  if (body_out_.failed()) return body_out_.error();
  return {};
}

} // namespace cornet::http
