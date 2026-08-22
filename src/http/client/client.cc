#include "cornet/http/client/client.h"

#include <spdlog/spdlog.h>

#include "cornet/http/common/trace.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

namespace {

bool is_redirect(uint16_t code) {
  return code == 301 || code == 302 || code == 303 || code == 307 || code == 308;
}

/**
 * @brief whether following this redirect turns the request into a GET.
 *
 * 303 says so outright, and 301/302 do in practice: every browser and every other
 * client has rewritten them to GET for decades, so a server that means "repeat the
 * POST" uses 307 instead.
 */
bool redirect_becomes_get(uint16_t code) {
  return code == 301 || code == 302 || code == 303;
}

} // namespace

// ─────────────────────────────── client_t ───────────────────────────────

client_t::client_t(context_t& ctx, client_options_t opt)
  : ctx_(ctx), opt_(std::move(opt)), bufs_(buffer_pool_t::local()),
    wheel_(ctx.wheel_for(opt_.timer_tick)), dns_(ctx, opt_, metrics_),
    pool_(ctx, opt_, bufs_, *wheel_, metrics_, dns_) {
  opt_.load(ctx.config());
}

client_t::~client_t() { close(); }

void client_t::close() {
  // Clearing the pool is what unlinks this client's nodes from the shared wheel;
  // the wheel itself belongs to the context and keeps serving the other tenants.
  pool_.clear();
  dns_.clear();
}

std::chrono::milliseconds client_t::remaining(uint64_t deadline_ns) const {
  if (deadline_ns == 0) return std::chrono::milliseconds(0);
  auto now = ctx_.coarse_now_ns();
  if (now >= deadline_ns) return std::chrono::milliseconds(1);
  return std::chrono::milliseconds((deadline_ns - now) / 1'000'000ull + 1);
}

expected<const url_t*> client_t::parse_cached(std::string_view url) {
  if (url.empty()) return http_unexpected(http_error_t::BadUrl);
  uint64_t h = 14695981039346656037ull;
  for (char c : url) {
    h ^= uint8_t(c);
    h *= 1099511628211ull;
  }
  auto& slot = url_cache_[h % url_cache_.size()];
  if (slot.raw == url) {
    ++metrics_.url_cache_hits;
    return &slot.parsed;
  }
  auto parsed = url_t::parse(url);
  if (!parsed) return unexpected(parsed.error());
  slot.raw = url;
  slot.parsed = *parsed;
  ++metrics_.url_cache_misses;
  return &slot.parsed;
}

client_request_t client_t::request(method_t m, std::string_view url) {
  auto built = client_request_t::make(bufs_, m, url, opt_.hdr_buffer_bytes, this);
  if (!built) {
    // A bad url is reported by send(), not by a check here: it keeps the builder
    // chainable, which is the whole point of the shape.
    client_request_t bad;
    bad.owner_ = this;
    bad.fail(built.error());
    return bad;
  }
  built->owner_ = this;
  return std::move(*built);
}

coro_t<expected<client_t::lease_t>> client_t::borrow(const url_t& url) {
  bool https = url.scheme() == scheme_t::Https;
#ifdef CORNET_WITH_TLS
  if (https && !opt_.tls) {
    // Lazily built on the first https request: a plain-http client must never
    // pay verify path loading for a context it never uses.
    auto cx = tls::tls_context_t::make_client(tls::tls_client_options_t{
        .verify_peer = opt_.tls_verify,
        .ca_file = opt_.tls_ca_file,
        .ca_dir = opt_.tls_ca_dir,
        .ca_pem = opt_.tls_ca_pem,
    });
    if (!cx) {
      SPDLOG_DEBUG("http client: tls context setup failed ({})", cx.error().message());
      co_return unexpected(cx.error());
    }
    opt_.tls = std::move(*cx);
  }
#else
  if (https) {
    // No TLS in this build. Saying so precisely beats a connect to port 443 that
    // then fails to parse whatever comes back.
    co_return http_unexpected(http_error_t::UnsupportedScheme);
  }
#endif
  lease_t lease;
  auto conn = co_await pool_.acquire(url.host(), url.port(), url.scheme(), lease.reused);
  if (!conn) co_return unexpected(conn.error());
  lease.conn = *conn;
  co_return lease;
}

void client_t::give_back(client_connection_t* conn, bool reusable) {
  pool_.release(conn, reusable);
}

bool client_t::follow(client_request_t& req, const client_response_t& resp) {
  auto location = resp.header(field_t::Location);
  if (location.empty()) return false;

  // Resolve against the current url *before* retargeting, since retargeting rewrites
  // the very bytes the base url views point at.
  char absolute[2048];
  auto len = write_absolute_url(req.url(), location, absolute, sizeof(absolute));
  if (!len) return false;

  auto target = url_t::parse(std::string_view(absolute, *len));
  if (!target) return false;

  if (!target->same_origin(req.url()) && req.saw_authorization()) {
    // Credentials must not cross an origin, and a staged header cannot be unwritten:
    // the user headers are raw bytes by design. Handing the 3xx back is the only safe
    // answer left.
    SPDLOG_DEBUG("http client: not following a cross-origin redirect that carries "
                 "Authorization");
    return false;
  }

  if (redirect_becomes_get(resp.status_code())) {
    req.set_method(method_t::Get);
    req.drop_body();
  }
  if (auto ok = req.retarget(std::string_view(absolute, *len)); !ok) return false;
  return true;
}

coro_t<expected<client_response_t>> client_t::send(client_request_t& req) {
  if (req.failed()) co_return unexpected(req.error());

  auto total = req.has_timeout_ ? req.timeout_ : opt_.total_timeout;
  uint64_t deadline = total.count() > 0
                          ? ctx_.coarse_now_ns() + uint64_t(total.count()) * 1'000'000ull
                          : 0;
  uint8_t redirects_left = req.has_redirects_ ? req.max_redirects_ : opt_.max_redirects;

  for (;;) {
    uint32_t tries = 1u + (req.has_retries_ ? req.max_retries_ : opt_.max_retries);
    bool idempotent =
        req.has_idempotent_ ? req.idempotent_ : method_is_idempotent(req.method());

    expected<client_response_t> result = http_unexpected(http_error_t::InvalidState);

    for (uint32_t attempt = 0; attempt < tries; ++attempt) {
      auto lease = co_await borrow(req.url());
      if (!lease) {
        result = unexpected(lease.error());
        break;
      }

      lease->conn->set_deadline(remaining(deadline));
      auto resp = co_await lease->conn->exchange(req);

      // Read the connection's state before handing it back: a connection that is not
      // reusable is closed and destroyed by release().
      bool reusable = lease->conn->reusable();
      bool responded = lease->conn->responded();
      auto err = resp ? error_t{} : resp.error();
      give_back(lease->conn, reusable);

      if (resp) {
        result = std::move(resp);
        break;
      }
      result = unexpected(err);

      // The only failure worth replaying: a connection that came from the pool died
      // before answering. Anything else either already produced part of an answer, or
      // would fail again the same way.
      bool retryable = lease->reused && idempotent && !responded &&
                       err.code != ETIMEDOUT && attempt + 1 < tries;
      if (!retryable) break;
      ++metrics_.retries;
      CORNET_HTTP_TRACE_LOG("client: retrying after {}", err.message());
    }

    if (!result) co_return result;
    if (redirects_left == 0 || !is_redirect(result->status_code())) co_return result;
    if (!follow(req, *result)) co_return result;

    --redirects_left;
    ++metrics_.redirects;
  }
}

// ─────────────────────────────── one-liners ───────────────────────────────

coro_t<expected<client_response_t>> client_t::get(std::string_view url) {
  auto req = request(method_t::Get, url);
  co_return co_await send(req);
}

coro_t<expected<client_response_t>> client_t::head(std::string_view url) {
  auto req = request(method_t::Head, url);
  co_return co_await send(req);
}

coro_t<expected<client_response_t>> client_t::del(std::string_view url) {
  auto req = request(method_t::Delete, url);
  co_return co_await send(req);
}

coro_t<expected<client_response_t>> client_t::post(std::string_view url, std::string_view body,
                                                   std::string_view content_type) {
  auto req = request(method_t::Post, url);
  if (!content_type.empty()) req.header(field_t::ContentType, content_type);
  req.body(body);
  co_return co_await send(req);
}

coro_t<expected<client_response_t>> client_t::put(std::string_view url, std::string_view body,
                                                  std::string_view content_type) {
  auto req = request(method_t::Put, url);
  if (!content_type.empty()) req.header(field_t::ContentType, content_type);
  req.body(body);
  co_return co_await send(req);
}

// ─────────────────────────────── streaming ───────────────────────────────

coro_t<expected<client_stream_t>> client_t::stream(method_t m, std::string_view url) {
  auto req = request(m, url);
  co_return co_await stream(req);
}

coro_t<expected<client_stream_t>> client_t::stream(client_request_t& req) {
  if (req.failed()) co_return unexpected(req.error());

  auto total = req.has_timeout_ ? req.timeout_ : opt_.total_timeout;
  uint64_t deadline = total.count() > 0
                          ? ctx_.coarse_now_ns() + uint64_t(total.count()) * 1'000'000ull
                          : 0;
  uint32_t tries = 1u + (req.has_retries_ ? req.max_retries_ : opt_.max_retries);
  bool idempotent = req.has_idempotent_ ? req.idempotent_ : method_is_idempotent(req.method());

  cornet::error_t last{};
  for (uint32_t attempt = 0; attempt < tries; ++attempt) {
    auto lease = co_await borrow(req.url());
    if (!lease) co_return unexpected(lease.error());

    lease->conn->set_deadline(remaining(deadline));
    auto begun = co_await lease->conn->begin_exchange(req);
    if (begun) {
      // The connection stays borrowed: the stream owns it until the body ends.
      co_return client_stream_t{this, lease->conn};
    }

    last = begun.error();
    bool responded = lease->conn->responded();
    give_back(lease->conn, false);

    bool retryable = lease->reused && idempotent && !responded && last.code != ETIMEDOUT &&
                     attempt + 1 < tries;
    if (!retryable) break;
    ++metrics_.retries;
  }
  co_return unexpected(last);
}

coro_t<expected<client_upload_t>> client_t::upload(method_t m, std::string_view url) {
  auto req = request(m, url);
  co_return co_await upload(req);
}

coro_t<expected<client_upload_t>> client_t::upload(client_request_t& req) {
  if (req.failed()) co_return unexpected(req.error());

  auto total = req.has_timeout_ ? req.timeout_ : opt_.total_timeout;
  uint64_t deadline = total.count() > 0
                          ? ctx_.coarse_now_ns() + uint64_t(total.count()) * 1'000'000ull
                          : 0;

  // No retry loop here on purpose: the body is produced by the caller as we go, so
  // there is nothing left to replay once the first chunk is out.
  auto lease = co_await borrow(req.url());
  if (!lease) co_return unexpected(lease.error());

  lease->conn->set_deadline(remaining(deadline));
  auto begun = co_await lease->conn->begin_chunked(req);
  if (!begun) {
    give_back(lease->conn, false);
    co_return unexpected(begun.error());
  }
  co_return client_upload_t{this, lease->conn, req.method()};
}

// ─────────────────────── client_request_t::send ───────────────────────

coro_t<expected<client_response_t>> client_request_t::send() {
  if (!owner_) co_return http_unexpected(http_error_t::InvalidState);
  co_return co_await owner_->send(*this);
}

// ─────────────────────────── client_stream_t ───────────────────────────

client_stream_t::client_stream_t(client_stream_t&& o) noexcept
  : owner_(o.owner_), conn_(o.conn_) {
  o.owner_ = nullptr;
  o.conn_ = nullptr;
}

client_stream_t& client_stream_t::operator=(client_stream_t&& o) noexcept {
  if (this != &o) {
    give_back();
    owner_ = o.owner_;
    conn_ = o.conn_;
    o.owner_ = nullptr;
    o.conn_ = nullptr;
  }
  return *this;
}

client_stream_t::~client_stream_t() { give_back(); }

void client_stream_t::give_back() {
  if (!conn_) return;
  // reusable() is false while the body is unfinished, so a stream dropped early
  // discards its connection instead of poisoning the pool with a half-read one.
  bool reusable = conn_->reusable();
  if (owner_) owner_->give_back(conn_, reusable);
  conn_ = nullptr;
  owner_ = nullptr;
}

status_t client_stream_t::status() const { return conn_ ? conn_->status() : status_t(0); }

bool client_stream_t::ok() const {
  auto code = uint16_t(status());
  return code >= 200 && code < 300;
}

const headers_t& client_stream_t::headers() const {
  static const headers_t empty;
  auto* h = conn_ ? conn_->headers() : nullptr;
  return h ? *h : empty;
}

coro_t<expected<std::string_view>> client_stream_t::read() {
  if (!conn_) co_return http_unexpected(http_error_t::InvalidState);
  co_return co_await conn_->read_body();
}

coro_t<expected<void>> client_stream_t::drain() {
  if (!conn_) co_return http_unexpected(http_error_t::InvalidState);
  co_return co_await conn_->drain_body();
}

expected<client_response_t> client_stream_t::finish() {
  if (!conn_) return http_unexpected(http_error_t::InvalidState);
  auto resp = conn_->take_response();
  give_back();
  return resp;
}

// ─────────────────────────── client_upload_t ───────────────────────────

client_upload_t::client_upload_t(client_upload_t&& o) noexcept
  : owner_(o.owner_), conn_(o.conn_), method_(o.method_) {
  o.owner_ = nullptr;
  o.conn_ = nullptr;
}

client_upload_t& client_upload_t::operator=(client_upload_t&& o) noexcept {
  if (this != &o) {
    give_back();
    owner_ = o.owner_;
    conn_ = o.conn_;
    method_ = o.method_;
    o.owner_ = nullptr;
    o.conn_ = nullptr;
  }
  return *this;
}

client_upload_t::~client_upload_t() { give_back(); }

void client_upload_t::give_back() {
  if (!conn_) return;
  // An upload abandoned mid-body leaves the request unfinished on the wire, so the
  // connection can never be reused.
  if (owner_) owner_->give_back(conn_, false);
  conn_ = nullptr;
  owner_ = nullptr;
}

coro_t<expected<void>> client_upload_t::write(std::string_view data) {
  if (!conn_) co_return http_unexpected(http_error_t::InvalidState);
  co_return co_await conn_->write_chunk(data);
}

coro_t<expected<client_response_t>> client_upload_t::finish() {
  if (!conn_) co_return http_unexpected(http_error_t::InvalidState);
  if (auto ok = co_await conn_->finish_chunks(); !ok) {
    give_back();
    co_return unexpected(ok.error());
  }
  auto resp = co_await conn_->read_response(method_);

  auto* conn = conn_;
  auto* owner = owner_;
  conn_ = nullptr;
  owner_ = nullptr;
  if (owner) owner->give_back(conn, resp ? conn->reusable() : false);
  co_return resp;
}

} // namespace cornet::http
