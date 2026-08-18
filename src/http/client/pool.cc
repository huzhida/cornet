#include "cornet/http/client/pool.h"

#include <algorithm>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <spdlog/spdlog.h>

#include "cornet/http/common/trace.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

// ─────────────────────────────── dns_cache_t ───────────────────────────────

namespace {

/**
 * @brief put the port into a cached address.
 * The cache is keyed by host alone — an address does not depend on the port — so the
 * copy handed out gets the port patched in.
 */
void set_port(resolved_address& addr, uint16_t port) {
  if (addr.addr.ss_family == AF_INET6) {
    reinterpret_cast<sockaddr_in6*>(&addr.addr)->sin6_port = htons(port);
  } else if (addr.addr.ss_family == AF_INET) {
    reinterpret_cast<sockaddr_in*>(&addr.addr)->sin_port = htons(port);
  }
}

} // namespace

coro_t<expected<resolved_address>> dns_cache_t::resolve(std::string_view host, uint16_t port) {
  auto now = ctx_.coarse_now_ns();

  // ── cache lookup ──
  if (auto it = entries_.find(host); it != entries_.end()) {
    if (it->second.expires_ns > now) {
      ++metrics_.dns_cache_hits;
      auto addr = it->second.addr;
      set_port(addr, port);
      co_return addr;
    }
    entries_.erase(it);
  }

  // ── DNS resolution ──
  ++metrics_.dns_lookups;
  auto r = co_await cornet::resolve(ctx_, host, port);
  if (!r) co_return unexpected(r.error());
  store(host, *r);
  co_return *r;
}

void dns_cache_t::store(std::string_view host, const resolved_address& addr) {
  if (opt_.dns_cache_entries == 0 || opt_.dns_cache_ttl.count() <= 0) return;

  if (entries_.size() >= opt_.dns_cache_entries) {
    // Drop what has already expired; if nothing has, drop one arbitrary entry. A
    // proper LRU would need a second index for a cache that is this small.
    auto now = ctx_.coarse_now_ns();
    for (auto it = entries_.begin(); it != entries_.end();) {
      it = it->second.expires_ns <= now ? entries_.erase(it) : std::next(it);
    }
    if (entries_.size() >= opt_.dns_cache_entries) entries_.erase(entries_.begin());
  }

  entry_t entry;
  entry.addr = addr;
  entry.expires_ns =
      ctx_.coarse_now_ns() + uint64_t(opt_.dns_cache_ttl.count()) * 1'000'000ull;
  entries_.insert_or_assign(std::string(host), entry);
}

// ─────────────────────────────── client_pool_t ───────────────────────────────

client_pool_t::client_pool_t(context_t& ctx, const client_options_t& opt, buffer_pool_t& bufs,
                             timer_wheel_t& wheel, client_metrics_t& metrics, dns_cache_t& dns)
  : ctx_(ctx), opt_(opt), bufs_(bufs), wheel_(wheel), metrics_(metrics), dns_(dns) {}

client_pool_t::~client_pool_t() { clear(); }

client_pool_t::bucket_t& client_pool_t::bucket_for(std::string_view host, uint16_t port) {
  auto it = origins_.find(host);
  if (it == origins_.end()) {
    it = origins_.emplace(std::string(host), std::map<uint16_t, bucket_t>{}).first;
  }
  auto& ports = it->second;
  auto pit = ports.find(port);
  if (pit == ports.end()) {
    bucket_t bucket;
    bucket.host = it->first;
    bucket.port = port;
    pit = ports.emplace(port, std::move(bucket)).first;
  }
  return pit->second;
}

client_pool_t::conn_ptr client_pool_t::detach(std::vector<conn_ptr>& list,
                                             client_connection_t* conn) {
  auto it = std::find_if(list.begin(), list.end(),
                         [conn](const conn_ptr& p) { return p.get() == conn; });
  if (it == list.end()) return nullptr;
  auto owned = std::move(*it);
  list.erase(it);
  return owned;
}

void client_pool_t::arm_idle(bucket_t& bucket, client_connection_t& conn) {
  (void)bucket;
  auto& node = conn.idle_timer();
  node.owner = &conn;
  node.on_expire = &client_pool_t::on_idle_expired;
  if (opt_.idle_timeout.count() > 0) wheel_.arm(node, opt_.idle_timeout);
}

void client_pool_t::on_idle_expired(void* owner) {
  auto* conn = static_cast<client_connection_t*>(owner);
  if (auto* pool = conn->pool_owner()) pool->drop_idle(*conn);
}

void client_pool_t::drop_idle(client_connection_t& conn) {
  auto& bucket = bucket_for(conn.host(), conn.port());
  auto owned = detach(bucket.idle, &conn);
  if (!owned) return;
  CORNET_HTTP_TRACE_LOG("client: fd={} idle timeout, closing", owned->native_fd());
  owned->close();
  if (total_ > 0) --total_;
}

void client_pool_t::grant_or_park(bucket_t& bucket, conn_ptr conn) {
  if (!bucket.waiters.empty()) {
    // Hand it straight to whoever has been waiting longest: parking it first would
    // let a newcomer take it instead.
    //
    // Peek first, though. A peer that answers one request and then closes is common
    // (a server with its own idle timeout, or one that simply serves once), and handing
    // such a connection to a waiter would cost it a failed request plus a retry.
    if (!conn->alive_hint()) {
      ++metrics_.stale_discarded;
      conn->close();
      if (total_ > 0) --total_;
      // A slot rather than a connection: the waiter opens a fresh one.
      auto* waiter = bucket.waiters.front();
      bucket.waiters.erase(bucket.waiters.begin());
      wheel_.cancel(waiter->timer);
      waiter->granted = nullptr;
      if (waiter->handle) ctx_.spawn(waiter->handle);
      return;
    }

    auto* waiter = bucket.waiters.front();
    bucket.waiters.erase(bucket.waiters.begin());
    wheel_.cancel(waiter->timer);
    waiter->granted = conn.get();
    bucket.busy.push_back(std::move(conn));
    ++metrics_.conn_reused;
    if (waiter->handle) ctx_.spawn(waiter->handle);
    return;
  }

  if (bucket.idle.size() >= opt_.max_idle_per_host) {
    conn->close();
    if (total_ > 0) --total_;
    return;
  }

  arm_idle(bucket, *conn);
  bucket.idle.push_back(std::move(conn));
}

void client_pool_t::on_wait_expired(void* owner) {
  auto* waiter = static_cast<waiter_t*>(owner);
  auto* bucket = static_cast<bucket_t*>(waiter->bucket);
  auto& queue = bucket->waiters;
  queue.erase(std::remove(queue.begin(), queue.end(), waiter), queue.end());
  waiter->expired = true;
  if (waiter->handle) waiter->pool->ctx_.spawn(waiter->handle);
}

void client_pool_t::wait_awaiter::await_suspend(std::coroutine_handle<> h) {
  waiter.handle = h;
  waiter.pool = pool;
  waiter.bucket = bucket;
  // Parked on a pool slot with no io in flight: the token is the only thing telling the
  // run loop that this request still exists.
  waiter.work = context_t::work_token_t(pool->ctx_);
  waiter.timer.owner = &waiter;
  waiter.timer.on_expire = &client_pool_t::on_wait_expired;
  bucket->waiters.push_back(&waiter);
  if (pool->opt_.pool_wait_timeout.count() > 0) {
    pool->wheel_.arm(waiter.timer, pool->opt_.pool_wait_timeout);
  }
}

expected<client_connection_t*> client_pool_t::wait_awaiter::await_resume() {
  pool->wheel_.cancel(waiter.timer);
  waiter.work.release();
  if (waiter.expired) return http_unexpected(http_error_t::PoolExhausted);
  // nullptr is not a failure: it means a slot freed up rather than a connection, and
  // the caller should look again.
  return waiter.granted;
}

coro_t<expected<client_connection_t*>> client_pool_t::acquire(std::string_view host,
                                                              uint16_t port, bool& reused) {
  reused = false;
  auto& bucket = bucket_for(host, port);

  for (;;) {
    // ── 1. an idle connection for this origin ──
    while (!bucket.idle.empty()) {
      auto conn = std::move(bucket.idle.back());
      bucket.idle.pop_back();
      wheel_.cancel(conn->idle_timer());

      if (!conn->alive_hint()) {
        // The peer closed it while it sat here, or sent something we never asked
        // for. Either way it cannot serve a request.
        ++metrics_.stale_discarded;
        CORNET_HTTP_TRACE_LOG("client: fd={} was stale, discarding", conn->native_fd());
        conn->close();
        if (total_ > 0) --total_;
        continue;
      }

      reused = true;
      ++metrics_.conn_reused;
      auto* raw = conn.get();
      bucket.busy.push_back(std::move(conn));
      co_return raw;
    }

    // ── 2. room for a new one ──
    if (bucket.busy.size() + bucket.opening < opt_.max_conns_per_host &&
        total_ < opt_.max_total_conns) {
      ++bucket.opening;

      // Fast path: numeric IP — skip the DNS cache coroutine entirely.
      auto addr_or = try_resolve_numeric(host, port);
      if (addr_or) {
        auto opened = co_await client_connection_t::open(ctx_, opt_, bufs_, wheel_, metrics_, host,
                                                         port, &*addr_or);
        --bucket.opening;
        if (!opened) co_return unexpected(opened.error());

        (*opened)->set_pool(this);
        auto* raw = opened->get();
        bucket.busy.push_back(std::move(*opened));
        ++total_;
        co_return raw;
      }

      // Slow path: DNS resolution through cache.
      auto addr = co_await dns_.resolve(host, port);
      if (!addr) {
        --bucket.opening;
        co_return unexpected(addr.error());
      }
      auto opened = co_await client_connection_t::open(ctx_, opt_, bufs_, wheel_, metrics_, host,
                                                       port, &*addr);
      --bucket.opening;
      if (!opened) co_return unexpected(opened.error());

      (*opened)->set_pool(this);
      auto* raw = opened->get();
      bucket.busy.push_back(std::move(*opened));
      ++total_;
      co_return raw;
    }

    // ── 3. wait for somebody to hand one back ──
    ++metrics_.pool_waits;
    auto got = co_await wait_awaiter{this, &bucket};
    if (!got) co_return unexpected(got.error());
    if (*got) {
      reused = true;
      co_return *got;
    }
    // A slot rather than a connection: round again.
  }
}

void client_pool_t::release(client_connection_t* conn, bool reusable) {
  if (!conn) return;
  auto& bucket = bucket_for(conn->host(), conn->port());
  auto owned = detach(bucket.busy, conn);
  if (!owned) return;   // never ours, or released twice

  if (!reusable) {
    owned->close();
    if (total_ > 0) --total_;
    // Closing frees a slot even though it frees no connection, so a waiter must still
    // be woken — it will open a fresh one.
    if (!bucket.waiters.empty()) {
      auto* waiter = bucket.waiters.front();
      bucket.waiters.erase(bucket.waiters.begin());
      wheel_.cancel(waiter->timer);
      waiter->granted = nullptr;
      if (waiter->handle) ctx_.spawn(waiter->handle);
    }
    return;
  }

  grant_or_park(bucket, std::move(owned));
}

void client_pool_t::clear() {
  for (auto& [host, ports] : origins_) {
    for (auto& [port, bucket] : ports) {
      for (auto* waiter : bucket.waiters) {
        wheel_.cancel(waiter->timer);
        waiter->expired = true;
        if (waiter->handle) ctx_.spawn(waiter->handle);
      }
      bucket.waiters.clear();

      for (auto& conn : bucket.idle) {
        wheel_.cancel(conn->idle_timer());
        conn->close();
      }
      bucket.idle.clear();

      // A busy connection means an exchange is still in flight, which is a caller
      // error at this point: aborting is the least surprising thing left to do.
      for (auto& conn : bucket.busy) {
        conn->abort();
        conn->close();
      }
      bucket.busy.clear();
    }
  }
  origins_.clear();
  total_ = 0;
}

uint32_t client_pool_t::idle_count() const {
  uint32_t n = 0;
  for (const auto& [host, ports] : origins_) {
    for (const auto& [port, bucket] : ports) n += uint32_t(bucket.idle.size());
  }
  return n;
}

uint32_t client_pool_t::busy_count() const {
  uint32_t n = 0;
  for (const auto& [host, ports] : origins_) {
    for (const auto& [port, bucket] : ports) n += uint32_t(bucket.busy.size());
  }
  return n;
}

} // namespace cornet::http
