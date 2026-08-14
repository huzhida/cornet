#include <gtest/gtest.h>

#include <string>

#include "cornet/http/router.h"

using namespace cornet;
using namespace cornet::http;

namespace {

/**
 * @brief marker so tests can tell which route ran without needing a live request.
 */
route_t& reg(router_t& r, method_t m, std::string_view path, std::string& sink,
             std::string tag) {
  return r.route(m, path, [&sink, tag](request_t&, response_t&) { sink = tag; });
}

/**
 * @brief inert request/response pair, enough to invoke a handler that ignores both.
 */
struct callable_env_t {
  request_t  req;
  response_t resp;
};

} // namespace

// ───────────────────────────── exact paths ─────────────────────────────

TEST(http_router, exact_match) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/hello", hit, "hello");
  reg(r, method_t::Get, "/world", hit, "world");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/hello", p);
  ASSERT_TRUE(m);
  callable_env_t env;
  m.route->sync_fn(env.req, env.resp);
  EXPECT_EQ(hit, "hello");
}

TEST(http_router, trailing_and_duplicate_slashes_normalise) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/a/b", hit, "ab");

  param_slots_t p;
  EXPECT_TRUE(r.match(method_t::Get, "/a/b", p));
  EXPECT_TRUE(r.match(method_t::Get, "/a/b/", p));
  EXPECT_TRUE(r.match(method_t::Get, "//a//b", p));
}

TEST(http_router, method_is_part_of_the_key) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/x", hit, "get");
  reg(r, method_t::Post, "/x", hit, "post");

  param_slots_t p;
  auto g = r.match(method_t::Get, "/x", p);
  auto o = r.match(method_t::Post, "/x", p);
  ASSERT_TRUE(g);
  ASSERT_TRUE(o);
  EXPECT_NE(g.route, o.route);
}

TEST(http_router, unknown_path_does_not_match) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/x", hit, "x");
  param_slots_t p;
  auto m = r.match(method_t::Get, "/y", p);
  EXPECT_FALSE(m);
  EXPECT_FALSE(m.method_mismatch);
}

// ──────────────────────────── parameters ────────────────────────────

TEST(http_router, single_parameter_is_captured) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/users/:id", hit, "user");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/users/42", p);
  ASSERT_TRUE(m);
  EXPECT_EQ(p.get("id"), "42");
}

TEST(http_router, several_parameters) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/users/:uid/posts/:pid", hit, "post");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/users/7/posts/99", p);
  ASSERT_TRUE(m);
  EXPECT_EQ(p.get("uid"), "7");
  EXPECT_EQ(p.get("pid"), "99");
}

TEST(http_router, parameter_does_not_span_segments) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/users/:id", hit, "user");
  param_slots_t p;
  EXPECT_FALSE(r.match(method_t::Get, "/users/7/extra", p));
}

TEST(http_router, literal_wins_over_parameter) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/users/:id", hit, "param");
  reg(r, method_t::Get, "/users/me", hit, "literal");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/users/me", p);
  ASSERT_TRUE(m);
  callable_env_t env;
  m.route->sync_fn(env.req, env.resp);
  // a specific route must not be shadowed by a general one
  EXPECT_EQ(hit, "literal");

  auto other = r.match(method_t::Get, "/users/17", p);
  ASSERT_TRUE(other);
  other.route->sync_fn(env.req, env.resp);
  EXPECT_EQ(hit, "param");
  EXPECT_EQ(p.get("id"), "17");
}

TEST(http_router, wildcard_captures_the_rest) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/static/*path", hit, "static");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/static/css/site.css", p);
  ASSERT_TRUE(m);
  EXPECT_EQ(p.get("path"), "css/site.css");
}

TEST(http_router, wildcard_yields_to_a_more_specific_route) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/static/*path", hit, "wild");
  reg(r, method_t::Get, "/static/favicon.ico", hit, "icon");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/static/favicon.ico", p);
  ASSERT_TRUE(m);
  callable_env_t env;
  m.route->sync_fn(env.req, env.resp);
  EXPECT_EQ(hit, "icon");
}

TEST(http_router, backtracks_from_a_dead_end_literal) {
  // "/a/b/c" must fall back to the parameter branch when the literal branch has
  // no route at the end.
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/a/b/d", hit, "literal");
  reg(r, method_t::Get, "/a/:x/c", hit, "param");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/a/b/c", p);
  ASSERT_TRUE(m);
  callable_env_t env;
  m.route->sync_fn(env.req, env.resp);
  EXPECT_EQ(hit, "param");
  EXPECT_EQ(p.get("x"), "b");
}

// ──────────────────────── method mismatch / fallback ────────────────────────

TEST(http_router, method_mismatch_is_distinguished_from_missing) {
  router_t r;
  std::string hit;
  reg(r, method_t::Post, "/items/:id", hit, "post");

  param_slots_t p;
  auto m = r.match(method_t::Get, "/items/5", p);
  EXPECT_FALSE(m);
  // the caller needs this to answer 405 rather than 404
  EXPECT_TRUE(m.method_mismatch);
}

TEST(http_router, fallback_is_reported_separately) {
  router_t r;
  EXPECT_EQ(r.fallback_route(), nullptr);
  std::string hit;
  r.fallback([&hit](request_t&, response_t&) { hit = "fallback"; });
  ASSERT_NE(r.fallback_route(), nullptr);
  EXPECT_EQ(r.fallback_route()->kind, route_t::kind_t::Sync);
}

// ─────────────────────── sync / async handler kinds ───────────────────────

TEST(http_router, handler_kind_is_deduced_from_the_return_type) {
  router_t r;
  // a plain lambda is synchronous: no frame, no suspension, no co_return noise
  auto& sync = r.get("/sync", [](request_t&, response_t&) {});
  EXPECT_EQ(sync.kind, route_t::kind_t::Sync);
  EXPECT_TRUE(sync.valid());

  auto& async = r.get("/async", [](request_t&, response_t&) -> coro_t<void> { co_return; });
  EXPECT_EQ(async.kind, route_t::kind_t::Async);
  EXPECT_TRUE(async.valid());
}

TEST(http_router, body_policy_defaults_to_auto_and_is_settable) {
  router_t r;
  auto& route = r.post("/upload", [](request_t&, response_t&) {});
  EXPECT_EQ(route.body, body_policy_t::Auto);
  route.body = body_policy_t::Stream;
  param_slots_t p;
  auto m = r.match(method_t::Post, "/upload", p);
  ASSERT_TRUE(m);
  EXPECT_EQ(m.route->body, body_policy_t::Stream);
}

TEST(http_router, filters_record_their_kind) {
  router_t r;
  EXPECT_FALSE(r.has_filters());
  r.filter([](request_t&, response_t&) { return true; });
  r.filter([](request_t&, response_t&) -> coro_t<bool> { co_return true; });
  ASSERT_TRUE(r.has_filters());
  ASSERT_EQ(r.filters().size(), 2u);
  EXPECT_EQ(r.filters()[0].kind, filter_entry_t::kind_t::Sync);
  EXPECT_EQ(r.filters()[1].kind, filter_entry_t::kind_t::Async);
}

// ───────────────────────────── param slots ─────────────────────────────

TEST(http_router, param_slots_are_bounded) {
  param_slots_t p;
  for (uint32_t i = 0; i < param_slots_t::kMax; ++i) {
    EXPECT_TRUE(p.add("k", "v"));
  }
  EXPECT_FALSE(p.add("overflow", "v"));
  EXPECT_EQ(p.count, param_slots_t::kMax);
  p.clear();
  EXPECT_EQ(p.count, 0u);
}

TEST(http_router, root_path) {
  router_t r;
  std::string hit;
  reg(r, method_t::Get, "/", hit, "root");
  param_slots_t p;
  EXPECT_TRUE(r.match(method_t::Get, "/", p));
  EXPECT_TRUE(r.match(method_t::Get, "", p));
}
