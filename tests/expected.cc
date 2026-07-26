#include "cornet/base/expected.h"

#include <gtest/gtest.h>
#include <string>

using namespace cornet;

TEST(expected, int_success) {
  expected<int> e(42);
  EXPECT_TRUE(e.has_value());
  EXPECT_EQ(*e, 42);
  EXPECT_EQ(e.value(), 42);
}

TEST(expected, int_error) {
  expected<int> e(unexpected(EINVAL));
  EXPECT_FALSE(e.has_value());
  EXPECT_EQ(e.error().code, EINVAL);
  EXPECT_EQ(e.error().domain, error_domain::system);
}

TEST(expected, void_success) {
  expected<void> e;
  EXPECT_TRUE(e.has_value());
}

TEST(expected, void_error) {
  expected<void> e(unexpected(ENOENT));
  EXPECT_FALSE(e.has_value());
  EXPECT_EQ(e.error().code, ENOENT);
}

TEST(expected, move_construct) {
  expected<std::string> e1(std::string("hello"));
  expected<std::string> e2(std::move(e1));
  EXPECT_TRUE(e2.has_value());
  EXPECT_EQ(*e2, "hello");
}

TEST(expected, move_assign) {
  expected<std::string> e1(std::string("hello"));
  expected<std::string> e2(std::string("world"));
  e2 = std::move(e1);
  EXPECT_TRUE(e2.has_value());
  EXPECT_EQ(*e2, "hello");
}

TEST(expected, assign_error_over_value) {
  expected<std::string> e(std::string("hello"));
  EXPECT_TRUE(e.has_value());
  e = unexpected(EPERM);
  EXPECT_FALSE(e.has_value());
  EXPECT_EQ(e.error().code, EPERM);
}

TEST(expected, assign_value_over_error) {
  expected<int> e(unexpected(EINVAL));
  EXPECT_FALSE(e.has_value());
  e = expected<int>(99);
  EXPECT_TRUE(e.has_value());
  EXPECT_EQ(*e, 99);
}

TEST(expected, error_domains) {
  auto sys = unexpected(ECONNREFUSED);
  expected<int> e1(sys);
  EXPECT_EQ(e1.error().domain, error_domain::system);
  EXPECT_STREQ(e1.error().message(), strerror(ECONNREFUSED));

  auto resolve = unexpected(EAI_NONAME, error_domain::resolve);
  expected<int> e2(resolve);
  EXPECT_EQ(e2.error().domain, error_domain::resolve);
  EXPECT_STREQ(e2.error().message(), gai_strerror(EAI_NONAME));
}

TEST(expected, arrow_operator) {
  struct Foo { int x = 5; };
  expected<Foo> e(Foo{10});
  EXPECT_EQ(e->x, 10);
}

TEST(expected, bool_conversion) {
  expected<int> ok(1);
  expected<int> err(unexpected(1));
  EXPECT_TRUE(static_cast<bool>(ok));
  EXPECT_FALSE(static_cast<bool>(err));
}
