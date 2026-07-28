#include "cornet/io_uring/awaiters.h"
#include "cornet/scheduling/context.h"

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <poll.h>
#include <sys/socket.h>

using namespace cornet;

class fs_io : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = new context_t();
  }

  void TearDown() override {
    delete ctx;
  }

  context_t* ctx;
};

TEST_F(fs_io, shutdown_socket) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    int sv[2];
    EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    auto ret = co_await shutdown_awaiter(ctx, sv[0], SHUT_WR);
    EXPECT_TRUE(ret.has_value());
    // after shutdown write end, read should get EOF
    char buf[1];
    auto n = co_await read_awaiter(ctx, sv[1], buf, 1);
    EXPECT_TRUE(n.has_value());
    EXPECT_EQ(*n, 0);
    ::close(sv[0]);
    ::close(sv[1]);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, openat_read_close) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    const char* path = "/tmp/cornet_test_openat.txt";
    int fd = ::open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    EXPECT_GE(fd, 0);
    ::write(fd, "hello", 5);
    ::close(fd);

    auto result = co_await openat_awaiter(ctx, AT_FDCWD, path, O_RDONLY);
    EXPECT_TRUE(result.has_value());
    if (!result) co_return;
    int afd = *result;
    EXPECT_GE(afd, 0);

    // async read
    char buf[16] = {};
    auto n = co_await read_awaiter(ctx, afd, buf, sizeof(buf));
    EXPECT_TRUE(n.has_value());
    EXPECT_EQ(*n, 5);
    EXPECT_STREQ(buf, "hello");

    co_await close_awaiter(ctx, afd);
    ::unlink(path);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, splice_pipe) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    int pipe1[2], pipe2[2];
    EXPECT_EQ(pipe(pipe1), 0);
    EXPECT_EQ(pipe(pipe2), 0);

    ::write(pipe1[1], "splice", 6);
    auto n = co_await splice_awaiter(ctx, pipe1[0], -1, pipe2[1], -1, 6, 0);
    EXPECT_TRUE(n.has_value());
    EXPECT_EQ(*n, 6);

    char buf[16] = {};
    ::read(pipe2[0], buf, sizeof(buf));
    EXPECT_STREQ(buf, "splice");

    ::close(pipe1[0]); ::close(pipe1[1]);
    ::close(pipe2[0]); ::close(pipe2[1]);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, poll_add_readable) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    int pipefd[2];
    EXPECT_EQ(pipe(pipefd), 0);
    ::write(pipefd[1], "x", 1);

    auto mask = co_await poll_add_awaiter(ctx, pipefd[0], POLLIN);
    EXPECT_TRUE(mask.has_value());
    EXPECT_NE(*mask & POLLIN, 0);

    ::close(pipefd[0]);
    ::close(pipefd[1]);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, statx_file) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    const char* path = "/tmp/cornet_test_statx.txt";
    int fd = ::open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ::write(fd, "abcdef", 6);
    ::close(fd);

    struct statx stx{};
    auto ret = co_await statx_awaiter(ctx, AT_FDCWD, path, 0, STATX_SIZE, &stx);
    EXPECT_TRUE(ret.has_value());
    EXPECT_EQ(stx.stx_size, 6u);

    ::unlink(path);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, mkdirat_unlinkat) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    const char* path = "/tmp/cornet_test_mkdir";

    auto ret = co_await mkdirat_awaiter(ctx, AT_FDCWD, path, 0755);
    EXPECT_TRUE(ret.has_value());

    struct stat st{};
    EXPECT_EQ(::stat(path, &st), 0);
    EXPECT_TRUE(S_ISDIR(st.st_mode));

    auto rm = co_await unlinkat_awaiter(ctx, AT_FDCWD, path, AT_REMOVEDIR);
    EXPECT_TRUE(rm.has_value());

    EXPECT_NE(::stat(path, &st), 0);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, renameat_file) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    const char* old_path = "/tmp/cornet_test_rename_old.txt";
    const char* new_path = "/tmp/cornet_test_rename_new.txt";

    int fd = ::open(old_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ::write(fd, "data", 4);
    ::close(fd);

    auto ret = co_await renameat_awaiter(ctx, AT_FDCWD, old_path, AT_FDCWD, new_path, 0);
    EXPECT_TRUE(ret.has_value());

    struct stat st{};
    EXPECT_NE(::stat(old_path, &st), 0);
    EXPECT_EQ(::stat(new_path, &st), 0);

    ::unlink(new_path);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(fs_io, fsync_file) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    const char* path = "/tmp/cornet_test_fsync.txt";
    int fd = ::open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    EXPECT_GE(fd, 0);
    if (fd < 0) co_return;
    ::write(fd, "sync", 4);

    auto ret = co_await fsync_awaiter(ctx, fd);
    EXPECT_TRUE(ret.has_value());

    ::close(fd);
    ::unlink(path);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}
