# ─── 测试 ───
# 目录结构镜像 src/：http/ 与 tls/ 各管各的，根上是核心库用例。
find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(unit EXCLUDE_FROM_ALL
    socket.cc
    expected.cc
    io_slot.cc
    generic_io.cc
    async.cc
    combinators.cc
    runtime.cc
    fs_io.cc
    frame_pool.cc
)
target_link_libraries(unit PRIVATE
    cornet
    concurrentqueue::concurrentqueue
    GTest::gtest_main
    GTest::gmock
)
# engine pair tests run without io_uring (pure BIO shuffle); with TLS off
# they still verify the Disabled error stays precise
target_sources(unit PRIVATE tls/engine.cc)
target_link_libraries(unit PRIVATE cornet_tls)

if(CORNET_WITH_HTTP)
    target_sources(unit PRIVATE
        http/common.cc
        http/buffer.cc
        http/parser.cc
        http/router.cc
        http/serializer.cc
        http/connection.cc
        http/e2e.cc
        http/url.cc
    )
    if(CORNET_HTTP_CLIENT)
        target_sources(unit PRIVATE
            http/client_parser.cc
            http/client_message.cc
            http/client_conn.cc
            http/client_pool.cc
            http/client_e2e.cc
        )
        if(CORNET_WITH_TLS)
            target_sources(unit PRIVATE tls/e2e.cc)
        endif()
    endif()
    target_sources(unit PRIVATE
        websocket/handshake.cc
        websocket/frame.cc
        websocket/e2e.cc
    )
    target_link_libraries(unit PRIVATE cornet_http)
endif()
gtest_discover_tests(unit)
