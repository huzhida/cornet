cmake_minimum_required(VERSION 3.16)
project(cornet VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT UNIX)
    message(FATAL_ERROR "Only Unix is supported")
endif()

# ─── 依赖 ───
find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)

# ─── 核心库 ───
add_library(cornet STATIC
    src/logging.cc
    src/task.cc
    src/cancel.cc
    src/executor.cc
    src/scheduler.cc
    src/context.cc
    src/uring.cc
    src/awaiters.cc
    src/socket.cc
    src/runtime.cc
)
add_library(cornet::cornet ALIAS cornet)
target_include_directories(
    cornet PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:cornet>
)
target_link_libraries(
    cornet PUBLIC
    PkgConfig::LIBURING
    spdlog::spdlog
    tomlplusplus::tomlplusplus
    concurrentqueue::concurrentqueue
)
target_compile_options(
    cornet PRIVATE
    $<$<NOT:$<CONFIG:Debug>>:-O2 -funroll-loops -march=native>
    $<$<CONFIG:Debug>:-fsanitize=address -g>
)
target_link_options(
    cornet PUBLIC
    $<$<CONFIG:Debug>:-fsanitize=address>
)

# ─── HTTP 模块 ───
# 独立 target：README 承诺「核心仅依赖 liburing」。llhttp 私有链接进 cornet_http，
# 且 llhttp.h 不出现在任何公开头文件中（parser_t 用不透明状态数组隔离依赖）
option(CORNET_WITH_HTTP "Build the cornet http module (requires llhttp)" ON)
# Phase tracing for the request path. Logs at INFO so it is visible without
# rebuilding spdlog's compile-time level; see include/cornet/http/trace.h.
option(CORNET_HTTP_TRACE "Log every http phase transition (accept/recv/parse/route/flush)" OFF)
if(CORNET_WITH_HTTP)
    find_package(llhttp CONFIG REQUIRED)

    add_library(cornet_http STATIC
        src/http/common.cc
        src/http/buffer.cc
        src/http/headers.cc
        src/http/parser.cc
        src/http/serializer.cc
        src/http/message.cc
        src/http/router.cc
        src/http/timer_wheel.cc
        src/http/connection.cc
        src/http/server.cc
    )
    add_library(cornet::http ALIAS cornet_http)
    target_link_libraries(cornet_http PUBLIC cornet PRIVATE llhttp::llhttp_static)
    if(CORNET_HTTP_TRACE)
        # PUBLIC: the macro also has to be defined in the translation unit that
        # includes the headers, or traced inline code silently stays disabled
        target_compile_definitions(cornet_http PUBLIC CORNET_HTTP_TRACE)
    endif()
    target_compile_options(
        cornet_http PRIVATE
        $<$<NOT:$<CONFIG:Debug>>:-O2 -funroll-loops -march=native>
        $<$<CONFIG:Debug>:-fsanitize=address -g>
    )

    # Built by default on purpose: it is the executable form of the "5 lines to a
    # server" claim, so a regression in the public API breaks the build.
    add_executable(http-hello examples/http_hello.cc)
    target_link_libraries(http-hello PRIVATE cornet_http)
endif()

# ─── 主程序 ───
add_executable(hello-cornet main.cc)
target_link_libraries(hello-cornet PRIVATE cornet cornet_http)

# ─── Benchmark ───
option(CORNET_ENABLE_BENCH "Build the benchmark target (requires asio)" OFF)
if(CORNET_ENABLE_BENCH)
    find_package(asio CONFIG REQUIRED)

    add_executable(bench EXCLUDE_FROM_ALL bench/benchmark.cc)
    target_include_directories(bench PRIVATE bench)
    target_link_libraries(
        bench PRIVATE
        cornet
        asio::asio
        concurrentqueue::concurrentqueue
    )
endif()

# ─── 测试 ───
option(CORNET_ENABLE_TESTS "Build the unit test target (requires GTest and asio)" OFF)
if(CORNET_ENABLE_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(unit EXCLUDE_FROM_ALL
        tests/socket.cc
        tests/expected.cc
        tests/io_slot.cc
        tests/generic_io.cc
        tests/async.cc
        tests/combinators.cc
        tests/runtime.cc
        tests/fs_io.cc
    )
    target_link_libraries(unit PRIVATE
        cornet
        concurrentqueue::concurrentqueue
        GTest::gtest_main
        GTest::gmock
    )
    if(CORNET_WITH_HTTP)
        target_sources(unit PRIVATE
            tests/http_common.cc
            tests/http_buffer.cc
            tests/http_parser.cc
            tests/http_router.cc
            tests/http_serializer.cc
            tests/http_connection.cc
            tests/http_e2e.cc
        )
        target_link_libraries(unit PRIVATE cornet_http)
    endif()
    gtest_discover_tests(unit)
endif()

# ─── metrics ───
option(CORNET_METRICS "Build with cornet metrics" OFF)
if(CORNET_METRICS)
    add_compile_definitions(CORNET_METRICS)
endif()

# ─── 安装 ───
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(
    TARGETS cornet
    EXPORT cornetTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(
    DIRECTORY include/ 
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
    EXPORT cornetTargets
    NAMESPACE cornet::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cornet
)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/cornetConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/cornetConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cornet
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/cornetConfigVersion.cmake
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cornetConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/cornetConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cornet
)

# cmake --build --preset debug/release