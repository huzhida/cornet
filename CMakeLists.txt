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
    src/context_cancellation.cc
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

# ─── 主程序 ───
add_executable(hello-cornet main.cc)
target_link_libraries(hello-cornet PRIVATE cornet)

# ─── Benchmark ───
option(CORNET_ENABLE_BENCH "Build the benchmark target (requires asio and libuv)" OFF)
if(CORNET_ENABLE_BENCH)
    find_package(asio CONFIG REQUIRED)
    find_package(libuv CONFIG REQUIRED)

    add_executable(bench EXCLUDE_FROM_ALL bench/benchmark.cc)
    target_include_directories(bench PRIVATE bench)
    target_link_libraries(
        bench PRIVATE
        cornet
        asio::asio
        concurrentqueue::concurrentqueue
        $<IF:$<TARGET_EXISTS:libuv::uv_a>,libuv::uv_a,libuv::uv>
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
    gtest_discover_tests(unit)
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