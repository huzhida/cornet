cmake_minimum_required(VERSION 3.16)
project(cornet VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 所有库与可执行文件统一输出到构建目录根（与 add_subdirectory 拆分前的布局一致）
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

if(NOT UNIX)
    message(FATAL_ERROR "Only Unix is supported")
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(CornetTargetOptions)

# ─── 构建选项（集中声明，实现见各子目录） ───
# transport_t（明文/TLS 统一传输抽象）总是构建，但加密路径需要 OpenSSL。
option(CORNET_WITH_TLS "Build TLS support in the transport (requires OpenSSL)" ON)
# 独立 target：llhttp 私有链接进 cornet_http，公开头文件不出现 llhttp.h。
option(CORNET_WITH_HTTP "Build the cornet http module (requires llhttp)" ON)
# Phase tracing for the request path. Logs at INFO so it is visible without
# rebuilding spdlog's compile-time level; see include/cornet/http/common/trace.h.
option(CORNET_HTTP_TRACE "Log every http phase transition (accept/recv/parse/route/flush)" OFF)
# The client shares the module's common layer and brings no dependency of its
# own; the switch only exists to leave it out of a server-only build.
option(CORNET_HTTP_CLIENT "Build the http client submodule" ON)
option(CORNET_ENABLE_BENCH "Build the benchmark target (requires asio)" OFF)
option(CORNET_ENABLE_TESTS "Build the unit test target (requires GTest and asio)" OFF)
option(CORNET_BUILD_EXAMPLES "Build examples and hello-cornet (not needed for library consumers)" OFF)
option(CORNET_METRICS "Build with cornet metrics" OFF)
if(CORNET_METRICS)
    add_compile_definitions(CORNET_METRICS)
endif()

# ─── 依赖 ───
find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)

add_subdirectory(src)

if(CORNET_BUILD_EXAMPLES)
    if(CORNET_WITH_HTTP)
        add_subdirectory(examples)
    endif()
endif()
if(CORNET_ENABLE_BENCH)
    # 自定义二进制目录：子目录名 bench 会和输出在构建根的 bench 可执行文件冲突
    add_subdirectory(bench bench-build)
endif()
if(CORNET_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ─── 主程序 ───
if(CORNET_BUILD_EXAMPLES)
    add_executable(hello-cornet main.cc)
    target_link_libraries(hello-cornet PRIVATE cornet)
    if(CORNET_WITH_HTTP)
        target_link_libraries(hello-cornet PRIVATE cornet_http)
    endif()
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
