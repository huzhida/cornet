# ─── 核心库 ───
add_library(cornet STATIC
    logging.cc
    task.cc
    cancel.cc
    executor.cc
    scheduler.cc
    context.cc
    uring.cc
    awaiters.cc
    socket.cc
    runtime.cc
    timer_wheel.cc
)
add_library(cornet::cornet ALIAS cornet)
target_include_directories(
    cornet PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:cornet>
)
target_link_libraries(
    cornet PUBLIC
    PkgConfig::LIBURING
    spdlog::spdlog
    tomlplusplus::tomlplusplus
    concurrentqueue::concurrentqueue
)
cornet_target_opts(cornet)

add_subdirectory(tls)

if(CORNET_WITH_HTTP)
    add_subdirectory(http)
endif()
