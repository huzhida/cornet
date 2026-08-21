# ─── Benchmark ───
find_package(asio CONFIG REQUIRED)

add_executable(bench EXCLUDE_FROM_ALL benchmark.cc)
target_include_directories(bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
    bench PRIVATE
    cornet
    asio::asio
    concurrentqueue::concurrentqueue
)

# The HTTP echo rows only exist when the http module is built; without them the
# bench still runs, it just has nothing to price the protocol against.
if(CORNET_WITH_HTTP)
    target_link_libraries(bench PRIVATE cornet_http)
    target_compile_definitions(bench PRIVATE CORNET_BENCH_HTTP)
    if(CORNET_WITH_TLS)
        target_compile_definitions(bench PRIVATE CORNET_BENCH_TLS)
        # bench 复用测试的内嵌 PKI（tests/tls/certs.h）
        target_include_directories(bench PRIVATE ${PROJECT_SOURCE_DIR}/tests/tls)
    endif()
endif()
