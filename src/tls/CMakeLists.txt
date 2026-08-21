# ─── TLS 模块 ───
# transport_t（明文/TLS 统一传输抽象）总是构建，但加密路径需要 OpenSSL。
# 与 llhttp 同策略：OpenSSL 私有链接，公开头文件不出现 openssl 类型
# （tls_context_t/tls_engine_t 均为 pimpl），CORNET_WITH_TLS=OFF 时
# start_tls() 在运行时返回 tls_error_t::Disabled 而不是链接失败。
add_library(cornet_tls STATIC
    error.cc
    context.cc
    engine.cc
    stream.cc
    transport.cc
)
add_library(cornet::tls ALIAS cornet_tls)
target_link_libraries(cornet_tls PUBLIC cornet)
if(CORNET_WITH_TLS)
    find_package(OpenSSL REQUIRED)
    # PRIVATE: callers never call OpenSSL themselves, and this target is the
    # only thing that should
    target_link_libraries(cornet_tls PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    # PUBLIC: like CORNET_HTTP_TRACE, the flag decides what headers declare,
    # so every TU that includes them must agree on it
    target_compile_definitions(cornet_tls PUBLIC CORNET_WITH_TLS)
endif()
cornet_target_opts(cornet_tls)
