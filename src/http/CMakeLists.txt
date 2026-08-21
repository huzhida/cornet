# ─── HTTP 模块 ───
# 独立 target：llhttp 私有链接进 cornet_http，且 llhttp.h 不出现在任何公开
# 头文件中（parser_t 用不透明状态数组隔离依赖），避免传递依赖给核心库用户
find_package(llhttp CONFIG REQUIRED)

# Sources mirror include/cornet/http: common/ is shared by both directions,
# server/ and client/ hold what only one of them needs.
add_library(cornet_http STATIC
    common/protocol.cc
    common/buffer.cc
    common/headers.cc
    common/parser.cc
    common/serializer.cc
    common/url.cc
    server/message.cc
    server/router.cc
    server/connection.cc
    server/server.cc
)
if(CORNET_HTTP_CLIENT)
    target_sources(cornet_http PRIVATE
        client/message.cc
        client/connection.cc
        client/pool.cc
        client/client.cc
    )
endif()
add_library(cornet::http ALIAS cornet_http)
# PUBLIC cornet_tls: connection_t speaks transport_t, so the type must be
# reachable through cornet_http's public interface; OpenSSL itself stays
# private inside cornet_tls
target_link_libraries(cornet_http PUBLIC cornet cornet_tls PRIVATE llhttp::llhttp_static)
if(CORNET_HTTP_TRACE)
    # PUBLIC: the macro also has to be defined in the translation unit that
    # includes the headers, or traced inline code silently stays disabled
    target_compile_definitions(cornet_http PUBLIC CORNET_HTTP_TRACE)
endif()
cornet_target_opts(cornet_http)
