# ─── 示例 ───
# Built by default on purpose: it is the executable form of the "5 lines to a
# server" claim, so a regression in the public API breaks the build.
add_executable(http-hello EXCLUDE_FROM_ALL http_hello.cc)
target_link_libraries(http-hello PRIVATE cornet_http)

# Same "regression in the public API breaks the build" discipline as http-hello
add_executable(websocket-echo EXCLUDE_FROM_ALL websocket_echo.cc)
target_link_libraries(websocket-echo PRIVATE cornet_http)

if(CORNET_HTTP_CLIENT)
    # Built by default for the same reason as http-hello: it is the executable
    # form of the client's public API, so a regression there breaks the build.
    add_executable(http-client EXCLUDE_FROM_ALL http_client.cc)
    target_link_libraries(http-client PRIVATE cornet_http)
endif()

if(CORNET_WITH_TLS)
    add_executable(https-hello EXCLUDE_FROM_ALL https_hello.cc)
    target_link_libraries(https-hello PRIVATE cornet_http)
endif()
