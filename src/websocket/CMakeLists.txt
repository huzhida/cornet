# ─── WebSocket 模块 ───
# Sources compile straight into cornet_http rather than a target of their own:
# the server's upgrade hook lives in http/connection.cc while the client reuses
# the http parser and url code — two libraries would need each other in a cycle.
# Hand-rolled SHA-1/base64 keeps the module dependency-free (llhttp treatment).
target_sources(cornet_http PRIVATE
    common/frame.cc
    common/handshake.cc
    session.cc
    server.cc
    client.cc
)
