# core/common — shared C++ utilities used by node, agent, and ABI layers.

add_library(kvcache_common STATIC
    locator.cpp
    b3_facade.cpp        # BLAKE3 facade (real impl via BLAKE3::blake3)
    logging.cpp
    log_sink.cpp         # JSON-line ConsoleLogger backend (Phase O-1, merged O-2)
    metrics.cpp
    hashing.cpp
    trace.cpp            # OTel-shaped tracing facade (Phase J-1)
)

# Sibling lib that ships the OTLP/HTTP exporter (Phase J-2). Kept out of
# kvcache_common so callers who don't need OTel-collector wire
# (everything in-process, dev runs, unit tests) don't link libcurl /
# nlohmann_json transitively.
add_library(kvcache_otlp STATIC
    trace_otlp.cpp
)

target_include_directories(kvcache_otlp
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(kvcache_otlp
    PUBLIC
        kvcache_common
        CURL::libcurl
        nlohmann_json::nlohmann_json
)

target_include_directories(kvcache_common
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# BLAKE3 is consumed transitively by anything that links kvcache_common.
target_link_libraries(kvcache_common
    PUBLIC
        BLAKE3::blake3
)

# TODO(stephen): link spdlog, abseil, prometheus-cpp once those deps are
# wired in.
