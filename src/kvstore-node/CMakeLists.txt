# kvstore-node — data-plane node binary, plus a reusable static library
# (`kvstore_node_core`) that bundles every TU except main.cpp. Both the node
# binary and the libkvcache.so in-process backend (see core-abi/) link
# against this static lib so subsystem code lives in exactly one place.

set(NODE_CORE_SRCS
    # tier
    src/tier/tier_manager.cpp
    src/tier/pinned_tier.cpp
    src/tier/dram_tier.cpp
    src/tier/nvme_tier.cpp
    src/tier/cold_tier.cpp
    # prefix
    src/prefix/art_index.cpp
    src/prefix/epoch.cpp
    src/prefix/lpm.cpp
    src/prefix/refcount.cpp
    src/prefix/kv_event_stream.cpp
    src/prefix/art_snapshot.cpp
    # ingest
    src/ingest/mutable_buffer.cpp
    src/ingest/watermark.cpp
    src/ingest/seal.cpp
    # transport
    src/transport/nixl_wrapper.cpp
    src/transport/tcp_backend.cpp
    src/transport/priority_scheduler.cpp
    # meta
    src/meta/rocksdb_store.cpp
    # qos
    src/qos/tenant.cpp
    src/qos/quota.cpp
    src/qos/priority_ctx.cpp
    # security
    src/security/mtls.cpp
    src/security/rbac.cpp
    src/security/audit.cpp
    # cluster
    src/cluster/etcd_client.cpp
    src/cluster/membership_fsm.cpp
    # routing
    src/routing/hrw.cpp
    src/routing/bloom_sketch.cpp
    # obs
    src/obs/metrics.cpp
    src/obs/logs.cpp
)

add_library(kvstore_node_core STATIC ${NODE_CORE_SRCS})

target_include_directories(kvstore_node_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvstore_node_core
    PUBLIC
        kvcache_common
    # TODO: rocksdb, nixl, ucx, uring, grpc++, protobuf, prometheus-cpp,
    # spdlog, abseil, openssl, $<IF:$<BOOL:${KVCACHE_ENABLE_SPDK}>,spdk,>.
)

# ---- Node binary ----
add_executable(kvstore-node src/main.cpp)
target_link_libraries(kvstore-node PRIVATE kvstore_node_core)
