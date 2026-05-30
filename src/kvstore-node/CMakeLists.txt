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
    src/prefix/art_wal.cpp
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
    src/cluster/http_etcd_client.cpp
    src/cluster/membership_fsm.cpp
    src/cluster/node_registrar.cpp
    src/cluster/node_directory.cpp
    src/cluster/bloom_publisher.cpp
    # routing
    src/routing/hrw.cpp
    src/routing/bloom_sketch.cpp
    # obs
    src/obs/metrics.cpp
    # logs.cpp moved to core/common/log_sink.cpp in Phase O-2 — pulled in
    # transitively through kvcache_common.
    # runtime (Phase L-1)
    src/runtime/node_runtime.cpp
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
        CURL::libcurl
        nlohmann_json::nlohmann_json
    # TODO: rocksdb, nixl, ucx, uring, prometheus-cpp,
    # spdlog, abseil, openssl, $<IF:$<BOOL:${KVCACHE_ENABLE_SPDK}>,spdk,>.
)

# Phase F-2: when gRPC + the vendored etcd proto lib are available,
# enable the real GrpcEtcdClient by defining KVCACHE_HAVE_GRPC and
# linking the proto + grpc++ targets. Absent any of those, the
# `#else` branch in cluster/etcd_client.cpp keeps the stub alive.
if(KVCACHE_HAVE_GRPC)
    target_link_libraries(kvstore_node_core PUBLIC kvcache_etcd_proto)
    target_compile_definitions(kvstore_node_core PUBLIC KVCACHE_HAVE_GRPC=1)

    # Phase M-1: NodeData gRPC service + a thin Server wrapper. Built
    # only when grpc is present; otherwise the binary stays on the
    # NodeRuntime accept-and-close placeholder.
    add_library(kvstore_node_grpc STATIC
        src/grpc/node_data_service.cpp
        src/grpc/grpc_server.cpp
    )
    target_include_directories(kvstore_node_grpc
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/include
    )
    target_link_libraries(kvstore_node_grpc
        PUBLIC
            kvcache_proto
            gRPC::grpc++
            # Phase Q-1: NodeDataServiceImpl references HrwRing +
            # NodeDirectory for cross-node Lookup fan-out; both live
            # inside kvstore_node_core.
            kvstore_node_core
            # Phase Q-5: SHA-1 to derive the 16-byte tenant fingerprint
            # consistent with the Python connector's Locator.
            OpenSSL::Crypto
    )
endif()

# ---- Node binary ----
add_executable(kvstore-node src/main.cpp)
target_link_libraries(kvstore-node PRIVATE kvstore_node_core)
if(KVCACHE_HAVE_GRPC)
    # The binary owns the NodeData service + a GrpcServer wired to it,
    # plus the public C ABI shared lib so it can open a kv_ctx_t for
    # the default tenant. main.cpp gates the wiring on KVCACHE_HAVE_GRPC.
    target_link_libraries(kvstore-node PRIVATE
        kvstore_node_grpc
        kvcache
    )
    target_compile_definitions(kvstore-node PRIVATE KVCACHE_HAVE_GRPC=1)
endif()
