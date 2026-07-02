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
    src/tier/rest_cold_tier.cpp
    src/tier/block_codec.cpp
    src/tier/compressing_cold_tier.cpp
    src/tier/encrypting_cold_tier.cpp
    src/tier/metrics_cold_tier.cpp
    src/tier/sigv4_transport.cpp
    src/tier/guarded_transport.cpp      # A10 — egress boundary guard decorator
    # codec
    src/codec/kv_tensor_codec.cpp
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
    src/transport/ucx_backend.cpp
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
    src/security/boundary_guard.cpp         # A10 — BoundaryGuard
    src/security/boundary_policy_builder.cpp # A10 — BuildPolicy
    src/security/regulated_mode.cpp         # A10 — ValidateRegulatedMode startup gate
    # cluster
    src/cluster/etcd_client.cpp
    src/cluster/http_etcd_client.cpp
    src/cluster/membership_fsm.cpp
    src/cluster/node_registrar.cpp
    src/cluster/node_directory.cpp
    src/cluster/bloom_publisher.cpp
    src/cluster/drain_gate.cpp
    src/cluster/identity_watcher.cpp
    # routing
    src/routing/hrw.cpp
    src/routing/bloom_sketch.cpp
    # obs — entire subdirectory removed in Phase O-3:
    #   - obs/logs.{h,cpp}    -> core/common/log_sink.{h,cpp} (Phase O-2)
    #   - obs/metrics.{h,cpp} -> deleted as dead code (Phase O-3);
    #     real metrics live in core/common/metrics.{h,cpp} under the
    #     kvcache::metrics namespace, pulled in transitively via
    #     kvcache_common.
    # runtime (Phase L-1)
    src/runtime/node_runtime.cpp
    src/runtime/regulated_flags.cpp         # A10 — Task 3: CLI flag → RegulatedModeConfig helper
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
    # TODO: rocksdb, nixl, ucx, prometheus-cpp, spdlog, abseil,
    # openssl, $<IF:$<BOOL:${KVCACHE_ENABLE_SPDK}>,spdk,>.
)

# Phase B1 — when KVCACHE_ENABLE_URING is on, link liburing + thread
# its include dirs through. The compile-time define KVCACHE_ENABLE_URING
# was added at the top level so the `#ifdef` in nvme_tier.cpp resolves.
if(KVCACHE_ENABLE_URING)
    target_include_directories(kvstore_node_core PUBLIC ${LIBURING_INCLUDE_DIRS})
    target_link_libraries(kvstore_node_core      PUBLIC ${LIBURING_LIBRARIES})
endif()

# Phase B3.1 — link libzstd when the cold-tier zstd codec is enabled. The
# compile-time KVCACHE_HAVE_ZSTD define (set at the top level) gates the
# ZstdCodec body in block_codec.cpp.
if(KVCACHE_ENABLE_ZSTD)
    target_link_libraries(kvstore_node_core PUBLIC PkgConfig::ZSTD)
endif()

# Phase A1 — link UCX when the ucx NIXL backend is enabled. The compile-time
# KVCACHE_HAVE_UCX define (set at the top level) gates the UcxBackend body in
# ucx_backend.cpp.
if(KVCACHE_ENABLE_UCX)
    target_link_libraries(kvstore_node_core PUBLIC PkgConfig::UCX)
endif()

# Phase A2 — link the CUDA runtime when the cudaHostAlloc pinned-tier path is
# enabled. KVCACHE_HAVE_CUDA (top level) gates the CUDA body in pinned_tier.cpp.
if(KVCACHE_ENABLE_CUDA)
    target_link_libraries(kvstore_node_core PUBLIC CUDA::cudart)
endif()

# Phase A3 — SPDK user-space NVMe backend. KVCACHE_HAVE_SPDK (top level) gates
# the SpdkImpl body in nvme_tier.cpp. SPDK_INCLUDE_DIR / SPDK_LINK_LIBS were
# computed at the top level (whole-archive wrapped).
if(KVCACHE_ENABLE_SPDK)
    target_include_directories(kvstore_node_core PUBLIC ${SPDK_INCLUDE_DIR})
    target_link_libraries(kvstore_node_core PUBLIC ${SPDK_LINK_LIBS})
endif()

# Phase B8 — security/mtls.cpp uses OpenSSL X.509 when available
# (KVCACHE_HAVE_OPENSSL defined at the top level). Link Crypto so the
# X509_*/PEM_*/GENERAL_NAME_* symbols resolve.
if(OpenSSL_FOUND)
    target_link_libraries(kvstore_node_core PUBLIC OpenSSL::Crypto)
endif()

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
            # Phase A11 — ReplicaFetch delegates to HeadlessNode::Active()
            # whose declaration lives in core-abi/src (not the public C ABI surface).
            ${CMAKE_SOURCE_DIR}/core-abi/src
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
