# Unit tests — gtest. The parent CMakeLists has already resolved GTest::gtest
# (via find_package or FetchContent), so the targets are guaranteed to exist
# by the time we get here.

include(GoogleTest)

set(KVCACHE_NODE_SRC ${CMAKE_SOURCE_DIR}/kvstore-node/src)

# Sources under test. We compile the prefix/ + meta/ TUs directly into each
# test binary so the tests don't need to depend on the full kvstore-node
# executable's link graph.
set(NODE_PREFIX_SRCS
    ${KVCACHE_NODE_SRC}/prefix/art_index.cpp
    ${KVCACHE_NODE_SRC}/prefix/epoch.cpp
    ${KVCACHE_NODE_SRC}/prefix/lpm.cpp
    ${KVCACHE_NODE_SRC}/prefix/kv_event_stream.cpp
    ${KVCACHE_NODE_SRC}/prefix/refcount.cpp
    ${KVCACHE_NODE_SRC}/prefix/art_snapshot.cpp
    ${KVCACHE_NODE_SRC}/prefix/art_wal.cpp
)

set(NODE_META_SRCS
    ${KVCACHE_NODE_SRC}/meta/rocksdb_store.cpp
)

set(NODE_TIER_SRCS
    ${KVCACHE_NODE_SRC}/tier/pinned_tier.cpp
    ${KVCACHE_NODE_SRC}/tier/dram_tier.cpp
    ${KVCACHE_NODE_SRC}/tier/nvme_tier.cpp
    ${KVCACHE_NODE_SRC}/tier/cold_tier.cpp
    ${KVCACHE_NODE_SRC}/tier/rest_cold_tier.cpp  # B3 — native-rest backend (libcurl)
    ${KVCACHE_NODE_SRC}/tier/block_codec.cpp           # B3.1 — codec seam
    ${KVCACHE_NODE_SRC}/tier/compressing_cold_tier.cpp # B3.1 — compression middleware
    ${KVCACHE_NODE_SRC}/tier/encrypting_cold_tier.cpp  # B3.2 — encryption middleware (OpenSSL)
    ${KVCACHE_NODE_SRC}/tier/metrics_cold_tier.cpp     # O-4  — observability middleware
    ${KVCACHE_NODE_SRC}/tier/sigv4_transport.cpp       # B5   — SigV4 signing decorator (OpenSSL)
    ${KVCACHE_NODE_SRC}/tier/guarded_transport.cpp     # A10  — egress boundary guard decorator
    ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp    # A10  — BoundaryGuard (referenced by guarded_transport)
    ${KVCACHE_NODE_SRC}/tier/tier_manager.cpp
)

set(NODE_INGEST_SRCS
    ${KVCACHE_NODE_SRC}/ingest/mutable_buffer.cpp
    ${KVCACHE_NODE_SRC}/ingest/watermark.cpp
    ${KVCACHE_NODE_SRC}/ingest/seal.cpp
)

set(NODE_TRANSPORT_SRCS
    ${KVCACHE_NODE_SRC}/transport/nixl_wrapper.cpp
    ${KVCACHE_NODE_SRC}/transport/tcp_backend.cpp
    ${KVCACHE_NODE_SRC}/transport/ucx_backend.cpp  # A1 — CreateUcxBackend (factory ref)
    ${KVCACHE_NODE_SRC}/transport/priority_scheduler.cpp
)

set(NODE_CLUSTER_SRCS
    ${KVCACHE_NODE_SRC}/cluster/etcd_client.cpp
    ${KVCACHE_NODE_SRC}/cluster/http_etcd_client.cpp
    ${KVCACHE_NODE_SRC}/cluster/membership_fsm.cpp
    ${KVCACHE_NODE_SRC}/cluster/node_registrar.cpp
    ${KVCACHE_NODE_SRC}/cluster/node_directory.cpp
    ${KVCACHE_NODE_SRC}/cluster/bloom_publisher.cpp
    ${KVCACHE_NODE_SRC}/cluster/drain_gate.cpp
    # NOTE: identity_watcher.cpp is intentionally NOT here — it depends on
    # security::MtlsRegistry, and the shared cluster-test group must not
    # drag security symbols into every cluster test. test_identity_watcher
    # compiles it explicitly alongside NODE_SECURITY_SRCS below.
)

set(NODE_ROUTING_SRCS
    ${KVCACHE_NODE_SRC}/routing/hrw.cpp
    ${KVCACHE_NODE_SRC}/routing/bloom_sketch.cpp
)

set(NODE_QOS_SRCS
    ${KVCACHE_NODE_SRC}/qos/tenant.cpp
    ${KVCACHE_NODE_SRC}/qos/quota.cpp
    ${KVCACHE_NODE_SRC}/qos/priority_ctx.cpp
)

set(NODE_SECURITY_SRCS
    ${KVCACHE_NODE_SRC}/security/mtls.cpp
    ${KVCACHE_NODE_SRC}/security/rbac.cpp
    ${KVCACHE_NODE_SRC}/security/audit.cpp
)

function(kvcache_add_test name)
    add_executable(${name} ${ARGN})
    target_include_directories(${name} PRIVATE
        ${KVCACHE_NODE_SRC}
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/core/common
    )
    # gtest_main already transitively depends on gtest, so listing both pulls
    # in libgtest.a twice on macOS (ld warns "ignoring duplicate libraries").
    target_link_libraries(${name} PRIVATE kvcache_common GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

kvcache_add_test(test_refcount         prefix/refcount_test.cpp         ${NODE_PREFIX_SRCS})
kvcache_add_test(test_event_stream     prefix/event_stream_test.cpp     ${NODE_PREFIX_SRCS})
kvcache_add_test(test_art_index        prefix/art_index_test.cpp        ${NODE_PREFIX_SRCS})
kvcache_add_test(test_lpm              prefix/lpm_test.cpp              ${NODE_PREFIX_SRCS})
kvcache_add_test(test_epoch            prefix/epoch_test.cpp            ${NODE_PREFIX_SRCS})
kvcache_add_test(test_art_concurrent   prefix/art_concurrent_test.cpp   ${NODE_PREFIX_SRCS})
kvcache_add_test(test_art_snapshot     prefix/art_snapshot_test.cpp     ${NODE_PREFIX_SRCS})
kvcache_add_test(test_art_wal          prefix/art_wal_test.cpp          ${NODE_PREFIX_SRCS})
kvcache_add_test(test_rocksdb_store    meta/rocksdb_store_test.cpp      ${NODE_META_SRCS})
if(KVCACHE_ENABLE_ROCKSDB)
    target_link_libraries(test_rocksdb_store PRIVATE RocksDB::rocksdb)
endif()

kvcache_add_test(test_pinned_tier      tier/pinned_tier_test.cpp        ${NODE_TIER_SRCS})
kvcache_add_test(test_dram_tier        tier/dram_tier_test.cpp          ${NODE_TIER_SRCS})
kvcache_add_test(test_nvme_tier        tier/nvme_tier_test.cpp          ${NODE_TIER_SRCS})
kvcache_add_test(test_cold_tier        tier/cold_tier_test.cpp          ${NODE_TIER_SRCS})
kvcache_add_test(test_tier_manager     tier/tier_manager_test.cpp       ${NODE_TIER_SRCS})

# B3 — rest_cold_tier.cpp (compiled into NODE_TIER_SRCS via the factory) links
# libcurl. Every target that pulls NODE_TIER_SRCS must link it.
kvcache_add_test(test_rest_cold_tier   tier/rest_cold_tier_test.cpp     ${NODE_TIER_SRCS})
kvcache_add_test(test_compressing_cold_tier tier/compressing_cold_tier_test.cpp ${NODE_TIER_SRCS})
kvcache_add_test(test_encrypting_cold_tier  tier/encrypting_cold_tier_test.cpp  ${NODE_TIER_SRCS})
kvcache_add_test(test_metrics_cold_tier     tier/metrics_cold_tier_test.cpp     ${NODE_TIER_SRCS})
kvcache_add_test(test_sigv4_transport       tier/sigv4_transport_test.cpp       ${NODE_TIER_SRCS})
foreach(_tier_t test_pinned_tier test_dram_tier test_nvme_tier test_cold_tier test_tier_manager test_rest_cold_tier test_compressing_cold_tier test_encrypting_cold_tier test_metrics_cold_tier test_sigv4_transport)
    target_link_libraries(${_tier_t} PRIVATE CURL::libcurl)
    # B3.1 — block_codec.cpp pulls libzstd when the codec is enabled.
    if(KVCACHE_ENABLE_ZSTD)
        target_link_libraries(${_tier_t} PRIVATE PkgConfig::ZSTD)
    endif()
    # B3.2 — encrypting_cold_tier.cpp pulls OpenSSL when available.
    if(OpenSSL_FOUND)
        target_link_libraries(${_tier_t} PRIVATE OpenSSL::Crypto)
    endif()
    # A2 — pinned_tier.cpp pulls the CUDA runtime when the cudaHostAlloc path
    # is enabled.
    if(KVCACHE_ENABLE_CUDA)
        target_link_libraries(${_tier_t} PRIVATE CUDA::cudart)
    endif()
    # A3 — nvme_tier.cpp pulls SPDK when the user-space NVMe path is enabled.
    if(KVCACHE_ENABLE_SPDK)
        target_include_directories(${_tier_t} PRIVATE ${SPDK_INCLUDE_DIR})
        target_link_libraries(${_tier_t} PRIVATE ${SPDK_LINK_LIBS})
    endif()
endforeach()

# KVZ-1 — KvTensorCodec (KV-tensor compression). Standalone; the zstd
# entropy stage is gated, so the test links libzstd only when enabled.
kvcache_add_test(test_kv_tensor_codec  codec/kv_tensor_codec_test.cpp
                 ${KVCACHE_NODE_SRC}/codec/kv_tensor_codec.cpp)
if(KVCACHE_ENABLE_ZSTD)
    target_link_libraries(test_kv_tensor_codec PRIVATE PkgConfig::ZSTD)
endif()

# NODE_INGEST_SRCS contains seal.cpp which depends on meta + prefix, so the
# ingest-side tests all need meta + prefix sources to link cleanly.
kvcache_add_test(test_mutable_buffer   ingest/mutable_buffer_test.cpp
                 ${NODE_INGEST_SRCS} ${NODE_TIER_SRCS}
                 ${NODE_META_SRCS}   ${NODE_PREFIX_SRCS})
kvcache_add_test(test_watermark        ingest/watermark_test.cpp
                 ${NODE_INGEST_SRCS} ${NODE_TIER_SRCS}
                 ${NODE_META_SRCS}   ${NODE_PREFIX_SRCS})
kvcache_add_test(test_seal             ingest/seal_test.cpp
                 ${NODE_INGEST_SRCS} ${NODE_TIER_SRCS}
                 ${NODE_META_SRCS}   ${NODE_PREFIX_SRCS})
if(KVCACHE_ENABLE_ROCKSDB)
    target_link_libraries(test_seal PRIVATE RocksDB::rocksdb)
endif()
# B3 — these pull NODE_TIER_SRCS (rest_cold_tier.cpp) → need libcurl;
# B3.1 — block_codec.cpp pulls libzstd when the codec is enabled.
foreach(_ing_t test_mutable_buffer test_watermark test_seal)
    target_link_libraries(${_ing_t} PRIVATE CURL::libcurl)
    if(KVCACHE_ENABLE_ZSTD)
        target_link_libraries(${_ing_t} PRIVATE PkgConfig::ZSTD)
    endif()
    # B3.2 — encrypting_cold_tier.cpp (in NODE_TIER_SRCS) needs OpenSSL.
    if(OpenSSL_FOUND)
        target_link_libraries(${_ing_t} PRIVATE OpenSSL::Crypto)
    endif()
    # A2 — pinned_tier.cpp (in NODE_TIER_SRCS) needs the CUDA runtime.
    if(KVCACHE_ENABLE_CUDA)
        target_link_libraries(${_ing_t} PRIVATE CUDA::cudart)
    endif()
    # A3 — nvme_tier.cpp (in NODE_TIER_SRCS) needs SPDK when enabled.
    if(KVCACHE_ENABLE_SPDK)
        target_include_directories(${_ing_t} PRIVATE ${SPDK_INCLUDE_DIR})
        target_link_libraries(${_ing_t} PRIVATE ${SPDK_LINK_LIBS})
    endif()
endforeach()

kvcache_add_test(test_nixl_wrapper      transport/nixl_wrapper_test.cpp      ${NODE_TRANSPORT_SRCS})
kvcache_add_test(test_priority_scheduler transport/priority_scheduler_test.cpp ${NODE_TRANSPORT_SRCS})
kvcache_add_test(test_tcp_backend       transport/tcp_backend_test.cpp       ${NODE_TRANSPORT_SRCS})
kvcache_add_test(test_ucx_backend       transport/ucx_backend_test.cpp       ${NODE_TRANSPORT_SRCS})
# A1 — every transport test links ucx_backend.cpp (factory ref). When the UCX
# backend is enabled, link the UCX lib + propagate KVCACHE_HAVE_UCX so the test
# exercises the real ucp path; otherwise the stub compiles and the test
# asserts the "not compiled in" contract.
if(KVCACHE_ENABLE_UCX)
    foreach(_tr_t test_nixl_wrapper test_priority_scheduler test_tcp_backend test_ucx_backend)
        target_link_libraries(${_tr_t} PRIVATE PkgConfig::UCX)
    endforeach()
endif()

kvcache_add_test(test_etcd_client       cluster/etcd_client_test.cpp         ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_etcd_client PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_membership_fsm    cluster/membership_fsm_test.cpp      ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_membership_fsm PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_http_etcd_client  cluster/http_etcd_client_test.cpp    ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_http_etcd_client PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_grpc_etcd_client  cluster/grpc_etcd_client_test.cpp    ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_grpc_etcd_client PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
if(KVCACHE_HAVE_GRPC)
    target_link_libraries(test_grpc_etcd_client PRIVATE kvcache_etcd_proto)
    target_compile_definitions(test_grpc_etcd_client PRIVATE KVCACHE_HAVE_GRPC=1)
endif()
kvcache_add_test(test_hrw               routing/hrw_test.cpp                 ${NODE_ROUTING_SRCS})

# Phase Q-1: NodeRegistrar + NodeDirectory cooperate to publish + observe
# /kvcache/nodes/ entries on etcd. The test fixture uses InMemoryEtcdClient,
# so the binary links cluster + routing TUs (no libcurl needed).
kvcache_add_test(test_node_registry     cluster/node_registry_test.cpp
                  ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_node_registry PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)

# Phase K-5: BloomPublisher + NodeDirectory sketch fan-out.
kvcache_add_test(test_bloom_publisher   cluster/bloom_publisher_test.cpp
                  ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_bloom_publisher PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_bloom_sketch      routing/bloom_sketch_test.cpp        ${NODE_ROUTING_SRCS})

# Phase A2.2: DrainGate + DrainWatcher over InMemoryEtcdClient.
kvcache_add_test(test_drain_gate        cluster/drain_gate_test.cpp          ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS})
target_link_libraries(test_drain_gate PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)

# Phase B8.3: IdentityWatcher syncs the MtlsRegistry from etcd.
# Compiles identity_watcher.cpp explicitly (it's NOT in NODE_CLUSTER_SRCS
# to keep security symbols out of the other cluster tests) + the security
# sources it depends on.
kvcache_add_test(test_identity_watcher  cluster/identity_watcher_test.cpp
                 ${KVCACHE_NODE_SRC}/cluster/identity_watcher.cpp
                 ${NODE_CLUSTER_SRCS} ${NODE_ROUTING_SRCS} ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
target_link_libraries(test_identity_watcher PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
if(OpenSSL_FOUND)
    target_link_libraries(test_identity_watcher PRIVATE OpenSSL::Crypto)
endif()

kvcache_add_test(test_quota             qos/quota_test.cpp                   ${NODE_QOS_SRCS})
kvcache_add_test(test_tenant            qos/tenant_test.cpp                  ${NODE_QOS_SRCS})

kvcache_add_test(test_mtls              security/mtls_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_rbac              security/rbac_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_audit             security/audit_test.cpp              ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
# Phase B8 — security TUs include security/mtls.cpp, which needs
# OpenSSL::Crypto when KVCACHE_HAVE_OPENSSL is defined. These tests
# compile the security sources directly (not via kvstore_node_core),
# so link Crypto explicitly here.
if(OpenSSL_FOUND)
    target_link_libraries(test_mtls  PRIVATE OpenSSL::Crypto)
    target_link_libraries(test_rbac  PRIVATE OpenSSL::Crypto)
    target_link_libraries(test_audit PRIVATE OpenSSL::Crypto)
endif()

# A10 — BoundaryGuard: fail-closed perimeter decision (glob/CIDR/default-deny).
# Pure stdlib; no extra link deps beyond kvcache_common (already added by
# kvcache_add_test). boundary_guard.cpp is compiled directly into the binary.
kvcache_add_test(test_boundary_guard
                  security/boundary_guard_test.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — BoundaryPolicyBuilder: classify operator allow-rules into host-glob vs CIDR.
# Round-trips through BoundaryGuard, so compiles both boundary_policy_builder.cpp
# and boundary_guard.cpp.
kvcache_add_test(test_boundary_policy_builder
                  security/boundary_policy_builder_test.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_policy_builder.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — RegulatedMode: fail-closed startup gate that checks every configured
# egress sink against the operator allowlist and runs injectable requirement
# predicates (FIPS / KMS / audit seam). Off by default → no-op.
kvcache_add_test(test_regulated_mode
                  security/regulated_mode_test.cpp
                  ${KVCACHE_NODE_SRC}/security/regulated_mode.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_policy_builder.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — GuardedHttpTransport: fail-closed boundary check on the cold-tier egress seam.
# IHttpTransport / HttpResult are header-only (pure virtual + POD in rest_cold_tier.h),
# so no libcurl dependency is required for this decorator + unit test alone.
kvcache_add_test(test_guarded_transport
                  tier/guarded_transport_test.cpp
                  ${KVCACHE_NODE_SRC}/tier/guarded_transport.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — Task 1: BoundaryGuard threaded into the cold-tier factory.
# Tests that CreateColdTier wraps the native-rest transport in GuardedHttpTransport
# when opts.guard is set. Links NODE_TIER_SRCS (which now includes
# guarded_transport.cpp + boundary_guard.cpp). libcurl is required because
# NODE_TIER_SRCS includes rest_cold_tier.cpp.
kvcache_add_test(test_cold_tier_guard
                  tier/cold_tier_guard_test.cpp
                  ${NODE_TIER_SRCS})
target_link_libraries(test_cold_tier_guard PRIVATE CURL::libcurl)
if(KVCACHE_ENABLE_ZSTD)
    target_link_libraries(test_cold_tier_guard PRIVATE PkgConfig::ZSTD)
endif()
if(OpenSSL_FOUND)
    target_link_libraries(test_cold_tier_guard PRIVATE OpenSSL::Crypto)
endif()
if(KVCACHE_ENABLE_CUDA)
    target_link_libraries(test_cold_tier_guard PRIVATE CUDA::cudart)
endif()
if(KVCACHE_ENABLE_SPDK)
    target_include_directories(test_cold_tier_guard PRIVATE ${SPDK_INCLUDE_DIR})
    target_link_libraries(test_cold_tier_guard PRIVATE ${SPDK_LINK_LIBS})
endif()

# A10 — BoundaryDenyObserver: factory that builds a DenyObserver wired to the
# boundary-deny metric counter and the AuditLog. boundary_deny_observer.h is
# header-only; the test links NODE_SECURITY_SRCS + NODE_QOS_SRCS so AuditLog +
# rbac + mtls + TenantHash resolve (same pattern as test_audit).
kvcache_add_test(test_boundary_deny_observer
                  security/boundary_deny_observer_test.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp
                  ${NODE_SECURITY_SRCS}
                  ${NODE_QOS_SRCS})
if(OpenSSL_FOUND)
    target_link_libraries(test_boundary_deny_observer PRIVATE OpenSSL::Crypto)
endif()

kvcache_add_test(test_metrics           obs/metrics_test.cpp)
# LLD §6.2 — logging facade. Foundation only; per-callsite migration
# of fprintf/cerr → KV_LOG_* is per-file follow-on work.
kvcache_add_test(test_logs              common/log_sink_test.cpp)
# Phase W-1 — Locator wire (de)serialization + canonical LE byte layout.
kvcache_add_test(test_locator           common/locator_test.cpp)
# SS-2 spine spike, Task 1 — StateIdentity (128B) + KV projection. Header-only;
# needs only kv_types.h + core/common include paths (already provided by
# kvcache_add_test).
kvcache_add_test(test_state_identity    common/state_identity_test.cpp)
# Phase W-2 — canonical FNV-1a-64 golden vectors (cross-language lock).
kvcache_add_test(test_hashing           common/hashing_test.cpp)
kvcache_add_test(test_node_runtime      runtime/node_runtime_test.cpp
                 ${KVCACHE_NODE_SRC}/runtime/node_runtime.cpp
                 ${KVCACHE_NODE_SRC}/security/regulated_mode.cpp
                 ${KVCACHE_NODE_SRC}/security/boundary_policy_builder.cpp
                 ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — Task 2: Regulated Mode startup gate in NodeRuntime.
# Verifies that NodeRuntime refuses to bind when Regulated Mode is enabled and
# a configured sink is out of boundary. Links the security TUs needed by
# ValidateRegulatedMode (regulated_mode.cpp → boundary_policy_builder.cpp →
# boundary_guard.cpp). node_runtime.cpp is compiled directly (not via kvcache
# dylib) so the constructor changes are exercised without a full link graph.
kvcache_add_test(test_node_runtime_regulated
                  runtime/node_runtime_regulated_test.cpp
                  ${KVCACHE_NODE_SRC}/runtime/node_runtime.cpp
                  ${KVCACHE_NODE_SRC}/security/regulated_mode.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_policy_builder.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# A10 — Task 3: RegulatedFlags CLI-flag helper.
# Tests BuildRegulatedModeConfig (the pure flag→config converter that keeps
# main.cpp thin). Links regulated_flags.cpp + the security TUs needed by
# ValidateRegulatedMode (regulated_mode.cpp → boundary_policy_builder.cpp →
# boundary_guard.cpp) so the test can call ValidateRegulatedMode for sanity.
kvcache_add_test(test_regulated_flags
                  runtime/regulated_flags_test.cpp
                  ${KVCACHE_NODE_SRC}/runtime/regulated_flags.cpp
                  ${KVCACHE_NODE_SRC}/security/regulated_mode.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_policy_builder.cpp
                  ${KVCACHE_NODE_SRC}/security/boundary_guard.cpp)

# Phase G-3 — Reserve backpressure surfaces through /metrics. Links
# the C ABI shared lib (kvcache) so it drives the singleton through
# its real surface.
kvcache_add_test(test_reserve_backpressure
                  abi/reserve_backpressure_test.cpp)
target_link_libraries(test_reserve_backpressure PRIVATE kvcache)

# Phase G-6 — alert-fires-when-expected smoke tests. Drives the
# C ABI to force each G-5 alert's condition, then asserts the
# underlying gauges/counters crossed the threshold the
# PrometheusRule expression checks.
kvcache_add_test(test_g6_alert_thresholds
                  abi/g6_alert_thresholds_test.cpp)
target_link_libraries(test_g6_alert_thresholds PRIVATE kvcache)

# Phase KVZ-2 — KV-tensor codec exposed through the C ABI.
kvcache_add_test(test_kvtensor_abi abi/kvtensor_abi_test.cpp)
target_link_libraries(test_kvtensor_abi PRIVATE kvcache)

# Phase KVZ-3 — kv_lookup_stored_bytes.
kvcache_add_test(test_stored_bytes abi/stored_bytes_test.cpp)
target_link_libraries(test_stored_bytes PRIVATE kvcache)

# Phase ABI-1 — BuildCtxOptions is pure (no HeadlessNode singleton), so the
# test compiles the TU directly instead of linking the kvcache dylib. Needs
# the core-abi + kvstore-node src include paths for ctx_options.h /
# headless_node.h (Options struct only — no node methods are called).
kvcache_add_test(test_ctx_options
                  abi/ctx_options_test.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/ctx_options.cpp)
target_include_directories(test_ctx_options PRIVATE
                  ${CMAKE_SOURCE_DIR}/core-abi/src
                  ${CMAKE_SOURCE_DIR}/kvstore-node/src)

# Phase M-1: gRPC NodeData service tests. Only built when grpc is
# present (the service impl + its headers don't exist otherwise).
if(KVCACHE_HAVE_GRPC)
    kvcache_add_test(test_node_data_service
                      grpc/node_data_service_test.cpp)
    target_link_libraries(test_node_data_service PRIVATE
        kvstore_node_grpc
        kvcache
        # Phase Q-5: BuildLocator now uses SHA-1 to mirror the
        # Python connector's tenant_id derivation.
        OpenSSL::Crypto
    )

    # Phase A9 / A11 — ReplicaFetch gRPC handler: VerifyInternalPeer gate
    # + delegate to HeadlessNode. HeadlessNode C++ methods are hidden-visibility
    # in the kvcache dylib, so we compile headless_node.cpp directly (same
    # pattern as test_replica_fetch and test_replication_consumer). This also
    # supplies the HeadlessNode::Active() symbol needed by node_data_service.cpp's
    # ReplicaFetch handler when it is linked into this test binary via
    # kvstore_node_grpc.
    kvcache_add_test(test_replica_fetch_rpc
                      grpc/replica_fetch_rpc_test.cpp
                      ${CMAKE_SOURCE_DIR}/core-abi/src/headless_node.cpp
                      ${CMAKE_SOURCE_DIR}/core-abi/src/ctx_options.cpp
                      ${CMAKE_SOURCE_DIR}/core-abi/src/kv_status.cpp)
    target_compile_definitions(test_replica_fetch_rpc PRIVATE KVCACHE_HAVE_GRPC=1)
    target_include_directories(test_replica_fetch_rpc PRIVATE
        ${CMAKE_SOURCE_DIR}/core-abi/src
        ${KVCACHE_NODE_SRC}
    )
    target_link_libraries(test_replica_fetch_rpc PRIVATE
        kvstore_node_grpc
        kvcache
        kvstore_node_core
        kvcache_common
        OpenSSL::Crypto
        CURL::libcurl
    )

    # Phase N-1: mTLS end-to-end. Skips at runtime if openssl(1) isn't
    # on PATH, but still gets built so the credentials surface stays
    # compile-tested.
    kvcache_add_test(test_grpc_server_tls
                      grpc/grpc_server_tls_test.cpp)
    target_link_libraries(test_grpc_server_tls PRIVATE
        kvstore_node_grpc
        kvcache
    )

    # Phase Q-1: cross-node Lookup fan-out via real gRPC + in-memory
    # etcd. Stands up two NodeDataService instances in-process, both
    # registered with the directory; verifies non-primary forwards.
    kvcache_add_test(test_lookup_forwarding
                      grpc/lookup_forwarding_test.cpp)
    target_link_libraries(test_lookup_forwarding PRIVATE
        kvstore_node_grpc
        kvcache
        CURL::libcurl
        nlohmann_json::nlohmann_json
        OpenSSL::Crypto
    )

    # Phase M-4: cross-process Pull e2e. Lives in its own binary so the
    # HeadlessNode singleton can be initialised in TCP mode without
    # colliding with the loopback-mode tests in test_node_data_service.
    kvcache_add_test(test_cross_process_pull
                      grpc/cross_process_pull_test.cpp
                      ${NODE_TRANSPORT_SRCS})
    target_include_directories(test_cross_process_pull PRIVATE
        ${CMAKE_SOURCE_DIR}/core-abi/src
    )
    target_link_libraries(test_cross_process_pull PRIVATE
        kvstore_node_grpc
        kvcache
        OpenSSL::Crypto
    )
    # A1 — this target also pulls NODE_TRANSPORT_SRCS (ucx_backend.cpp).
    if(KVCACHE_ENABLE_UCX)
        target_link_libraries(test_cross_process_pull PRIVATE PkgConfig::UCX)
    endif()
endif()
kvcache_add_test(test_trace             obs/trace_test.cpp)
kvcache_add_test(test_trace_otlp        obs/trace_otlp_test.cpp)
target_link_libraries(test_trace_otlp PRIVATE kvcache_otlp CURL::libcurl nlohmann_json::nlohmann_json)

# Task 1 — A9 DR warm-standby: ReplicaFetch on HeadlessNode.
#
# The test calls HeadlessNode methods directly (not via the C ABI), so we
# compile headless_node.cpp and ctx_options.cpp into the binary rather than
# linking the kvcache dylib (which hides non-ABI C++ symbols). We link
# kvstore_node_core for all node subsystem deps and kvcache_common for
# metrics/tracing/hashing utilities.
kvcache_add_test(test_replica_fetch
                  replication/replica_fetch_test.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/headless_node.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/ctx_options.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/kv_status.cpp)
target_include_directories(test_replica_fetch PRIVATE
                  ${CMAKE_SOURCE_DIR}/core-abi/src
                  ${KVCACHE_NODE_SRC})
target_link_libraries(test_replica_fetch PRIVATE
                  kvstore_node_core
                  kvcache_common
                  CURL::libcurl)
if(OpenSSL_FOUND)
    target_link_libraries(test_replica_fetch PRIVATE OpenSSL::Crypto)
endif()

# Task 2 — A9 DR warm-standby: SealByChunkPath on HeadlessNode.
#
# Mirrors test_replica_fetch: compiles headless_node.cpp directly so the
# non-ABI C++ symbols (SealByChunkPath, SealCommit) are reachable without
# going through the kvcache dylib's public C surface.
kvcache_add_test(test_seal_by_chunk_path
                  replication/seal_by_chunk_path_test.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/headless_node.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/ctx_options.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/kv_status.cpp)
target_include_directories(test_seal_by_chunk_path PRIVATE
                  ${CMAKE_SOURCE_DIR}/core-abi/src
                  ${KVCACHE_NODE_SRC})
target_link_libraries(test_seal_by_chunk_path PRIVATE
                  kvstore_node_core
                  kvcache_common
                  CURL::libcurl)
if(OpenSSL_FOUND)
    target_link_libraries(test_seal_by_chunk_path PRIVATE OpenSSL::Crypto)
endif()

# Task 3 — A9 DR warm-standby: WarmSetFilter (header-only predicate).
#
# The filter lives in replication/warm_filter.h and has no .cpp of its own.
# The test only needs NODE_PREFIX_SRCS so the kv_event_stream.h include chain
# resolves (EventType / Tier enums live there).
kvcache_add_test(test_warm_filter
                  replication/warm_filter_test.cpp
                  ${NODE_PREFIX_SRCS})

# Task 4 — A9 DR warm-standby: ReplicationCursor (header-only monotonic epoch cursor).
#
# The cursor lives in replication/replication_cursor.h and has no .cpp of its own.
# The test needs NODE_PREFIX_SRCS for the kv_event_stream.h include chain
# (though the cursor itself is a pure-header dependency tracker, NODE_PREFIX_SRCS
# is included for consistency with other replication tests).
kvcache_add_test(test_replication_cursor
                  replication/replication_cursor_test.cpp
                  ${NODE_PREFIX_SRCS})

# Task 5 / Task 6 — A9 DR warm-standby: ReplicationConsumer.
#
# The consumer now lives in kvcache (core-abi), so we link the dylib rather
# than compiling replication_consumer.cpp directly.  headless_node.cpp is also
# inside kvcache, so the non-ABI C++ symbols we need (ReplicaFetch,
# SealByChunkPath, Reserve, Publish, SubscribeEvents, Start, Stop) are all
# exported via the dylib's hidden-visibility build — but wait: visibility is
# hidden. We therefore still compile headless_node.cpp directly so tests can
# call non-ABI C++ methods; replication_consumer.cpp is NOT compiled separately
# because it's already part of kvcache.  We link kvcache to pull in its
# dependent link graph (kvstore_node_core etc.) and also compile
# headless_node.cpp directly so C++ methods are symbol-visible in the test.
#
# Include path: core-abi/src so the test can #include "replication/replication_consumer.h"
#               and "headless_node.h" / "ctx_options.h" directly.
kvcache_add_test(test_replication_consumer
                  replication/replication_consumer_test.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/headless_node.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/ctx_options.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/kv_status.cpp
                  ${CMAKE_SOURCE_DIR}/core-abi/src/replication/replication_consumer.cpp)
target_include_directories(test_replication_consumer PRIVATE
                  ${CMAKE_SOURCE_DIR}/core-abi/src
                  ${KVCACHE_NODE_SRC})
target_link_libraries(test_replication_consumer PRIVATE
                  kvstore_node_core
                  kvcache_common
                  CURL::libcurl)
if(OpenSSL_FOUND)
    target_link_libraries(test_replication_consumer PRIVATE OpenSSL::Crypto)
endif()

# Phase A1 — kvagent module tests. Each test links the agent's
# static lib; the include surface is the kvagent_lib PUBLIC dirs.
if(KVCACHE_BUILD_AGENT)
    kvcache_add_test(test_sq_cq          kvagent/sq_cq_test.cpp)
    target_link_libraries(test_sq_cq PRIVATE kvagent_lib)
    kvcache_add_test(test_doorbell       kvagent/doorbell_test.cpp)
    target_link_libraries(test_doorbell PRIVATE kvagent_lib)
    kvcache_add_test(test_routing_cache  kvagent/routing_cache_test.cpp)
    target_link_libraries(test_routing_cache PRIVATE kvagent_lib)
    kvcache_add_test(test_bloom_view     kvagent/bloom_view_test.cpp)
    target_link_libraries(test_bloom_view PRIVATE kvagent_lib)
    kvcache_add_test(test_router         kvagent/router_test.cpp)
    target_link_libraries(test_router PRIVATE kvagent_lib)
    kvcache_add_test(test_hrw_resolver   kvagent/hrw_resolver_test.cpp)
    target_link_libraries(test_hrw_resolver PRIVATE kvagent_lib)
    kvcache_add_test(test_cluster_view   kvagent/cluster_view_test.cpp)
    target_link_libraries(test_cluster_view PRIVATE kvagent_lib)
    kvcache_add_test(test_etcd_view_loader kvagent/etcd_view_loader_test.cpp)
    target_link_libraries(test_etcd_view_loader PRIVATE kvagent_lib)
endif()
