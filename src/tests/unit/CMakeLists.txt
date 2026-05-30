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
    ${KVCACHE_NODE_SRC}/transport/priority_scheduler.cpp
)

set(NODE_CLUSTER_SRCS
    ${KVCACHE_NODE_SRC}/cluster/etcd_client.cpp
    ${KVCACHE_NODE_SRC}/cluster/http_etcd_client.cpp
    ${KVCACHE_NODE_SRC}/cluster/membership_fsm.cpp
    ${KVCACHE_NODE_SRC}/cluster/node_registrar.cpp
    ${KVCACHE_NODE_SRC}/cluster/node_directory.cpp
    ${KVCACHE_NODE_SRC}/cluster/bloom_publisher.cpp
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

kvcache_add_test(test_nixl_wrapper      transport/nixl_wrapper_test.cpp      ${NODE_TRANSPORT_SRCS})
kvcache_add_test(test_priority_scheduler transport/priority_scheduler_test.cpp ${NODE_TRANSPORT_SRCS})
kvcache_add_test(test_tcp_backend       transport/tcp_backend_test.cpp       ${NODE_TRANSPORT_SRCS})

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

kvcache_add_test(test_quota             qos/quota_test.cpp                   ${NODE_QOS_SRCS})
kvcache_add_test(test_tenant            qos/tenant_test.cpp                  ${NODE_QOS_SRCS})

kvcache_add_test(test_mtls              security/mtls_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_rbac              security/rbac_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_audit             security/audit_test.cpp              ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})

kvcache_add_test(test_metrics           obs/metrics_test.cpp)
# LLD §6.2 — logging facade. Foundation only; per-callsite migration
# of fprintf/cerr → KV_LOG_* is per-file follow-on work.
kvcache_add_test(test_logs              common/log_sink_test.cpp)
kvcache_add_test(test_node_runtime      runtime/node_runtime_test.cpp
                 ${KVCACHE_NODE_SRC}/runtime/node_runtime.cpp)

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
endif()
kvcache_add_test(test_trace             obs/trace_test.cpp)
kvcache_add_test(test_trace_otlp        obs/trace_otlp_test.cpp)
target_link_libraries(test_trace_otlp PRIVATE kvcache_otlp CURL::libcurl nlohmann_json::nlohmann_json)
