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

kvcache_add_test(test_etcd_client       cluster/etcd_client_test.cpp         ${NODE_CLUSTER_SRCS})
target_link_libraries(test_etcd_client PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_membership_fsm    cluster/membership_fsm_test.cpp      ${NODE_CLUSTER_SRCS})
target_link_libraries(test_membership_fsm PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_http_etcd_client  cluster/http_etcd_client_test.cpp    ${NODE_CLUSTER_SRCS})
target_link_libraries(test_http_etcd_client PRIVATE CURL::libcurl nlohmann_json::nlohmann_json)
kvcache_add_test(test_hrw               routing/hrw_test.cpp                 ${NODE_ROUTING_SRCS})
kvcache_add_test(test_bloom_sketch      routing/bloom_sketch_test.cpp        ${NODE_ROUTING_SRCS})

kvcache_add_test(test_quota             qos/quota_test.cpp                   ${NODE_QOS_SRCS})
kvcache_add_test(test_tenant            qos/tenant_test.cpp                  ${NODE_QOS_SRCS})

kvcache_add_test(test_mtls              security/mtls_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_rbac              security/rbac_test.cpp               ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})
kvcache_add_test(test_audit             security/audit_test.cpp              ${NODE_SECURITY_SRCS} ${NODE_QOS_SRCS})

kvcache_add_test(test_metrics           obs/metrics_test.cpp)
kvcache_add_test(test_trace             obs/trace_test.cpp)
kvcache_add_test(test_trace_otlp        obs/trace_otlp_test.cpp)
target_link_libraries(test_trace_otlp PRIVATE kvcache_otlp CURL::libcurl nlohmann_json::nlohmann_json)
