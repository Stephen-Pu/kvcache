# kvagent — sidecar daemon (Phase A1).
#
# Library + binary split so unit tests can link the modules in
# isolation without the main.cpp entrypoint dragging in.

set(KVAGENT_LIB_SRCS
    src/shmem_ring/sq_cq.cpp
    src/shmem_ring/doorbell.cpp
    src/routing_cache/routing_cache.cpp
    src/bloom_view/bloom_view.cpp
    src/router/router.cpp
    src/router/hrw_resolver.cpp
    src/router/cluster_view.cpp
    src/router/etcd_view_loader.cpp
    # Phase A1.9 — InMemoryEtcdClient (the grpc client in the same TU is
    # #if'd out: kvagent doesn't define KVCACHE_HAVE_GRPC, so no grpc
    # link is dragged in). Lets the ClusterViewWatcher read the view
    # straight from etcd via MakeEtcdViewLoader.
    ${CMAKE_SOURCE_DIR}/kvstore-node/src/cluster/etcd_client.cpp
    # Phase A1.10 — HttpEtcdClient: a real cross-process etcd v3 client
    # over the JSON/HTTP gateway (libcurl + nlohmann, no grpc). main.cpp
    # dials this when KVCACHE_ETCD_ENDPOINTS is set so the view watcher
    # reads the live /kvcache/cluster/view across processes.
    ${CMAKE_SOURCE_DIR}/kvstore-node/src/cluster/http_etcd_client.cpp
    # BloomView wraps kvcache::node::routing::AggregatedBloom and the
    # HrwResolver (Phase A1.7) wraps kvcache::node::routing::HrwRing —
    # pull in those TUs so the agent lib is self-contained at link time.
    # Once kvstore-node grows a 'kvcache_routing' static lib we can drop
    # these direct path inclusions.
    ${CMAKE_SOURCE_DIR}/kvstore-node/src/routing/bloom_sketch.cpp
    ${CMAKE_SOURCE_DIR}/kvstore-node/src/routing/hrw.cpp
)

add_library(kvagent_lib STATIC ${KVAGENT_LIB_SRCS})

target_include_directories(kvagent_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/kvstore-node/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvagent_lib
    PUBLIC
        kvcache_common
        # Phase A1.8 — ClusterViewWatcher parses the CP's JSON view.
        nlohmann_json::nlohmann_json
        # Phase A1.10 — HttpEtcdClient talks the etcd JSON gateway.
        CURL::libcurl
)

add_executable(kvagent src/main.cpp)
target_link_libraries(kvagent PRIVATE kvagent_lib)
