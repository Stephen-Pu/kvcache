# core-abi — libkvcache.so: stable C ABI consumed by all engine adapters.
# LLD §6.1.2.
#
# In Step-6 the implementation is the in-process HeadlessNode backend, which
# pulls in kvstore_node_core for the full subsystem stack. The Step-7+ shmem
# variant will be a sibling translation unit selected via a build flag.

add_library(kvcache SHARED
    src/kv_abi.cpp
    src/kv_status.cpp
    src/headless_node.cpp
    src/ctx_options.cpp
)

target_include_directories(kvcache
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/kvstore-node/src
)

target_link_libraries(kvcache
    PRIVATE
        kvstore_node_core
        kvcache_common
)

# Hide non-public C++ symbols; the C ABI functions are explicitly tagged
# `__attribute__((visibility("default")))` in kv_abi.cpp.
set_target_properties(kvcache PROPERTIES
    C_VISIBILITY_PRESET    hidden
    CXX_VISIBILITY_PRESET  hidden
    VISIBILITY_INLINES_HIDDEN ON
)
