# Top-level CMake for KV Cache C/C++ components.
#
# Builds:
#   - kvcache_common     (static lib, shared types: Locator, hashing, logging)
#   - kvcache_abi        (libkvcache.so, stable C ABI; LLD §6.1.2)
#   - kvstore-node       (data-plane node binary)
#   - kvagent            (sidecar binary)
#
# External dependencies (resolved via find_package or FetchContent later):
#   - RocksDB, NIXL, UCX, liburing, SPDK (optional), gRPC, Protobuf,
#     OpenSSL, abseil, spdlog, prometheus-cpp, gtest, benchmark.

cmake_minimum_required(VERSION 3.22)

# CMP0135: use extraction-time timestamps for FetchContent URL downloads,
# so re-extracting bumps mtimes and dependent rules re-run as expected.
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

project(kvcache LANGUAGES C CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_compile_options(-Wall -Wextra -Wpedantic -Werror=return-type)

# macOS-only: Homebrew often leaves /opt/homebrew/opt/llvm@*/lib on the
# default library search path, which makes ld print "reexported library
# ...libunwind.1.dylib couldn't be matched" warnings at every link step.
# The warnings are cosmetic — links succeed, runtime is unaffected — so
# we suppress link-time warnings on macOS. On Linux they stay on so real
# issues remain visible.
if(APPLE)
    add_link_options("LINKER:-w")
endif()

option(KVCACHE_BUILD_TESTS    "Build unit tests"                 ON)
option(KVCACHE_BUILD_NODE     "Build kvstore-node"               ON)
option(KVCACHE_BUILD_AGENT    "Build kvagent sidecar"            ON)
option(KVCACHE_BUILD_ABI      "Build libkvcache.so (Core ABI)"   ON)
option(KVCACHE_ENABLE_SPDK    "Enable SPDK NVMe backend"         OFF)
option(KVCACHE_ENABLE_CUDA    "Enable CUDA / GDS code paths"     OFF)
option(KVCACHE_ENABLE_ROCKSDB "Link against RocksDB"             OFF)

if(KVCACHE_ENABLE_ROCKSDB)
    find_package(RocksDB REQUIRED CONFIG)
    add_compile_definitions(KVCACHE_HAVE_ROCKSDB=1)
endif()

# --------------------------------------------------------------------------
# BLAKE3 — real cryptographic hash (replaces the FNV-1a + SplitMix64 facade).
# We prefer a system install (`find_package(BLAKE3 CONFIG)`) and fall back to
# FetchContent pulling the official BLAKE3-team reference implementation.
# Same pattern as gtest below; gives reproducible CI without requiring a
# system package on a fresh Ubuntu image.
# --------------------------------------------------------------------------
find_package(BLAKE3 QUIET CONFIG)
if(NOT BLAKE3_FOUND)
    message(STATUS "BLAKE3 not found locally; fetching v1.5.4 via FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        blake3
        GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
        GIT_TAG        1.5.4
        GIT_SHALLOW    TRUE
        SOURCE_SUBDIR  c
    )
    set(BLAKE3_TESTING OFF CACHE BOOL "" FORCE)
    # BLAKE3 1.5.4's c/CMakeLists.txt uses `cmake_minimum_required(VERSION 3.9)`.
    # CMake 4.x prints a deprecation warning about VERSION < 3.10 — that's
    # upstream's problem to fix, not ours. CMAKE_POLICY_VERSION_MINIMUM does
    # NOT silence it (that variable only raises the floor for OLDER versions).
    # The right knob is CMAKE_WARN_DEPRECATED: turn it off across the
    # FetchContent_MakeAvailable call, restore immediately after.
    set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(blake3)
    set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)
    if(NOT TARGET BLAKE3::blake3)
        add_library(BLAKE3::blake3 ALIAS blake3)
    endif()
    # BLAKE3 1.5.4's blake3_dispatch.c declares get_cpu_features() inside an
    # x86-only path but the call sites are gated by IS_X86; on ARM Mac the
    # function is unused. Upstream bug; suppress locally.
    if(TARGET blake3)
        target_compile_options(blake3 PRIVATE -Wno-unused-function)
    endif()
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)

# --------------------------------------------------------------------------
# HttpEtcdClient deps — libcurl (system) + nlohmann/json (FetchContent).
# Both are consumed only by src/kvstore-node/src/cluster/http_etcd_client.cpp
# and a single test binary. Pulled in here so any TU under kvstore-node can
# pick them up via the standard include path.
# --------------------------------------------------------------------------
find_package(CURL REQUIRED)

find_package(nlohmann_json 3 QUIET CONFIG)
if(NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann_json not found locally; fetching v3.11.3 via FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG        v3.11.3
        GIT_SHALLOW    TRUE
    )
    set(JSON_BuildTests OFF CACHE INTERNAL "")
    set(JSON_Install    OFF CACHE INTERNAL "")
    set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(nlohmann_json)
    set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)
endif()

add_subdirectory(core/common)

# core/proto is optional: only build when Protobuf is available. This keeps the
# tree buildable on developer machines that don't have protoc installed yet.
find_package(Protobuf QUIET)
if(Protobuf_FOUND)
    add_subdirectory(core/proto)
else()
    message(STATUS "Protobuf not found; skipping core/proto. "
                   "Install protobuf-compiler to enable gRPC codegen.")
endif()

if(KVCACHE_BUILD_ABI)
  add_subdirectory(core-abi)
endif()

if(KVCACHE_BUILD_NODE)
  add_subdirectory(kvstore-node)
endif()

if(KVCACHE_BUILD_AGENT)
  add_subdirectory(kvagent)
endif()

if(KVCACHE_BUILD_TESTS)
  enable_testing()

  # Make GoogleTest available even when the system package isn't installed.
  # We prefer find_package (faster, no network) and fall back to FetchContent
  # so CI on a vanilla Ubuntu image can run unit tests out of the box.
  find_package(GTest QUIET CONFIG)
  if(NOT GTest_FOUND)
    find_package(GTest QUIET)
  endif()
  if(NOT GTest_FOUND)
    message(STATUS "GTest not found locally; fetching v1.14.0 via FetchContent")
    include(FetchContent)
    FetchContent_Declare(
      googletest
      URL      https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
      URL_HASH SHA256=8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    if(NOT TARGET GTest::gtest)
      add_library(GTest::gtest      ALIAS gtest)
      add_library(GTest::gtest_main ALIAS gtest_main)
    endif()
    set(GTest_FOUND TRUE)
  endif()

  add_subdirectory(tests/unit)
endif()
