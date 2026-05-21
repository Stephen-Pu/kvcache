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

include_directories(${CMAKE_SOURCE_DIR}/include)

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
