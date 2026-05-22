# core/proto — generates C++ stubs from .proto files.
#
# Generated headers land under build/proto/ and are exposed via the
# kvcache_proto target. Go stubs are generated separately by the Go build
# (see control-plane/Makefile, to be added).

# Protobuf + gRPC were already resolved at the top-level
# src/CMakeLists.txt (find_package(Protobuf) + find_package(gRPC CONFIG)).
# Re-finding them here would cause a duplicate-target error since gRPC's
# transitive Protobuf import collides with the local one.
if(NOT gRPC_FOUND)
  message(STATUS "gRPC not found via find_package; C++ proto build will be "
                 "stubbed until dependencies are vendored.")
endif()

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/events.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/node.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/cp.proto
)

set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

# Pre-compute generated source file names so add_custom_command knows the deps.
# When gRPC is available we ALSO emit `_name.grpc.pb.{cc,h}` for every .proto
# that declares a `service` — protoc emits empty stub files for service-less
# inputs which we filter out below.
set(PROTO_SRCS "")
set(PROTO_HDRS "")
foreach(_p ${PROTO_FILES})
    get_filename_component(_name ${_p} NAME_WE)
    list(APPEND PROTO_SRCS ${PROTO_OUT_DIR}/${_name}.pb.cc)
    list(APPEND PROTO_HDRS ${PROTO_OUT_DIR}/${_name}.pb.h)
endforeach()

# Service-bearing protos that get gRPC stubs generated.
set(PROTO_GRPC_FILES "")
if(gRPC_FOUND)
    foreach(_p ${PROTO_FILES})
        get_filename_component(_name ${_p} NAME_WE)
        # `events.proto` carries no service; skip it (protoc would emit an
        # empty .grpc.pb.cc which is fine to compile but creates a stale
        # target file every reconfigure).
        if(NOT _name STREQUAL "events")
            list(APPEND PROTO_GRPC_FILES ${_p})
            list(APPEND PROTO_SRCS ${PROTO_OUT_DIR}/${_name}.grpc.pb.cc)
            list(APPEND PROTO_HDRS ${PROTO_OUT_DIR}/${_name}.grpc.pb.h)
        endif()
    endforeach()
endif()

# Resolve `protoc` from whatever shape the package import gave us —
# either the imported target (`protobuf::protoc`) or the legacy
# `Protobuf_PROTOC_EXECUTABLE` variable.
if(TARGET protobuf::protoc)
    set(_protoc "$<TARGET_FILE:protobuf::protoc>")
elseif(Protobuf_PROTOC_EXECUTABLE)
    set(_protoc "${Protobuf_PROTOC_EXECUTABLE}")
else()
    message(FATAL_ERROR "protoc not found via Protobuf package")
endif()

# Run protoc once for the message stubs (every file) ...
add_custom_command(
    OUTPUT  ${PROTO_OUT_DIR}/events.pb.cc ${PROTO_OUT_DIR}/events.pb.h
            ${PROTO_OUT_DIR}/node.pb.cc   ${PROTO_OUT_DIR}/node.pb.h
            ${PROTO_OUT_DIR}/cp.pb.cc     ${PROTO_OUT_DIR}/cp.pb.h
    COMMAND ${_protoc}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${PROTO_OUT_DIR}
            ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating C++ protobuf message sources"
)

# ... and a separate run for the grpc service stubs (only service-bearing).
if(gRPC_FOUND AND PROTO_GRPC_FILES)
    set(_grpc_outputs "")
    foreach(_p ${PROTO_GRPC_FILES})
        get_filename_component(_name ${_p} NAME_WE)
        list(APPEND _grpc_outputs
             ${PROTO_OUT_DIR}/${_name}.grpc.pb.cc
             ${PROTO_OUT_DIR}/${_name}.grpc.pb.h)
    endforeach()
    add_custom_command(
        OUTPUT  ${_grpc_outputs}
        COMMAND ${_protoc}
                --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
                --grpc_out=${PROTO_OUT_DIR}
                --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
                ${PROTO_GRPC_FILES}
        DEPENDS ${PROTO_GRPC_FILES} gRPC::grpc_cpp_plugin
        COMMENT "Generating C++ gRPC service stubs"
    )
endif()

add_library(kvcache_proto STATIC ${PROTO_SRCS})
target_include_directories(kvcache_proto PUBLIC ${PROTO_OUT_DIR})
target_link_libraries(kvcache_proto PUBLIC protobuf::libprotobuf)
if(gRPC_FOUND)
    # When gRPC is on the system the kvcache_proto sources include the
    # `*.grpc.pb.cc` stubs which reference grpc++ symbols. Link the
    # imported targets so downstream `target_link_libraries(... kvcache_proto)`
    # callers don't have to re-thread the grpc deps themselves.
    target_link_libraries(kvcache_proto PUBLIC gRPC::grpc++)
endif()
