// Phase M-1 — gRPC handlers for the kvstore-node data plane.
//
// Implements the proto-defined NodeData service (LLD §3 / §4) by
// forwarding each RPC to the public C ABI. Holds a single kv_ctx_t* for
// the "default" tenant the binary was launched with; Phase M-2 will
// promote this to a (tenant, model) → ctx cache so each agent
// connection talks to the right context.
//
// What's wired up:
//
//   Lookup   → kv_lookup       (metadata only, hot path)
//   Reserve  → kv_reserve      (returns slot iova + bytes + mr_key —
//                                only meaningful in-process today)
//   Publish  → kv_publish      (advance watermark)
//   Fetch    → kv_fetch + kv_wait (synchronous, in-process)
//   Seal     → kv_seal         (commit; tokens carried over the wire)
//   Release  → kv_release
//   Subscribe                  — returns UNIMPLEMENTED (Phase M-2).
//
// The slot_iova / dst_iova fields in Reserve/Fetch carry server-side
// host pointers. Today's KVAgent is colocated in-process with the
// kvstore-node binary, so the agent can dereference these directly.
// Across processes (or hosts) those fields will be replaced by the
// NIXL `RemoteMrDescriptor` exchange the TcpBackend already handles
// (LLD §3.5 D-NET-1) — wiring that into Reserve/Fetch is Phase M-2.
#pragma once

#include <memory>

#include "kvcache/kv_abi.h"
#include "node.grpc.pb.h"

namespace kvcache::node::grpc_server {

class NodeDataServiceImpl final : public kvcache::proto::NodeData::Service {
   public:
    // Service is constructed with a long-lived agent context. The
    // caller (main.cpp or unit tests) opens a kv_ctx_t via the public
    // C ABI and hands the pointer over; the service does not own it.
    explicit NodeDataServiceImpl(kv_ctx_t* ctx) : ctx_(ctx) {}

    ::grpc::Status Lookup(::grpc::ServerContext*               context,
                            const kvcache::proto::LookupRequest* request,
                            kvcache::proto::LookupResponse*      response) override;

    ::grpc::Status Reserve(::grpc::ServerContext*                context,
                             const kvcache::proto::ReserveRequest* request,
                             kvcache::proto::ReserveResponse*      response) override;

    ::grpc::Status Publish(::grpc::ServerContext*                context,
                             const kvcache::proto::PublishRequest* request,
                             kvcache::proto::PublishResponse*      response) override;

    ::grpc::Status Fetch(::grpc::ServerContext*               context,
                            const kvcache::proto::FetchRequest* request,
                            kvcache::proto::FetchResponse*      response) override;

    ::grpc::Status Seal(::grpc::ServerContext*               context,
                          const kvcache::proto::SealRequest* request,
                          kvcache::proto::SealResponse*      response) override;

    ::grpc::Status Release(::grpc::ServerContext*                context,
                             const kvcache::proto::ReleaseRequest* request,
                             kvcache::proto::ReleaseResponse*      response) override;

    ::grpc::Status Subscribe(::grpc::ServerContext*                  context,
                               const kvcache::proto::SubscribeRequest* request,
                               ::grpc::ServerWriter<kvcache::proto::Event>* writer)
        override;

   private:
    kv_ctx_t* ctx_;  // not owned
};

}  // namespace kvcache::node::grpc_server
