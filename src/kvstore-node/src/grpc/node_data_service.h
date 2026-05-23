// Phase M-1 — gRPC handlers for the kvstore-node data plane.
// Phase M-3 — promoted to a per-(tenant_hash, model_hash) kv_ctx_t cache
//             so each agent connection lands on a ctx with the right
//             scheduler bucket + ART partition identity.
//
// Implements the proto-defined NodeData service (LLD §3 / §4) by
// forwarding each RPC to the public C ABI. The cache is keyed by
// (tenant_id_hash, model_id_hash): handlers compute the key from the
// request's identity fields and lazily open a ctx via
// kv_ctx_open_from_hashes the first time a (tenant, model) pair is
// seen. A reverse handle→ctx map lets handle-based RPCs
// (Publish / Fetch / Seal / Release) find the ctx that created the
// handle, since those request types don't carry tenant/model on the
// wire.
//
// What's wired up:
//
//   Lookup    → kv_lookup       (request carries tenant_id + model_id_hash)
//   Reserve   → kv_reserve      (request carries Locator with both hashes;
//                                response now also carries the NIXL
//                                RemoteMrDescriptor for the slot)
//   Publish   → kv_publish      (ctx looked up via server_handle)
//   Fetch     → kv_fetch+kv_wait (ctx via handle; dst_remote_mr_descriptor
//                                  imported through the NIXL backend
//                                  when present)
//   Seal      → kv_seal
//   Release   → kv_release
//   Subscribe → EventStream stream  (uses the bootstrap default ctx since
//                                     SubscribeRequest carries only
//                                     tenant_id — no model)
//
// The service holds a bootstrap "default" ctx supplied at construction
// time. Test fixtures that want to exercise the cache directly call
// CachedCtxCount() to assert lazy-open behaviour.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "kvcache/kv_abi.h"
#include "node.grpc.pb.h"

namespace kvcache::node::grpc_server {

class NodeDataServiceImpl final : public kvcache::proto::NodeData::Service {
   public:
    // Bootstrap ctx — used by Subscribe (which only carries tenant_id on
    // the wire) and as the fallback when a handle doesn't appear in the
    // reverse map (typically tests that mint synthetic handles). The
    // service does not own this pointer.
    explicit NodeDataServiceImpl(kv_ctx_t* default_ctx)
        : default_ctx_(default_ctx) {}

    ~NodeDataServiceImpl() override;

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

    // Number of distinct (tenant_hash, model_hash) ctxs the service has
    // lazily opened. Exposed for tests; cheap O(1) lookup under a lock.
    std::size_t CachedCtxCount() const;

   private:
    // 128-bit cache key reduced to a single u64 by mixing the two hashes
    // — collisions are astronomically unlikely (two FNV-1a 64s xored).
    static uint64_t MakeKey(uint64_t tenant_hash, uint64_t model_hash) {
        // Knuth's multiplicative spreader on the model half so coincidental
        // tenant prefixes don't collide; xor the tenant hash on top.
        return tenant_hash ^ (model_hash * 0x9E3779B97F4A7C15ULL);
    }

    // Returns an open ctx for (tenant_hash, model_hash); opens lazily on
    // first miss. Returns nullptr on open failure.
    kv_ctx_t* GetOrOpenCtx(uint64_t tenant_hash, uint64_t model_hash);

    // Record that `server_handle` was minted on `ctx` so subsequent
    // handle-based RPCs can find it.
    void RememberHandle(uint64_t server_handle, kv_ctx_t* ctx);
    // Drop a remembered (handle → ctx) mapping. Called from Release.
    void ForgetHandle(uint64_t server_handle);
    // Resolve a handle back to its ctx; falls back to default_ctx_ on miss.
    kv_ctx_t* CtxForHandle(uint64_t server_handle);

    kv_ctx_t* default_ctx_;  // not owned

    mutable std::mutex                       mu_;
    std::unordered_map<uint64_t, kv_ctx_t*>  cache_;        // owned ctxs
    std::unordered_map<uint64_t, kv_ctx_t*>  handle_to_ctx_;
};

}  // namespace kvcache::node::grpc_server
