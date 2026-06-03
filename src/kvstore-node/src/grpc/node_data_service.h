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
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kvcache/kv_abi.h"
#include "node.grpc.pb.h"

namespace kvcache::node::cluster { class NodeDirectory; }
namespace kvcache::node::cluster { class BloomPublisher; }
namespace kvcache::node::cluster { class DrainGate; }
namespace kvcache::node::security { class MtlsRegistry; }
namespace kvcache::node::routing { class HrwRing; }

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

    // Phase Q-1 — enable cross-node Lookup fan-out. The service holds
    // non-owning pointers to the HrwRing (decides primary for a key)
    // and NodeDirectory (resolves node_id -> dial target). The
    // self_node_id MUST match the registrar's node_id so handlers can
    // short-circuit the "I am the owner" case. When unset, fan-out is
    // disabled and every Lookup is served locally (single-node mode).
    void EnableForwarding(std::string             self_node_id,
                          routing::HrwRing*       ring,
                          cluster::NodeDirectory* directory);

    // Phase K-8 — install a BloomPublisher to receive AddTokens on
    // every successful Seal. Optional; when null the service skips
    // the publisher call and the node's sketch stays empty. The
    // pointer is non-owning and must outlive the service.
    void EnableSketchPublishing(cluster::BloomPublisher* publisher);

    // Phase N-2 — install mTLS material the service should use when
    // it dials peers for cross-node forwarding. When unset (default)
    // GetPeerStub falls back to InsecureChannelCredentials. The PEM
    // material is COPIED into the service so callers can free their
    // buffers. SetSslTargetNameOverride is what the cert's SAN /
    // CN must match — operators emit one cluster-wide leaf so all
    // peers present the same identity.
    void EnableMtlsClient(std::string ca_pem,
                           std::string cert_pem,
                           std::string key_pem,
                           std::string ssl_target_name_override);

    // Phase N-3 — when enabled, the Lookup / Reserve / Seal handlers
    // require the inbound peer's client-cert CN to match the
    // request's tenant string (or, for Locator-bearing requests, the
    // tenant derived from the Locator's tenant_id bytes). Mismatch
    // surfaces as UNAUTHENTICATED. Forwarded requests
    // (x-kvcache-forwarded header set) are exempt because the
    // ORIGINAL hop has already enforced the binding and node→node
    // traffic uses the cluster's shared peer cert (Phase N-2), not
    // a per-tenant leaf. Opt-in so existing TLS tests (whose certs
    // use a generic CN) keep passing. Defaults to OFF.
    void EnableTenantCertBinding(bool enable);

    // Phase B8.2 — point the tenant-cert binding at an MtlsRegistry for
    // SPIFFE-first / table-driven tenant resolution. When set, the
    // binding resolves the peer cert's authoritative tenant via
    // MtlsRegistry::ResolveTenant (table → SPIFFE-path → CN); when
    // nullptr (default) it still resolves via SPIFFE-path → CN, so a
    // SPIFFE-SAN cert works even without a published table and a plain
    // CN cert keeps the historical behaviour. The registry must outlive
    // the service.
    void SetMtlsRegistry(const security::MtlsRegistry* reg) { mtls_registry_ = reg; }

    // Phase A2.3 — point Reserve at a DrainGate. When the gate reports
    // draining, Reserve rejects NEW writes with FAILED_PRECONDITION so
    // a draining node bleeds out (reads + in-flight Seals still flow).
    // The gate must outlive the service; nullptr (default) disables the
    // check. The DrainWatcher (Phase A2.2) drives the gate from etcd.
    void SetDrainGate(const cluster::DrainGate* gate) { drain_gate_ = gate; }

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

    // Phase K-6 — RPC arrival counters for tests. Each handler bumps
    // these BEFORE any forwarding decision, so the count reflects
    // "this service was actually called", whether or not it served
    // the request locally.
    uint64_t LookupCalls() const noexcept {
        return lookup_calls_.load(std::memory_order_relaxed);
    }

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

    // Phase N-5 — record / verify handle ownership by cert CN. No-op
    // when binding is disabled. CheckHandleOwnership returns true if
    // the handle is unowned-under-binding-off OR the recorded CN
    // matches `cn`; false on mismatch.
    void RecordHandleCN(uint64_t server_handle, const std::string& cn);
    bool CheckHandleOwnership(uint64_t server_handle, const std::string& cn);
    // Drop a remembered (handle → ctx) mapping. Called from Release.
    void ForgetHandle(uint64_t server_handle);
    // Resolve a handle back to its ctx; falls back to default_ctx_ on miss.
    kv_ctx_t* CtxForHandle(uint64_t server_handle);

    kv_ctx_t* default_ctx_;  // not owned

    mutable std::mutex                       mu_;
    std::unordered_map<uint64_t, kv_ctx_t*>  cache_;        // owned ctxs
    std::unordered_map<uint64_t, kv_ctx_t*>  handle_to_ctx_;
    // Phase K-8 — handle→(tenant_hash, model_hash) shadow so Seal
    // can publish to the bloom sketch under the same hashes Reserve
    // resolved (the kv_ctx_t* doesn't expose them externally).
    struct HandleHashes { uint64_t tenant_hash; uint64_t model_hash; };
    std::unordered_map<uint64_t, HandleHashes> handle_to_hashes_;

    // Phase N-5 — handle→owner-CN binding. When tenant cert binding
    // is on, Reserve / Lookup record the minting peer's cert CN here;
    // Publish / Fetch / Seal / Release reject a handle whose recorded
    // CN doesn't match the calling peer's CN (guards against a tenant
    // guessing or replaying another tenant's server_handle). Guarded
    // by mu_ alongside the other handle maps.
    std::unordered_map<uint64_t, std::string>  handle_to_cn_;

    // Forwarding state — populated by EnableForwarding. All three are
    // either all set (fan-out enabled) or all empty/null (local-only).
    std::string                              self_node_id_;
    routing::HrwRing*                        ring_      = nullptr;
    cluster::NodeDirectory*                  directory_ = nullptr;
    // Phase K-8 — set by EnableSketchPublishing.
    cluster::BloomPublisher*                 publisher_ = nullptr;

    // Phase N-2 — mTLS material for outbound peer dials. Empty when
    // EnableMtlsClient was never called; in that case GetPeerStub
    // falls back to InsecureChannelCredentials.
    std::string                              peer_tls_ca_;
    std::string                              peer_tls_cert_;
    std::string                              peer_tls_key_;
    std::string                              peer_tls_ssl_target_override_;

    // Phase N-3 — atomic so handler reads don't need to acquire mu_.
    std::atomic<bool>                        tenant_cert_binding_enabled_{false};

    // Phase A2.3 — optional drain gate (owned elsewhere; see SetDrainGate).
    const cluster::DrainGate*                drain_gate_ = nullptr;

    // Phase B8.2 — optional mTLS registry for table-driven tenant
    // resolution (owned elsewhere; see SetMtlsRegistry).
    const security::MtlsRegistry*            mtls_registry_ = nullptr;

    // Per-peer NodeData stub cache. The Channel keeps itself reusable
    // across RPCs; the stub is light-weight on top.
    struct PeerStub;
    mutable std::mutex                                            stub_mu_;
    std::unordered_map<std::string, std::shared_ptr<PeerStub>>    stubs_;

    // Phase Q-2 — sticky-writes. When Reserve/Lookup forwards to an
    // owner and the owner returns a handle, we record (handle →
    // owner_node_id) so subsequent Publish/Fetch/Seal/Release on the
    // same handle land on the same owner. Client assumption: a logical
    // session sticks to a single forwarder between Reserve and
    // Release; if a client switches nodes mid-session the second node
    // will try to serve handle-based RPCs locally and fail.
    mutable std::mutex                                  fwd_handle_mu_;
    std::unordered_map<uint64_t, std::string>           forwarded_handles_;

    // Phase K-6 — per-RPC arrival counter for tests.
    std::atomic<uint64_t>                               lookup_calls_{0};

    void RememberForwardedHandle(uint64_t handle, std::string owner);
    void ForgetForwardedHandle(uint64_t handle);
    std::string ForwardOwnerForHandle(uint64_t handle) const;

    // Resolve a key (tenant_id + model_id_hash + tokens) -> primary
    // node id under the current HRW snapshot. Empty if ring/directory
    // are absent.
    std::string PrimaryFor(const std::string& tenant_id,
                            uint64_t           model_id_hash,
                            const std::string& tokens_bytes) const;

    // Get-or-create the cached gRPC stub for `node_id`. Returns nullptr
    // on dial failure or unknown node.
    std::shared_ptr<PeerStub> GetPeerStub(const std::string& node_id);
};

}  // namespace kvcache::node::grpc_server
