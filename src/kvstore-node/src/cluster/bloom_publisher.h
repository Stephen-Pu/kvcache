// Phase K-5 — periodic bloom-sketch publisher.
//
// Each kvstore-node owns one BloomPublisher. The publisher:
//
//   * Holds a LocalBloom tracking the chunk-keys this node has cached.
//     Callers feed it via Add() (in production, hooked off the
//     EventStream's KV_EVENT_ADD; the publisher itself doesn't depend
//     on the ART layer, which keeps the test surface narrow).
//
//   * Every `publish_period` seconds, snapshots the bloom and PUTs it
//     to `/kvcache/sketches/<node_id>` bound to the lease the
//     NodeRegistrar already grants. When the node dies, etcd drops
//     the sketch key in tandem with /kvcache/nodes/<node_id>.
//
//   * Encoding (so consumers know how to deserialise without a
//     separate schema lookup):
//
//       [m_bits:u32 LE][k_hashes:u32 LE][bit_array...]
//
// Consumers (peer NodeDirectories, future kvagent router) decode this
// blob, feed (params, bytes) into AggregatedBloom::Set, and consult
// MaybeContains during routing.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "cluster/etcd_client.h"
#include "routing/bloom_sketch.h"

namespace kvcache::node::cluster {

class BloomPublisher {
   public:
    struct Options {
        std::string node_id;
        // Lease the published key is bound to. Typically reuse the
        // NodeRegistrar's lease so the sketch dies with the node.
        // 0 = no lease (test convenience; the key persists).
        LeaseId   lease = kNoLease;
        // Capacity / FPR knobs for the LocalBloom. Defaults size for
        // ~1M chunks at 1% FPR — sketches end up ~1.2 MiB on the
        // wire, which is fine for a 30s publish cadence.
        uint64_t expected_chunks = 1ull << 20;
        double   target_fpr      = 0.01;
        // How often the publisher snapshots + PUTs the sketch.
        std::chrono::milliseconds publish_period{30000};
        // Etcd key prefix. Full key = `<prefix><node_id>`.
        std::string key_prefix = "/kvcache/sketches/";
    };

    BloomPublisher(IEtcdClient* etcd, const Options& opts);
    ~BloomPublisher();

    BloomPublisher(const BloomPublisher&)            = delete;
    BloomPublisher& operator=(const BloomPublisher&) = delete;

    // Insert a chunk-key (typically the 40-byte SealedChunk identity).
    // Thread-safe; piggy-backs on LocalBloom's internal mutex.
    void Add(std::span<const uint8_t> key);

    // Phase K-6 — convenience for the Seal hook: build the canonical
    // sketch key from (tenant_hash, model_hash, tokens) and Add it.
    // Mirrors the consumer-side derivation that NodeDataService uses
    // on a Lookup local-miss when consulting PeerMaybeHas, so a
    // chunk added here is discoverable via that path.
    void AddTokens(uint64_t        tenant_hash,
                    uint64_t        model_hash,
                    const uint32_t* tokens,
                    std::size_t     n);

    // First publish runs synchronously inside Start() so peers see a
    // sketch from the very first revision. Subsequent publishes fire
    // on the background loop.
    bool Start(std::string* err);
    void Stop();

    // Force-publish now (in addition to the timer). Useful for tests
    // and for the operator drain path. Returns true on success.
    bool PublishNow(std::string* err);

    // Key / count exposed for tests.
    const std::string& Key() const noexcept { return key_; }
    uint64_t PublishCount() const noexcept {
        return publish_count_.load(std::memory_order_relaxed);
    }

    // Encode the current snapshot to the wire format described in
    // the file header. Public for the symmetric decoder helper to
    // assert against in tests.
    std::vector<uint8_t> EncodeSnapshot() const;

   private:
    void PublishLoop();

    IEtcdClient*                  etcd_;
    Options                       opts_;
    std::string                   key_;
    routing::LocalBloom           bloom_;
    std::atomic<bool>             running_{false};
    std::atomic<bool>             stop_{false};
    std::atomic<uint64_t>         publish_count_{0};

    std::mutex                    cv_mu_;
    std::condition_variable       cv_;
    std::thread                   thread_;
};

// Decode a snapshot blob into (params, raw_bytes) suitable for
// AggregatedBloom::Set. Returns false on malformed input.
bool DecodeBloomSnapshot(const std::string&    encoded,
                          routing::BloomParams* out_params,
                          std::vector<uint8_t>* out_bytes);

// Phase K-6 — canonical sketch key shape for "is this token-prefix
// chunk hosted by peer X?" queries. Both the publisher's AddTokens
// hook (at Seal time) and the router's PeerMaybeHas hint (at Lookup
// local-miss time) feed identical bytes into the Bloom math:
//
//   [tenant_hash:u64 LE][model_hash:u64 LE][token_0:u32 LE]...[token_{n-1}:u32 LE]
//
// Token-set match is exact — partial-prefix matches need a separate
// chunked variant (Phase K-8) that pre-hashes every 16-token boundary.
std::vector<uint8_t> SketchKeyForTokens(uint64_t        tenant_hash,
                                         uint64_t        model_hash,
                                         const uint32_t* tokens,
                                         std::size_t     n);

}  // namespace kvcache::node::cluster
