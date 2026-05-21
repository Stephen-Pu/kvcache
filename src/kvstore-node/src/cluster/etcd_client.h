// LLD §4.1 — Etcd client facade used by the cluster coordination layer.
//
// We define an `IEtcdClient` interface and ship two implementations:
//
//   * `InMemoryEtcdClient` — semantically faithful to etcd v3 (monotonic
//     revisions, TTL-based leases, prefix watches, KeepAlive). Used by unit
//     tests and by the in-process demo path. NOT for production: it has no
//     consensus and no persistence.
//
//   * `GrpcEtcdClient` — talks to a real etcd cluster via gRPC. Compiled
//     when `KVCACHE_ENABLE_ETCD=ON`. Requires vendored etcd v3 protos
//     (rpc.proto + kv.proto + auth.proto, stripped of gogo extensions).
//     See cluster/README.md for the exact vendoring steps.
//
// All upper layers (`MembershipFsm`, `BloomSketch` sync, config push) program
// against `IEtcdClient`; switching implementations is a one-line change at
// node bring-up.
//
// Surface mirrors the etcd v3 KV/Lease/Watch APIs:
//   * KV     : Put / Get / Delete / Txn-on-CompareAndSwap (PutIfRevision)
//   * Lease  : Grant / KeepAlive / Revoke
//   * Watch  : prefix-watch with revision cursor
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace kvcache::node::cluster {

using Revision = uint64_t;
using LeaseId  = int64_t;

inline constexpr LeaseId  kNoLease  = 0;

struct KeyValue {
    std::string key;
    std::string value;
    Revision    mod_revision    = 0;
    Revision    create_revision = 0;
    LeaseId     lease           = kNoLease;
};

enum class WatchEventType { kPut, kDelete };

struct WatchEvent {
    WatchEventType type;
    KeyValue       kv;
    KeyValue       prev_kv;   // populated for Put-overwrite
};

using WatchHandle   = uint64_t;
using WatchCallback = std::function<void(const WatchEvent&)>;

class IEtcdClient {
   public:
    virtual ~IEtcdClient() = default;

    virtual std::string Backend() const = 0;

    // ---- KV ----
    virtual bool Put(const std::string& key, const std::string& value,
                     LeaseId lease, Revision* out_rev, std::string* err) = 0;

    virtual std::optional<KeyValue> Get(const std::string& key, std::string* err) = 0;

    virtual std::vector<KeyValue> GetPrefix(const std::string& prefix, std::string* err) = 0;

    virtual bool Delete(const std::string& key, std::string* err) = 0;

    // Compare-and-swap: write iff `key`'s current mod_revision == `expected_rev`.
    // expected_rev == 0 means "key must not exist".
    virtual bool PutIfRevision(const std::string& key, const std::string& value,
                                Revision expected_rev, LeaseId lease,
                                Revision* out_rev, std::string* err) = 0;

    // ---- Lease ----
    virtual LeaseId LeaseGrant(uint32_t ttl_seconds, std::string* err) = 0;
    virtual bool    LeaseKeepAlive(LeaseId lease, std::string* err) = 0;
    virtual bool    LeaseRevoke   (LeaseId lease, std::string* err) = 0;
    virtual uint32_t LeaseTTLRemaining(LeaseId lease) const = 0;

    // ---- Watch ----
    // Subscribes to all events under `prefix` starting at the current revision.
    // The callback runs on an internal dispatch thread; the caller is
    // responsible for offloading any heavy work.
    virtual WatchHandle WatchPrefix(const std::string& prefix,
                                     WatchCallback cb) = 0;
    virtual void        Unwatch(WatchHandle handle) = 0;
};

// ---------------------------------------------------------------------------
// InMemoryEtcdClient
// ---------------------------------------------------------------------------

class InMemoryEtcdClient final : public IEtcdClient {
   public:
    struct Options {
        std::chrono::milliseconds lease_sweep_interval{100};
    };
    InMemoryEtcdClient();
    explicit InMemoryEtcdClient(const Options& opts);
    ~InMemoryEtcdClient() override;

    std::string Backend() const override { return "in-memory"; }

    bool Put(const std::string&, const std::string&, LeaseId, Revision*, std::string*) override;
    std::optional<KeyValue> Get(const std::string&, std::string*) override;
    std::vector<KeyValue> GetPrefix(const std::string&, std::string*) override;
    bool Delete(const std::string&, std::string*) override;
    bool PutIfRevision(const std::string&, const std::string&,
                        Revision, LeaseId, Revision*, std::string*) override;

    LeaseId  LeaseGrant(uint32_t ttl_seconds, std::string*) override;
    bool     LeaseKeepAlive(LeaseId, std::string*) override;
    bool     LeaseRevoke   (LeaseId, std::string*) override;
    uint32_t LeaseTTLRemaining(LeaseId) const override;

    WatchHandle WatchPrefix(const std::string& prefix, WatchCallback cb) override;
    void        Unwatch(WatchHandle handle) override;

   private:
    struct Lease {
        uint32_t                              ttl_seconds;
        std::chrono::steady_clock::time_point deadline;
    };

    struct Watcher {
        std::string   prefix;
        WatchCallback cb;
    };

    // Emit `ev` to every watcher whose prefix matches `key`. Caller holds mu_.
    void NotifyLocked(const WatchEvent& ev);
    // Remove all keys associated with `lease`. Caller holds mu_.
    void ExpireLeaseLocked(LeaseId lease);
    void SweeperLoop();

    mutable std::mutex                            mu_;
    Revision                                       revision_ = 0;
    std::unordered_map<std::string, KeyValue>     kvs_;
    std::unordered_map<LeaseId, Lease>            leases_;
    std::unordered_map<WatchHandle, Watcher>      watchers_;
    std::atomic<LeaseId>     next_lease_{1};
    std::atomic<WatchHandle> next_watch_{1};

    std::atomic<bool>        stop_{false};
    std::thread              sweeper_;
    std::chrono::milliseconds sweep_interval_;
};

// ---------------------------------------------------------------------------
// GrpcEtcdClient — real etcd v3 client. Skeleton; .cpp gated on
// KVCACHE_ENABLE_ETCD (requires vendored etcd protos + grpc++).
// ---------------------------------------------------------------------------

class GrpcEtcdClient final : public IEtcdClient {
   public:
    struct Options {
        std::vector<std::string> endpoints;       // e.g. {"127.0.0.1:2379"}
        std::chrono::milliseconds dial_timeout{5000};
        // mTLS material (LLD §5.2). Empty paths = insecure channel.
        std::string ca_pem_path;
        std::string client_cert_pem_path;
        std::string client_key_pem_path;
    };

    static std::unique_ptr<GrpcEtcdClient> Create(const Options& opts, std::string* err);
    ~GrpcEtcdClient() override;

    std::string Backend() const override { return "grpc-etcd"; }

    // ---- KV / Lease / Watch overrides ----
    bool Put(const std::string&, const std::string&, LeaseId, Revision*, std::string*) override;
    std::optional<KeyValue> Get(const std::string&, std::string*) override;
    std::vector<KeyValue> GetPrefix(const std::string&, std::string*) override;
    bool Delete(const std::string&, std::string*) override;
    bool PutIfRevision(const std::string&, const std::string&,
                        Revision, LeaseId, Revision*, std::string*) override;
    LeaseId  LeaseGrant(uint32_t, std::string*) override;
    bool     LeaseKeepAlive(LeaseId, std::string*) override;
    bool     LeaseRevoke   (LeaseId, std::string*) override;
    uint32_t LeaseTTLRemaining(LeaseId) const override;
    WatchHandle WatchPrefix(const std::string&, WatchCallback) override;
    void        Unwatch(WatchHandle) override;

   private:
    GrpcEtcdClient() = default;
    // Hidden pImpl so the public header does not pull in grpc++ / proto types.
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kvcache::node::cluster
