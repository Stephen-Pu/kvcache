// LLD §4.1 — Etcd client implementations.
#include "cluster/etcd_client.h"

#include <algorithm>

namespace kvcache::node::cluster {

// ===========================================================================
// InMemoryEtcdClient — semantic-faithful etcd v3 surface for tests / demos.
// ===========================================================================

InMemoryEtcdClient::InMemoryEtcdClient() : InMemoryEtcdClient(Options{}) {}

InMemoryEtcdClient::InMemoryEtcdClient(const Options& opts)
    : sweep_interval_(opts.lease_sweep_interval) {
    sweeper_ = std::thread([this] { SweeperLoop(); });
}

InMemoryEtcdClient::~InMemoryEtcdClient() {
    stop_.store(true, std::memory_order_release);
    if (sweeper_.joinable()) sweeper_.join();
}

void InMemoryEtcdClient::NotifyLocked(const WatchEvent& ev) {
    // Dispatch synchronously while holding mu_. Watchers must be cheap;
    // production use would post to a dispatcher thread (TODO(stephen)).
    for (const auto& [_, w] : watchers_) {
        if (ev.kv.key.rfind(w.prefix, 0) == 0) {
            w.cb(ev);
        }
    }
}

void InMemoryEtcdClient::ExpireLeaseLocked(LeaseId lease) {
    // Drop every KV bound to the lease and emit Delete events.
    for (auto it = kvs_.begin(); it != kvs_.end(); ) {
        if (it->second.lease == lease) {
            WatchEvent ev{WatchEventType::kDelete, it->second, it->second};
            NotifyLocked(ev);
            it = kvs_.erase(it);
        } else {
            ++it;
        }
    }
    leases_.erase(lease);
}

void InMemoryEtcdClient::SweeperLoop() {
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(sweep_interval_);
        if (stop_.load(std::memory_order_acquire)) break;
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard lk(mu_);
        std::vector<LeaseId> expired;
        for (const auto& [id, l] : leases_) {
            if (l.deadline <= now) expired.push_back(id);
        }
        for (auto id : expired) ExpireLeaseLocked(id);
    }
}

bool InMemoryEtcdClient::Put(const std::string& key, const std::string& value,
                              LeaseId lease, Revision* out_rev,
                              std::string* err) {
    std::lock_guard lk(mu_);
    if (lease != kNoLease && leases_.find(lease) == leases_.end()) {
        if (err) *err = "etcd: lease not found"; return false;
    }
    ++revision_;
    auto it = kvs_.find(key);
    KeyValue prev{};
    bool had_prev = (it != kvs_.end());
    if (had_prev) prev = it->second;
    KeyValue cur{key, value, revision_,
                  had_prev ? prev.create_revision : revision_,
                  lease};
    kvs_[key] = cur;
    WatchEvent ev{WatchEventType::kPut, cur, had_prev ? prev : KeyValue{}};
    NotifyLocked(ev);
    if (out_rev) *out_rev = revision_;
    return true;
}

std::optional<KeyValue> InMemoryEtcdClient::Get(const std::string& key, std::string*) {
    std::lock_guard lk(mu_);
    auto it = kvs_.find(key);
    if (it == kvs_.end()) return std::nullopt;
    return it->second;
}

std::vector<KeyValue> InMemoryEtcdClient::GetPrefix(const std::string& prefix,
                                                    std::string*) {
    std::lock_guard lk(mu_);
    std::vector<KeyValue> out;
    for (const auto& [k, v] : kvs_) {
        if (k.rfind(prefix, 0) == 0) out.push_back(v);
    }
    std::sort(out.begin(), out.end(),
              [](const auto& a, const auto& b) { return a.key < b.key; });
    return out;
}

bool InMemoryEtcdClient::Delete(const std::string& key, std::string*) {
    std::lock_guard lk(mu_);
    auto it = kvs_.find(key);
    if (it == kvs_.end()) return false;
    WatchEvent ev{WatchEventType::kDelete, it->second, it->second};
    NotifyLocked(ev);
    kvs_.erase(it);
    ++revision_;
    return true;
}

bool InMemoryEtcdClient::PutIfRevision(const std::string& key,
                                        const std::string& value,
                                        Revision expected_rev,
                                        LeaseId lease,
                                        Revision* out_rev,
                                        std::string* err) {
    std::lock_guard lk(mu_);
    auto it = kvs_.find(key);
    const Revision actual = (it == kvs_.end()) ? 0 : it->second.mod_revision;
    if (actual != expected_rev) {
        if (err) *err = "etcd: revision mismatch";
        return false;
    }
    if (lease != kNoLease && leases_.find(lease) == leases_.end()) {
        if (err) *err = "etcd: lease not found"; return false;
    }
    ++revision_;
    KeyValue prev = (it != kvs_.end()) ? it->second : KeyValue{};
    KeyValue cur{key, value, revision_,
                  (it != kvs_.end()) ? it->second.create_revision : revision_,
                  lease};
    kvs_[key] = cur;
    WatchEvent ev{WatchEventType::kPut, cur,
                  (it != kvs_.end()) ? prev : KeyValue{}};
    NotifyLocked(ev);
    if (out_rev) *out_rev = revision_;
    return true;
}

LeaseId InMemoryEtcdClient::LeaseGrant(uint32_t ttl_seconds, std::string*) {
    std::lock_guard lk(mu_);
    LeaseId id = next_lease_.fetch_add(1, std::memory_order_relaxed);
    leases_[id] = Lease{
        ttl_seconds,
        std::chrono::steady_clock::now() + std::chrono::seconds(ttl_seconds)
    };
    return id;
}

bool InMemoryEtcdClient::LeaseKeepAlive(LeaseId id, std::string* err) {
    std::lock_guard lk(mu_);
    auto it = leases_.find(id);
    if (it == leases_.end()) {
        if (err) *err = "etcd: lease not found"; return false;
    }
    it->second.deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(it->second.ttl_seconds);
    return true;
}

bool InMemoryEtcdClient::LeaseRevoke(LeaseId id, std::string* err) {
    std::lock_guard lk(mu_);
    auto it = leases_.find(id);
    if (it == leases_.end()) {
        if (err) *err = "etcd: lease not found"; return false;
    }
    ExpireLeaseLocked(id);
    return true;
}

uint32_t InMemoryEtcdClient::LeaseTTLRemaining(LeaseId id) const {
    std::lock_guard lk(mu_);
    auto it = leases_.find(id);
    if (it == leases_.end()) return 0;
    const auto now = std::chrono::steady_clock::now();
    if (it->second.deadline <= now) return 0;
    return static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::seconds>(
        it->second.deadline - now).count());
}

WatchHandle InMemoryEtcdClient::WatchPrefix(const std::string& prefix,
                                             WatchCallback cb) {
    std::lock_guard lk(mu_);
    auto id = next_watch_.fetch_add(1, std::memory_order_relaxed);
    watchers_[id] = Watcher{prefix, std::move(cb)};
    return id;
}

void InMemoryEtcdClient::Unwatch(WatchHandle handle) {
    std::lock_guard lk(mu_);
    watchers_.erase(handle);
}

// ===========================================================================
// GrpcEtcdClient — real etcd v3 over gRPC.
// ===========================================================================
//
// This implementation is gated on KVCACHE_ENABLE_ETCD. When the flag is on,
// the build expects the etcd v3 protos to be vendored at
// third_party/etcd-api/{rpc,kv,auth}.proto (stripped of gogo extensions) and
// for grpc++ to be discoverable via find_package(gRPC CONFIG).
//
// The pImpl below intentionally hides those dependencies from the public
// header so an out-of-the-box build (without etcd) still compiles.

struct GrpcEtcdClient::Impl {
    // TODO(stephen): hold std::shared_ptr<grpc::Channel>, generated stubs
    // (etcdserverpb::KV::Stub, Lease::Stub, Watch::Stub), plus a thread
    // dispatching incoming WatchResponse messages to local callbacks.
};

#if defined(KVCACHE_ENABLE_ETCD)

std::unique_ptr<GrpcEtcdClient>
GrpcEtcdClient::Create(const Options&, std::string* err) {
    // TODO(stephen): construct gRPC channel (insecure or TLS), grant a default
    // lease, start the watch dispatcher thread.
    if (err) *err = "GrpcEtcdClient: not yet implemented (vendor etcd protos)";
    return nullptr;
}

GrpcEtcdClient::~GrpcEtcdClient() = default;

bool GrpcEtcdClient::Put(const std::string&, const std::string&, LeaseId,
                          Revision*, std::string* err) {
    if (err) *err = "GrpcEtcdClient::Put: TODO"; return false;
}
std::optional<KeyValue> GrpcEtcdClient::Get(const std::string&, std::string* err) {
    if (err) *err = "GrpcEtcdClient::Get: TODO"; return std::nullopt;
}
std::vector<KeyValue> GrpcEtcdClient::GetPrefix(const std::string&, std::string*) {
    return {};
}
bool GrpcEtcdClient::Delete(const std::string&, std::string*) { return false; }
bool GrpcEtcdClient::PutIfRevision(const std::string&, const std::string&,
                                    Revision, LeaseId, Revision*, std::string*) {
    return false;
}
LeaseId GrpcEtcdClient::LeaseGrant(uint32_t, std::string*) { return kNoLease; }
bool     GrpcEtcdClient::LeaseKeepAlive(LeaseId, std::string*) { return false; }
bool     GrpcEtcdClient::LeaseRevoke   (LeaseId, std::string*) { return false; }
uint32_t GrpcEtcdClient::LeaseTTLRemaining(LeaseId) const { return 0; }
WatchHandle GrpcEtcdClient::WatchPrefix(const std::string&, WatchCallback) { return 0; }
void        GrpcEtcdClient::Unwatch(WatchHandle) {}

#else  // !KVCACHE_ENABLE_ETCD — facade returns errors so the tree compiles.

std::unique_ptr<GrpcEtcdClient>
GrpcEtcdClient::Create(const Options&, std::string* err) {
    if (err) *err = "GrpcEtcdClient: built without KVCACHE_ENABLE_ETCD";
    return nullptr;
}
GrpcEtcdClient::~GrpcEtcdClient() = default;
bool GrpcEtcdClient::Put(const std::string&, const std::string&, LeaseId,
                          Revision*, std::string* err) {
    if (err) *err = "GrpcEtcdClient: not built"; return false;
}
std::optional<KeyValue> GrpcEtcdClient::Get(const std::string&, std::string*) { return std::nullopt; }
std::vector<KeyValue>   GrpcEtcdClient::GetPrefix(const std::string&, std::string*) { return {}; }
bool GrpcEtcdClient::Delete(const std::string&, std::string*) { return false; }
bool GrpcEtcdClient::PutIfRevision(const std::string&, const std::string&,
                                    Revision, LeaseId, Revision*, std::string*) { return false; }
LeaseId  GrpcEtcdClient::LeaseGrant(uint32_t, std::string*) { return kNoLease; }
bool     GrpcEtcdClient::LeaseKeepAlive(LeaseId, std::string*) { return false; }
bool     GrpcEtcdClient::LeaseRevoke   (LeaseId, std::string*) { return false; }
uint32_t GrpcEtcdClient::LeaseTTLRemaining(LeaseId) const { return 0; }
WatchHandle GrpcEtcdClient::WatchPrefix(const std::string&, WatchCallback) { return 0; }
void        GrpcEtcdClient::Unwatch(WatchHandle) {}

#endif  // KVCACHE_ENABLE_ETCD

}  // namespace kvcache::node::cluster
