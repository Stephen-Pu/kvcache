// LLD §4.1 — Etcd client implementations.
#include "cluster/etcd_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#if defined(KVCACHE_HAVE_GRPC)
#  include <grpcpp/grpcpp.h>
#  include "etcdserverpb/rpc.pb.h"
#  include "etcdserverpb/rpc.grpc.pb.h"
#  include "mvccpb/kv.pb.h"
#endif

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
// Phase F-2. Gated on KVCACHE_HAVE_GRPC: when the build found
// `find_package(gRPC CONFIG)` and the vendored etcd protos under
// `third_party/etcd-proto/`, the file links `kvcache_etcd_proto` and
// compiles the real implementation below. Otherwise the `#else` branch
// keeps a no-op fallback so the tree builds on dev laptops without grpc.
//
// Coverage matches HttpEtcdClient: KV (Put/Get/GetPrefix/Delete/
// PutIfRevision via Txn) + Lease (Grant/KeepAlive/Revoke/TTL). Watch
// is polling-based, identical to the HTTP variant — the bidirectional
// stream lands in Phase F-3.

struct GrpcEtcdClient::Impl {
#if defined(KVCACHE_HAVE_GRPC)
    Options                                   opts;
    std::shared_ptr<grpc::Channel>            channel;
    std::unique_ptr<etcdserverpb::KV::Stub>   kv_stub;
    std::unique_ptr<etcdserverpb::Lease::Stub> lease_stub;
    std::unique_ptr<etcdserverpb::Watch::Stub> watch_stub;

    // Phase F-3 — one bidirectional Watch stream per client, multiplexing
    // every Watcher's events via the etcd-assigned `watch_id`.
    //
    // Lifecycle: the first WatchPrefix() call lazily opens the stream
    // and spawns a reader thread. Each subsequent WatchPrefix sends a
    // WatchCreateRequest (under writer_mu_) and blocks on `create_cv`
    // until the reader sees a `created=true` response carrying the
    // server-assigned watch_id. The reader dispatches subsequent
    // Put/Delete events to the matching callback. Unwatch sends a
    // WatchCancelRequest and removes the (watch_id → callback) entry.
    struct Watcher {
        WatchHandle   handle;
        std::string   prefix;
        WatchCallback cb;
        // After Create acks, the server assigns a watch_id; this maps
        // the client-side handle to it. Cleared on cancel/error.
        int64_t       watch_id = -1;
    };

    grpc::ClientContext                       watch_ctx;
    std::unique_ptr<
        grpc::ClientReaderWriter<etcdserverpb::WatchRequest,
                                   etcdserverpb::WatchResponse>>
                                              watch_stream;
    std::mutex                                writer_mu;   // serialises Write()
    std::mutex                                w_mu;        // protects maps below
    std::condition_variable                   create_cv;
    // Pending creates: matched FIFO against the next `created=true`
    // response. Etcd preserves request order on the stream.
    std::deque<WatchHandle>                   pending_creates;
    std::unordered_map<WatchHandle, Watcher>  watchers;
    std::unordered_map<int64_t,  WatchHandle> id_to_handle;
    std::atomic<WatchHandle>                  next_watch{1};
    std::atomic<bool>                         stop{false};
    std::thread                               reader_thread;
#endif
};

#if defined(KVCACHE_HAVE_GRPC)

namespace {

// `range_end` for "all keys with this prefix" is the prefix with the
// last byte incremented (etcd v3 convention; same shape as
// http_etcd_client.cpp).
std::string PrefixRangeEnd(const std::string& prefix) {
    std::string e = prefix;
    if (e.empty()) return std::string(1, '\0');
    for (std::size_t i = e.size(); i > 0; ) {
        --i;
        if (static_cast<unsigned char>(e[i]) < 0xff) {
            ++e[i];
            e.resize(i + 1);
            return e;
        }
    }
    return std::string{};
}

KeyValue FromPbKv(const mvccpb::KeyValue& pb) {
    KeyValue k;
    k.key             = pb.key();
    k.value           = pb.value();
    k.create_revision = static_cast<Revision>(pb.create_revision());
    k.mod_revision    = static_cast<Revision>(pb.mod_revision());
    k.lease           = static_cast<LeaseId>(pb.lease());
    return k;
}

// Strip an http:// prefix so a callers-supplied "http://host:2379"
// works the same way as a bare "host:2379" via clientv3.
std::string StripScheme(const std::string& s) {
    if (s.rfind("http://", 0) == 0)  return s.substr(7);
    if (s.rfind("https://", 0) == 0) return s.substr(8);
    return s;
}

}  // namespace

std::unique_ptr<GrpcEtcdClient>
GrpcEtcdClient::Create(const Options& opts, std::string* err) {
    if (opts.endpoints.empty()) {
        if (err) *err = "GrpcEtcdClient: endpoints required";
        return nullptr;
    }
    auto self  = std::unique_ptr<GrpcEtcdClient>(new GrpcEtcdClient());
    self->impl_ = std::make_unique<Impl>();
    self->impl_->opts = opts;

    // Build a comma-separated target string for the multi-endpoint
    // round-robin balancer. Per gRPC docs the scheme-less host:port
    // list is the canonical form.
    std::string target;
    for (std::size_t i = 0; i < opts.endpoints.size(); ++i) {
        if (i > 0) target.push_back(',');
        target += StripScheme(opts.endpoints[i]);
    }

    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (!opts.ca_pem_path.empty() || !opts.client_cert_pem_path.empty()) {
        // Load the PEM material the operator emits into the Secret.
        auto read_file = [](const std::string& p) -> std::string {
            std::ifstream f(p); std::stringstream ss;
            if (f) ss << f.rdbuf();
            return ss.str();
        };
        grpc::SslCredentialsOptions sslo;
        if (!opts.ca_pem_path.empty()) sslo.pem_root_certs = read_file(opts.ca_pem_path);
        if (!opts.client_key_pem_path.empty()) sslo.pem_private_key = read_file(opts.client_key_pem_path);
        if (!opts.client_cert_pem_path.empty()) sslo.pem_cert_chain = read_file(opts.client_cert_pem_path);
        creds = grpc::SslCredentials(sslo);
    } else {
        creds = grpc::InsecureChannelCredentials();
    }

    self->impl_->channel = grpc::CreateChannel(target, creds);
    self->impl_->kv_stub    = etcdserverpb::KV::NewStub(self->impl_->channel);
    self->impl_->lease_stub = etcdserverpb::Lease::NewStub(self->impl_->channel);
    self->impl_->watch_stub = etcdserverpb::Watch::NewStub(self->impl_->channel);

    // Smoke-test: a benign Range(empty key). Fails fast when etcd is
    // unreachable so callers see the error at Create-time.
    grpc::ClientContext ctx;
    auto deadline = std::chrono::system_clock::now() + opts.dial_timeout;
    ctx.set_deadline(deadline);
    etcdserverpb::RangeRequest  req;
    etcdserverpb::RangeResponse resp;
    req.set_key(std::string(1, '\0'));
    auto s = self->impl_->kv_stub->Range(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd dial: ") + s.error_message();
        return nullptr;
    }
    return self;
}

GrpcEtcdClient::~GrpcEtcdClient() {
    if (!impl_) return;
    impl_->stop.store(true, std::memory_order_release);
    // Cancel the bidi stream so the reader thread's Read() unblocks.
    if (impl_->watch_stream) {
        impl_->watch_ctx.TryCancel();
        {
            std::lock_guard<std::mutex> lk(impl_->writer_mu);
            impl_->watch_stream->WritesDone();
        }
    }
    if (impl_->reader_thread.joinable()) impl_->reader_thread.join();
    if (impl_->watch_stream) {
        // Drain the Finish() so we don't trip the grpc++ assert about
        // calling Finish only after the reader returned false.
        (void)impl_->watch_stream->Finish();
    }
}

bool GrpcEtcdClient::Put(const std::string& key, const std::string& value,
                          LeaseId lease, Revision* out_rev, std::string* err) {
    etcdserverpb::PutRequest  req;
    etcdserverpb::PutResponse resp;
    req.set_key(key);
    req.set_value(value);
    req.set_lease(static_cast<int64_t>(lease));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->kv_stub->Put(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd Put: ") + s.error_message();
        return false;
    }
    if (out_rev && resp.has_header()) {
        *out_rev = static_cast<Revision>(resp.header().revision());
    }
    return true;
}

std::optional<KeyValue> GrpcEtcdClient::Get(const std::string& key,
                                              std::string* err) {
    etcdserverpb::RangeRequest  req;
    etcdserverpb::RangeResponse resp;
    req.set_key(key);
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->kv_stub->Range(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd Get: ") + s.error_message();
        return std::nullopt;
    }
    if (resp.kvs_size() == 0) return std::nullopt;
    return FromPbKv(resp.kvs(0));
}

std::vector<KeyValue> GrpcEtcdClient::GetPrefix(const std::string& prefix,
                                                  std::string* err) {
    etcdserverpb::RangeRequest  req;
    etcdserverpb::RangeResponse resp;
    req.set_key(prefix);
    req.set_range_end(PrefixRangeEnd(prefix));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->kv_stub->Range(&ctx, req, &resp);
    std::vector<KeyValue> out;
    if (!s.ok()) {
        if (err) *err = std::string("etcd GetPrefix: ") + s.error_message();
        return out;
    }
    out.reserve(resp.kvs_size());
    for (int i = 0; i < resp.kvs_size(); ++i) out.push_back(FromPbKv(resp.kvs(i)));
    return out;
}

bool GrpcEtcdClient::Delete(const std::string& key, std::string* err) {
    etcdserverpb::DeleteRangeRequest  req;
    etcdserverpb::DeleteRangeResponse resp;
    req.set_key(key);
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->kv_stub->DeleteRange(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd Delete: ") + s.error_message();
        return false;
    }
    return resp.deleted() > 0;
}

bool GrpcEtcdClient::PutIfRevision(const std::string& key,
                                     const std::string& value,
                                     Revision expected_rev, LeaseId lease,
                                     Revision* out_rev, std::string* err) {
    etcdserverpb::TxnRequest  req;
    etcdserverpb::TxnResponse resp;
    auto* cmp = req.add_compare();
    cmp->set_target(etcdserverpb::Compare::MOD);
    cmp->set_result(etcdserverpb::Compare::EQUAL);
    cmp->set_key(key);
    cmp->set_mod_revision(static_cast<int64_t>(expected_rev));
    auto* op = req.add_success();
    auto* put = op->mutable_request_put();
    put->set_key(key);
    put->set_value(value);
    put->set_lease(static_cast<int64_t>(lease));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->kv_stub->Txn(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd Txn: ") + s.error_message();
        return false;
    }
    if (!resp.succeeded()) {
        if (err) *err = "etcd: revision mismatch";
        return false;
    }
    if (out_rev && resp.has_header()) {
        *out_rev = static_cast<Revision>(resp.header().revision());
    }
    return true;
}

LeaseId GrpcEtcdClient::LeaseGrant(uint32_t ttl_seconds, std::string* err) {
    etcdserverpb::LeaseGrantRequest  req;
    etcdserverpb::LeaseGrantResponse resp;
    req.set_ttl(static_cast<int64_t>(ttl_seconds));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->lease_stub->LeaseGrant(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd LeaseGrant: ") + s.error_message();
        return kNoLease;
    }
    if (!resp.error().empty()) {
        if (err) *err = "etcd LeaseGrant: " + resp.error();
        return kNoLease;
    }
    return static_cast<LeaseId>(resp.id());
}

bool GrpcEtcdClient::LeaseKeepAlive(LeaseId id, std::string* err) {
    // Single-shot ping via LeaseTimeToLive — matches the convention
    // HttpEtcdClient uses (the bidi keepalive stream is Phase F-3).
    etcdserverpb::LeaseTimeToLiveRequest  req;
    etcdserverpb::LeaseTimeToLiveResponse resp;
    req.set_id(static_cast<int64_t>(id));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->lease_stub->LeaseTimeToLive(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd LeaseTimeToLive: ") + s.error_message();
        return false;
    }
    if (resp.ttl() <= 0) {
        if (err) *err = "etcd: lease expired or not found";
        return false;
    }
    return true;
}

bool GrpcEtcdClient::LeaseRevoke(LeaseId id, std::string* err) {
    etcdserverpb::LeaseRevokeRequest  req;
    etcdserverpb::LeaseRevokeResponse resp;
    req.set_id(static_cast<int64_t>(id));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->lease_stub->LeaseRevoke(&ctx, req, &resp);
    if (!s.ok()) {
        if (err) *err = std::string("etcd LeaseRevoke: ") + s.error_message();
        return false;
    }
    return true;
}

uint32_t GrpcEtcdClient::LeaseTTLRemaining(LeaseId id) const {
    etcdserverpb::LeaseTimeToLiveRequest  req;
    etcdserverpb::LeaseTimeToLiveResponse resp;
    req.set_id(static_cast<int64_t>(id));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + impl_->opts.dial_timeout);
    auto s = impl_->lease_stub->LeaseTimeToLive(&ctx, req, &resp);
    if (!s.ok() || resp.ttl() < 0) return 0;
    return static_cast<uint32_t>(resp.ttl());
}

// Phase F-3 — bidi-stream watcher.
//
// First WatchPrefix() lazily opens the stream and spawns a reader
// thread. Each call sends a WatchCreateRequest and blocks until the
// reader observes the matching `created=true` ack and binds the
// server-assigned `watch_id` to the client-side handle. Subsequent
// events arrive on the same stream multiplexed by watch_id.
WatchHandle GrpcEtcdClient::WatchPrefix(const std::string& prefix,
                                          WatchCallback cb) {
    // Lazily open the stream + spawn reader on the first watcher.
    {
        std::lock_guard<std::mutex> lk(impl_->writer_mu);
        if (!impl_->watch_stream) {
            impl_->watch_stream = impl_->watch_stub->Watch(&impl_->watch_ctx);
            impl_->reader_thread = std::thread([this] {
                etcdserverpb::WatchResponse resp;
                while (impl_->watch_stream->Read(&resp)) {
                    // Match a `created=true` ack to the next pending
                    // WatchPrefix() call (etcd preserves request order
                    // on the stream, so FIFO works).
                    if (resp.created()) {
                        std::lock_guard<std::mutex> lk(impl_->w_mu);
                        if (impl_->pending_creates.empty()) continue;
                        const WatchHandle h = impl_->pending_creates.front();
                        impl_->pending_creates.pop_front();
                        auto it = impl_->watchers.find(h);
                        if (it != impl_->watchers.end()) {
                            it->second.watch_id = resp.watch_id();
                            impl_->id_to_handle[resp.watch_id()] = h;
                        }
                        impl_->create_cv.notify_all();
                        continue;
                    }
                    if (resp.canceled()) {
                        std::lock_guard<std::mutex> lk(impl_->w_mu);
                        auto it = impl_->id_to_handle.find(resp.watch_id());
                        if (it != impl_->id_to_handle.end()) {
                            impl_->id_to_handle.erase(it);
                        }
                        continue;
                    }
                    // Event batch — find the callback by watch_id and
                    // dispatch each event without holding the lock (the
                    // user callback might re-enter the client).
                    WatchCallback cb_copy;
                    {
                        std::lock_guard<std::mutex> lk(impl_->w_mu);
                        auto it = impl_->id_to_handle.find(resp.watch_id());
                        if (it == impl_->id_to_handle.end()) continue;
                        auto wit = impl_->watchers.find(it->second);
                        if (wit == impl_->watchers.end()) continue;
                        cb_copy = wit->second.cb;
                    }
                    for (const auto& ev_pb : resp.events()) {
                        WatchEvent ev;
                        ev.type = ev_pb.type() == mvccpb::Event::DELETE
                                      ? WatchEventType::kDelete
                                      : WatchEventType::kPut;
                        if (ev_pb.has_kv())      ev.kv      = FromPbKv(ev_pb.kv());
                        if (ev_pb.has_prev_kv()) ev.prev_kv = FromPbKv(ev_pb.prev_kv());
                        cb_copy(ev);
                    }
                }
                // Stream closed — wake any waiters so they don't hang.
                std::lock_guard<std::mutex> lk(impl_->w_mu);
                impl_->create_cv.notify_all();
            });
        }
    }

    const WatchHandle h = impl_->next_watch.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(impl_->w_mu);
        Impl::Watcher w;
        w.handle = h;
        w.prefix = prefix;
        w.cb     = std::move(cb);
        impl_->watchers[h] = std::move(w);
        impl_->pending_creates.push_back(h);
    }

    // Send the WatchCreateRequest. The writer mutex serialises stream
    // writes (gRPC's bidi stream is single-Write at a time).
    etcdserverpb::WatchRequest req;
    auto* cr = req.mutable_create_request();
    cr->set_key(prefix);
    cr->set_range_end(PrefixRangeEnd(prefix));
    {
        std::lock_guard<std::mutex> lk(impl_->writer_mu);
        if (!impl_->watch_stream->Write(req)) {
            // Write failed — drop the watcher and surface a "dead handle".
            std::lock_guard<std::mutex> lk2(impl_->w_mu);
            impl_->watchers.erase(h);
            // Best-effort: remove from pending_creates if still there.
            for (auto it = impl_->pending_creates.begin();
                 it != impl_->pending_creates.end(); ++it) {
                if (*it == h) { impl_->pending_creates.erase(it); break; }
            }
            return 0;
        }
    }

    // Wait until the reader maps a watch_id to this handle (or stop).
    std::unique_lock<std::mutex> lk(impl_->w_mu);
    impl_->create_cv.wait_for(lk, std::chrono::seconds(5), [&] {
        auto it = impl_->watchers.find(h);
        return impl_->stop.load(std::memory_order_acquire) ||
               (it != impl_->watchers.end() && it->second.watch_id >= 0);
    });
    auto it = impl_->watchers.find(h);
    if (it == impl_->watchers.end() || it->second.watch_id < 0) {
        // Timeout / error: don't leak the entry.
        if (it != impl_->watchers.end()) impl_->watchers.erase(it);
        for (auto pit = impl_->pending_creates.begin();
             pit != impl_->pending_creates.end(); ++pit) {
            if (*pit == h) { impl_->pending_creates.erase(pit); break; }
        }
        return 0;
    }
    return h;
}

void GrpcEtcdClient::Unwatch(WatchHandle h) {
    int64_t watch_id = -1;
    {
        std::lock_guard<std::mutex> lk(impl_->w_mu);
        auto it = impl_->watchers.find(h);
        if (it == impl_->watchers.end()) return;
        watch_id = it->second.watch_id;
        if (watch_id >= 0) impl_->id_to_handle.erase(watch_id);
        impl_->watchers.erase(it);
    }
    if (watch_id < 0 || !impl_->watch_stream) return;
    etcdserverpb::WatchRequest req;
    req.mutable_cancel_request()->set_watch_id(watch_id);
    std::lock_guard<std::mutex> lk(impl_->writer_mu);
    (void)impl_->watch_stream->Write(req);
}

#else  // !KVCACHE_HAVE_GRPC — facade returns errors so the tree compiles.

std::unique_ptr<GrpcEtcdClient>
GrpcEtcdClient::Create(const Options&, std::string* err) {
    if (err) *err = "GrpcEtcdClient: built without grpc++. "
                     "Reconfigure with grpc available "
                     "(brew install grpc / apt-get install libgrpc++-dev) "
                     "to enable.";
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

#endif  // KVCACHE_HAVE_GRPC

}  // namespace kvcache::node::cluster
