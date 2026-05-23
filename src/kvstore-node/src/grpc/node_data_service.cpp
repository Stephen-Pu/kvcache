// Phase M-1 — NodeData gRPC handlers, forwarding to the C ABI.
//
// Each handler is a thin proto<->C-struct translator plus a single
// kv_* call. Error mapping follows the convention:
//
//   KV_OK              → grpc::Status::OK
//   KV_E_NOT_FOUND     → grpc::Status::NOT_FOUND
//   KV_E_INVAL         → grpc::Status::INVALID_ARGUMENT
//   anything else      → grpc::Status::INTERNAL (with kv_status_str text)
//
// The intent matches what an agent would do if it hit the C ABI directly,
// so test failures look identical whether you go through the wire or not.
#include "grpc/node_data_service.h"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include "kvcache/kv_errors.h"
#include "kvcache/kv_types.h"

namespace kvcache::node::grpc_server {

namespace {

// FNV-1a 64-bit hash, identical to the C ABI's tenant-string hash so
// engine-side and server-side land on the same tenant bucket when the
// request's tenant_id string matches what the engine opened its ctx
// with. Bytes-form (used for the Locator's 16-byte tenant_id) yields
// a deterministic-but-distinct namespace; that's fine because the
// PriorityScheduler only cares about *some* stable per-tenant id.
uint64_t Fnv1a64Bytes(const void* data, std::size_t n) {
    auto* p = static_cast<const uint8_t*>(data);
    uint64_t h = 0xcbf29ce484222325ULL;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

uint64_t HashTenantString(const std::string& s) {
    return s.empty() ? 0 : Fnv1a64Bytes(s.data(), s.size());
}

uint64_t HashTenantBytes16(const std::string& s) {
    // Locator.tenant_id is 16 bytes (may include trailing nulls).
    return s.empty() ? 0 : Fnv1a64Bytes(s.data(), s.size());
}

::grpc::Status ToGrpcStatus(int rc, const char* op) {
    switch (rc) {
        case KV_OK:           return ::grpc::Status::OK;
        case KV_E_NOT_FOUND:  return {::grpc::StatusCode::NOT_FOUND,
                                       std::string(op) + ": not found"};
        case KV_E_INVAL:      return {::grpc::StatusCode::INVALID_ARGUMENT,
                                       std::string(op) + ": invalid argument"};
        case KV_E_NOMEM:      return {::grpc::StatusCode::RESOURCE_EXHAUSTED,
                                       std::string(op) + ": out of memory"};
        case KV_E_TIMEOUT:    return {::grpc::StatusCode::DEADLINE_EXCEEDED,
                                       std::string(op) + ": timeout"};
        case KV_E_PERM:       return {::grpc::StatusCode::PERMISSION_DENIED,
                                       std::string(op) + ": permission denied"};
        case KV_E_QUOTA:      return {::grpc::StatusCode::RESOURCE_EXHAUSTED,
                                       std::string(op) + ": quota exceeded"};
        case KV_E_SEALED:     return {::grpc::StatusCode::FAILED_PRECONDITION,
                                       std::string(op) + ": handle already sealed"};
        case KV_E_NOT_SEALED: return {::grpc::StatusCode::FAILED_PRECONDITION,
                                       std::string(op) + ": handle not sealed"};
        default:
            return {::grpc::StatusCode::INTERNAL,
                      std::string(op) + ": " + kv_status_str(rc) +
                      " (status=" + std::to_string(rc) + ")"};
    }
}

void FromProtoLocator(const kvcache::proto::Locator& in, kv_locator_t* out) {
    std::memset(out, 0, sizeof(*out));
    const std::string& tid = in.tenant_id();
    std::memcpy(out->tenant_id, tid.data(),
                  std::min<std::size_t>(tid.size(), sizeof(out->tenant_id)));
    out->model_id_hash = in.model_id_hash();
    const std::string& ph = in.prefix_hash();
    std::memcpy(out->prefix_hash, ph.data(),
                  std::min<std::size_t>(ph.size(), sizeof(out->prefix_hash)));
    if (in.has_range()) {
        const auto& r = in.range();
        out->range.layer_start = static_cast<uint16_t>(r.layer_start());
        out->range.layer_count = static_cast<uint16_t>(r.layer_count());
        out->range.head_start  = static_cast<uint16_t>(r.head_start());
        out->range.head_count  = static_cast<uint16_t>(r.head_count());
        out->range.token_start = r.token_start();
        out->range.token_count = r.token_count();
    }
    out->version = in.version() == 0 ? 1 : in.version();
    out->flags   = in.flags();
}

void ToProtoLocator(const kv_locator_t& in, kvcache::proto::Locator* out) {
    out->set_tenant_id(std::string(reinterpret_cast<const char*>(in.tenant_id),
                                       sizeof(in.tenant_id)));
    out->set_model_id_hash(in.model_id_hash);
    out->set_prefix_hash(std::string(reinterpret_cast<const char*>(in.prefix_hash),
                                         sizeof(in.prefix_hash)));
    auto* r = out->mutable_range();
    r->set_layer_start(in.range.layer_start);
    r->set_layer_count(in.range.layer_count);
    r->set_head_start (in.range.head_start);
    r->set_head_count (in.range.head_count);
    r->set_token_start(in.range.token_start);
    r->set_token_count(in.range.token_count);
    out->set_version(in.version);
    out->set_flags  (in.flags);
}

}  // namespace

NodeDataServiceImpl::~NodeDataServiceImpl() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& kv : cache_) {
        // Owned by the service — close on shutdown. default_ctx_ is
        // never inserted into cache_, so it's not double-closed here.
        if (kv.second) kv_ctx_close(kv.second);
    }
    cache_.clear();
    handle_to_ctx_.clear();
}

std::size_t NodeDataServiceImpl::CachedCtxCount() const {
    std::lock_guard<std::mutex> lk(mu_);
    return cache_.size();
}

kv_ctx_t* NodeDataServiceImpl::GetOrOpenCtx(uint64_t tenant_hash,
                                             uint64_t model_hash) {
    const uint64_t key = MakeKey(tenant_hash, model_hash);
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = cache_.find(key);
        if (it != cache_.end()) return it->second;
    }
    // Open outside the lock — kv_ctx_open_from_hashes may take its own
    // singletons internally; we don't want to nest.
    kv_ctx_t* fresh = nullptr;
    int rc = kv_ctx_open_from_hashes(KVCACHE_ABI_VERSION, tenant_hash,
                                      model_hash, /*flags=*/0, &fresh);
    if (rc != KV_OK || !fresh) return nullptr;
    {
        std::lock_guard<std::mutex> lk(mu_);
        // Resolve the race: if another thread inserted concurrently, drop ours.
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            kv_ctx_close(fresh);
            return it->second;
        }
        cache_.emplace(key, fresh);
    }
    return fresh;
}

void NodeDataServiceImpl::RememberHandle(uint64_t h, kv_ctx_t* ctx) {
    if (!ctx) return;
    std::lock_guard<std::mutex> lk(mu_);
    handle_to_ctx_[h] = ctx;
}

void NodeDataServiceImpl::ForgetHandle(uint64_t h) {
    std::lock_guard<std::mutex> lk(mu_);
    handle_to_ctx_.erase(h);
}

kv_ctx_t* NodeDataServiceImpl::CtxForHandle(uint64_t h) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = handle_to_ctx_.find(h);
    if (it != handle_to_ctx_.end()) return it->second;
    return default_ctx_;
}

::grpc::Status NodeDataServiceImpl::Lookup(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::LookupRequest* request,
    kvcache::proto::LookupResponse*      response) {
    // Route into the (tenant_hash, model_hash) ctx for this request so
    // per-tenant scheduler buckets are honoured even when many tenants
    // multiplex over one connection.
    const uint64_t th = HashTenantString(request->tenant_id());
    const uint64_t mh = request->model_id_hash();
    kv_ctx_t* ctx = GetOrOpenCtx(th, mh);
    if (!ctx) return {::grpc::StatusCode::INTERNAL, "kv_ctx_open_from_hashes failed"};

    std::vector<uint32_t> tokens(request->tokens().begin(),
                                   request->tokens().end());
    kv_locator_t meta{};
    kv_handle_t  handle  = 0;
    uint32_t     matched = 0;
    int rc = kv_lookup(ctx, tokens.data(), tokens.size(),
                         &meta, &handle, &matched);
    if (rc == KV_E_NOT_FOUND) {
        response->set_hit(false);
        return ::grpc::Status::OK;
    }
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_lookup");

    RememberHandle(handle, ctx);
    response->set_hit(true);
    ToProtoLocator(meta, response->mutable_locator());
    response->set_server_handle(handle);
    response->set_matched_tokens(matched);
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Reserve(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::ReserveRequest* request,
    kvcache::proto::ReserveResponse*      response) {
    // Reserve carries identity inside the embedded Locator.
    const uint64_t th = HashTenantBytes16(request->locator().tenant_id());
    const uint64_t mh = request->locator().model_id_hash();
    kv_ctx_t* ctx = GetOrOpenCtx(th, mh);
    if (!ctx) return {::grpc::StatusCode::INTERNAL, "kv_ctx_open_from_hashes failed"};

    kv_locator_t loc{};
    FromProtoLocator(request->locator(), &loc);

    kv_handle_t      h    = 0;
    kv_buffer_desc_t slot{};
    int rc = kv_reserve(ctx, &loc, request->bytes(), &h, &slot);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_reserve");

    RememberHandle(h, ctx);
    response->set_server_handle(h);
    response->set_slot_iova(reinterpret_cast<uint64_t>(slot.addr));
    response->set_slot_bytes(slot.len);
    response->set_mr_key(slot.mr_key);

    // Phase M-3 B — best-effort export of the slot MR. The loopback
    // backend only knows about MRs that were registered through its
    // RegisterRegion, so this typically fails for slot.mr_key == 0
    // (pinned tier opened without a register_region callback). When
    // ExportMr fails we leave the descriptor empty; in-process callers
    // ignore it and fall back to slot_iova / mr_key as today.
    if (slot.mr_key != 0) {
        std::size_t need = 0;
        if (kv_export_mr(ctx, slot.mr_key, nullptr, 0, &need) == KV_E_NOMEM
            && need > 0) {
            std::string buf(need, '\0');
            if (kv_export_mr(ctx, slot.mr_key,
                              reinterpret_cast<uint8_t*>(buf.data()),
                              buf.size(), &need) == KV_OK) {
                response->set_remote_mr_descriptor(std::move(buf));
            }
        }
    }
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Publish(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::PublishRequest* request,
    kvcache::proto::PublishResponse*      response) {
    kv_ctx_t* ctx = CtxForHandle(request->server_handle());
    if (!ctx) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    // Send an empty buffer descriptor since the caller has already
    // written into the previously-reserved slot.
    kv_buffer_desc_t empty{};
    int rc = kv_publish(ctx, request->server_handle(), empty,
                          request->watermark());
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_publish");
    response->set_queue_position(0);  // backpressure metric — Phase M-2
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Fetch(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::FetchRequest* request,
    kvcache::proto::FetchResponse*      response) {
    kv_ctx_t* ctx = CtxForHandle(request->server_handle());
    if (!ctx) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    kv_buffer_desc_t dst{};
    dst.addr     = reinterpret_cast<void*>(request->dst_iova());
    dst.len      = request->dst_bytes();
    dst.mem_type = 0;  // KV_MEM_HOST
    dst.mr_key   = request->dst_mr_key();

    // Phase M-3 B — if the client supplied a RemoteMrDescriptor for
    // its destination buffer, import it through the NIXL backend and
    // use the resulting local key as dst.mr_key. The legacy in-process
    // path (dst_iova + dst_mr_key) stays the fallback so existing
    // callers keep working.
    const std::string& rmd = request->dst_remote_mr_descriptor();
    if (!rmd.empty()) {
        uint32_t imported = 0;
        int rc = kv_import_remote_mr(ctx,
                                       reinterpret_cast<const uint8_t*>(rmd.data()),
                                       rmd.size(), &imported);
        if (rc != KV_OK) return ToGrpcStatus(rc, "kv_import_remote_mr");
        dst.mr_key = imported;
    }

    // The proto carries `repeated Range ranges`; the C ABI takes a flat
    // C array. We translate them 1:1 — empty list means "whole prefix".
    std::vector<kv_range_t> ranges;
    ranges.reserve(request->ranges_size());
    for (const auto& r : request->ranges()) {
        kv_range_t out{};
        out.layer_start = static_cast<uint16_t>(r.layer_start());
        out.layer_count = static_cast<uint16_t>(r.layer_count());
        out.head_start  = static_cast<uint16_t>(r.head_start());
        out.head_count  = static_cast<uint16_t>(r.head_count());
        out.token_start = r.token_start();
        out.token_count = r.token_count();
        ranges.push_back(out);
    }
    kv_completion_t cid = 0;
    int rc = kv_fetch(ctx, request->server_handle(),
                        ranges.empty() ? nullptr : ranges.data(),
                        ranges.size(), dst, &cid);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_fetch");
    rc = kv_wait(ctx, cid, 5000);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_wait");
    response->set_completion_id(cid);
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Seal(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::SealRequest* request,
    kvcache::proto::SealResponse*      response) {
    kv_ctx_t* ctx = CtxForHandle(request->server_handle());
    if (!ctx) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    // Phase M-2: the proto now carries the token list. We forward it
    // straight to kv_seal as the ART path source — same shape every
    // Python adapter uses via its connector layer.
    std::vector<uint32_t> tokens(request->tokens().begin(),
                                   request->tokens().end());
    int rc = kv_seal(ctx, request->server_handle(),
                       tokens.empty() ? nullptr : tokens.data(),
                       tokens.size());
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_seal");
    response->mutable_locator();  // empty Locator; Phase H-3 fills in if needed
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Release(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::ReleaseRequest* request,
    kvcache::proto::ReleaseResponse*      /*response*/) {
    kv_ctx_t* ctx = CtxForHandle(request->server_handle());
    if (!ctx) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    int rc = kv_release(ctx, request->server_handle());
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_release");
    ForgetHandle(request->server_handle());
    return ::grpc::Status::OK;
}

// ---- Subscribe streaming (Phase M-2) -----------------------------------
//
// The C ABI's kv_subscribe_events drives a per-ctx poller thread that
// fires our callback for each Add / Evict / Promote / Demote event the
// HeadlessNode publishes. We bridge that into the gRPC stream by
// shoving every event onto a thread-safe queue and letting the RPC
// thread drain it via ServerWriter::Write. The RPC stays in this
// function until the client cancels or the writer fails; on exit we
// always unsubscribe (idempotent).

namespace {

// Bridge state for the duration of one Subscribe RPC. The callback
// fires on the poller thread inside the kv_ctx_t; the gRPC thread
// reads from this queue and writes to the wire.
struct SubBridge {
    std::mutex                              mu;
    std::condition_variable                 cv;
    std::deque<kvcache::proto::Event>       queue;
    bool                                    closed = false;

    void Push(const kv_event_t* in) {
        kvcache::proto::Event ev;
        // Map the C ABI's int32_t enum to proto's EventType.
        switch (in->type) {
            case KV_EVENT_ADD:     ev.set_type(kvcache::proto::EVENT_ADD); break;
            case KV_EVENT_EVICT:   ev.set_type(kvcache::proto::EVENT_EVICT); break;
            case KV_EVENT_PROMOTE: ev.set_type(kvcache::proto::EVENT_PROMOTE); break;
            case KV_EVENT_DEMOTE:  ev.set_type(kvcache::proto::EVENT_DEMOTE); break;
            default:               ev.set_type(kvcache::proto::EVENT_UNSPECIFIED);
        }
        ev.set_epoch(in->epoch);
        // Skip Locator / Tier for the MVP — proto Event carries them
        // but the gRPC test only needs type + epoch to verify the
        // path works. Full mapping is mechanical and lands when a
        // consumer actually reads those fields.
        std::lock_guard<std::mutex> lk(mu);
        queue.push_back(std::move(ev));
        cv.notify_one();
    }

    // Block until an event arrives or the bridge is closed.
    // Returns false on close (no event was produced).
    bool Pop(kvcache::proto::Event* out,
             std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mu);
        if (!cv.wait_for(lk, timeout,
                          [&] { return closed || !queue.empty(); })) {
            return false;  // timeout
        }
        if (queue.empty()) return false;  // closed
        *out = std::move(queue.front());
        queue.pop_front();
        return true;
    }

    void Close() {
        std::lock_guard<std::mutex> lk(mu);
        closed = true;
        cv.notify_all();
    }
};

void SubBridgeCallback(const kv_event_t* event, void* user) {
    static_cast<SubBridge*>(user)->Push(event);
}

}  // namespace

::grpc::Status NodeDataServiceImpl::Subscribe(
    ::grpc::ServerContext* context,
    const kvcache::proto::SubscribeRequest* /*request*/,
    ::grpc::ServerWriter<kvcache::proto::Event>* writer) {
    // Subscribe carries only tenant_id (no model); we use the bootstrap
    // default ctx since HeadlessNode events are process-wide anyway.
    kv_ctx_t* ctx = default_ctx_;
    if (!ctx) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};

    SubBridge bridge;
    const int rc = kv_subscribe_events(ctx, &SubBridgeCallback, &bridge);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_subscribe_events");

    // Loop until the client cancels (or Write fails). We poll with a
    // small timeout so we can spot context cancellation responsively.
    while (!context->IsCancelled()) {
        kvcache::proto::Event ev;
        if (!bridge.Pop(&ev, std::chrono::milliseconds(100))) {
            continue;  // timeout — check cancellation, then loop
        }
        if (!writer->Write(ev)) break;  // client disconnected
    }
    bridge.Close();
    kv_unsubscribe_events(ctx);
    return ::grpc::Status::OK;
}

}  // namespace kvcache::node::grpc_server
