// HeadlessNode — in-process backend for the Core ABI.
#include "headless_node.h"

#include <sys/stat.h>

#include <cstring>

#include "kvcache/kv_errors.h"
#include "metrics.h"  // kvcache::metrics::Registry + Counter/Gauge
#include "prefix/art_snapshot.h"
#include "prefix/art_wal.h"
#include "trace.h"

namespace kvcache::abi {

namespace {

HeadlessNode*       g_singleton = nullptr;

// Phase G-3 — backpressure metrics. Lazy-initialised on first use so
// builds without the metrics registry still link. Mirrors the static-
// + lambda pattern PriorityScheduler uses.
struct ReserveMetrics {
    kvcache::metrics::Counter* nomem_total       = nullptr;
    kvcache::metrics::Counter* invalid_total     = nullptr;
    kvcache::metrics::Counter* reserves_total    = nullptr;
    kvcache::metrics::Gauge*   slots_in_use      = nullptr;
    kvcache::metrics::Gauge*   slots_total       = nullptr;
    kvcache::metrics::Gauge*   slots_utilization = nullptr;  // [0..1]
};
ReserveMetrics& Rm() {
    static ReserveMetrics m = [] {
        ReserveMetrics x;
        auto& r = kvcache::metrics::Registry::Default();
        x.nomem_total = &r.GetOrCreateCounter(
            "kv_reserve_nomem_total",
            "Reserves rejected because the pinned slot pool was exhausted.",
            /*labels=*/{});
        x.invalid_total = &r.GetOrCreateCounter(
            "kv_reserve_invalid_total",
            "Reserves rejected for argument validation (null / zero-byte / oversized).",
            {});
        x.reserves_total = &r.GetOrCreateCounter(
            "kv_reserves_total",
            "Successful Reserves issued by the node.",
            {});
        x.slots_in_use = &r.GetOrCreateGauge(
            "kv_pinned_tier_slots_in_use",
            "Pinned-tier slots currently held by an in-flight Reserve.",
            {});
        x.slots_total = &r.GetOrCreateGauge(
            "kv_pinned_tier_slots_total",
            "Pinned-tier slot capacity (constant after Init).",
            {});
        x.slots_utilization = &r.GetOrCreateGauge(
            "kv_pinned_tier_slots_utilization_ratio",
            "Fraction of pinned-tier slots in use (0=idle, 1=saturated).",
            {});
        // Seed each series with a zero so Scrape() emits a line for
        // it even before the first event fires — Prometheus consumers
        // and dashboards expect the metric to be present at t=0.
        x.nomem_total->Inc(0.0, {});
        x.invalid_total->Inc(0.0, {});
        x.reserves_total->Inc(0.0, {});
        x.slots_in_use->Set(0.0, {});
        x.slots_total->Set(0.0, {});
        x.slots_utilization->Set(0.0, {});
        return x;
    }();
    return m;
}

inline void BumpPinnedTierGauges(node::tier::TierManager* tm) {
    if (!tm) return;
    const auto in_use = static_cast<double>(tm->pinned().SlotsInUse());
    const auto total  = static_cast<double>(tm->pinned().SlotCount());
    Rm().slots_in_use->Set(in_use, {});
    Rm().slots_total->Set(total,  {});
    Rm().slots_utilization->Set(
        total > 0 ? in_use / total : 0.0, {});
}

// Build a DramKey from a Locator's identity (tenant|model|prefix).
node::tier::DramKey LocatorContentKey(const kv_locator_t& loc) {
    node::tier::DramKey k{};
    // Mix the identity into 16 bytes via a simple XOR-fold of the 40 identity
    // bytes (tenant 16 + model 8 + prefix 16). Replace with BLAKE3-128 once
    // the hash facade is wired against a real vendored impl.
    for (int i = 0; i < 16; ++i) {
        k.bytes[i] = loc.tenant_id[i] ^ loc.prefix_hash[i];
    }
    uint64_t m = loc.model_id_hash;
    for (int i = 0; i < 8; ++i) {
        k.bytes[i] ^= static_cast<uint8_t>(m & 0xff);
        m >>= 8;
    }
    return k;
}

}  // namespace

HeadlessNode* HeadlessNode::GetOrCreate(const Options& opts, std::string* err) {
    if (g_singleton) return g_singleton;
    auto* n = new HeadlessNode();
    if (!n->Init(opts, err)) { delete n; return nullptr; }
    g_singleton = n;
    return n;
}

void HeadlessNode::Shutdown() {
    delete g_singleton;
    g_singleton = nullptr;
}

HeadlessNode::~HeadlessNode() {
    // Stop the sweeper first; everything below may otherwise be torn
    // down while SweeperLoop is mid-iteration.
    sweeper_stop_.store(true, std::memory_order_release);
    defer_cv_.notify_all();
    if (sweeper_.joinable()) sweeper_.join();
}

bool HeadlessNode::Init(const Options& opts, std::string* err) {
    // Phase M-4 — build the NIXL backend FIRST so we can hand its
    // RegisterRegion to the pinned tier as its register_region
    // callback. Without this hookup slot.mr_key stays 0 and the
    // backend cannot ExportMr the slot pool for cross-process Pull.
    node::transport::BackendOptions bo2;
    bo2.name      = opts.nixl_backend;
    bo2.bind_host = opts.nixl_bind_host;
    bo2.bind_port = opts.nixl_bind_port;
    auto backend = node::transport::CreateBackend(bo2, err);
    if (!backend) return false;
    auto* backend_raw = backend.get();
    nixl_ = std::make_unique<node::transport::NixlWrapper>(std::move(backend));

    // Inject the backend's RegisterRegion into the pinned tier options
    // (caller-supplied callback wins if they already wired one).
    node::tier::TierManager::Options tier_opts = opts.tier;
    if (!tier_opts.pinned.register_region) {
        tier_opts.pinned.register_region =
            [backend_raw](void* base, uint64_t bytes) -> uint32_t {
                std::string err;
                return backend_raw->RegisterRegion(base, bytes, &err);
            };
    }
    // Phase G-1 — bridge DramTier evictions to ART-leaf pruning +
    // KV_EVENT_EVICT publication.
    if (!tier_opts.dram.on_evict) {
        tier_opts.dram.on_evict =
            [this](const node::tier::DramKey& k) { this->OnDramEvict(k); };
    }
    tm_ = node::tier::TierManager::Create(tier_opts, err);
    if (!tm_) return false;

    if (!opts.rocksdb_path.empty()) {
        node::meta::RocksdbStore::Options ro;
        ro.path = opts.rocksdb_path;
        rocks_ = node::meta::RocksdbStore::Open(ro, err);
        if (!rocks_) {
            // If rocksdb isn't compiled in, Open returns nullptr with err set.
            // Headless mode tolerates that — clear err and proceed without it.
            err->clear();
            rocks_.reset();
        }
    }

    node::ingest::MutableBufferPool::Options bo;
    bo.start_sweeper = true;
    buffers_ = std::make_unique<node::ingest::MutableBufferPool>(tm_.get(), bo);
    wm_      = std::make_unique<node::ingest::WatermarkTracker>();

    // Try the snapshot path first; fall back to a fresh ART on any failure.
    // The fall-back is intentional — a stale/corrupt snapshot must never
    // block boot, because RocksDB still holds the authoritative seal log
    // (LLD §7.3 crash recovery). The legacy RocksDB rebuild path can be
    // re-introduced here later as the second fall-back; for Phase D-1 the
    // contract is "fast restore if a clean snapshot is on disk, otherwise
    // start empty and let the warm-up window rebuild lazily".
    // ART durability — three modes:
    //   1. No persistence (both paths empty): fresh in-memory ART.
    //   2. Snapshot-only (Phase D-1): restore from snapshot at boot,
    //      no on-disk durability until the next WriteArtSnapshot().
    //   3. Snapshot + WAL (Phase D-2): ArtWal owns the ART; every
    //      Insert / Remove is appended-and-fsynced to art_wal_path
    //      before mutating the in-memory tree, and the WAL is
    //      replayed on top of the snapshot at Init.
    if (!opts.art_wal_path.empty()) {
        node::prefix::ArtWal::Options wo{};
        wo.snapshot_path     = opts.art_snapshot_path;
        wo.wal_path          = opts.art_wal_path;
        wo.fsync_each_write  = true;
        std::string wal_err;
        art_wal_ = node::prefix::ArtWal::Open(wo, &wal_err);
        if (art_wal_) {
            // Transfer ownership of the ART into our local handle so
            // Lookup / Reserve / Seal / etc. don't need to know about
            // the WAL. Mutators below funnel through art_wal_.
            art_ = nullptr;  // owned by art_wal_
        }
    } else if (!opts.art_snapshot_path.empty()) {
        struct ::stat st{};
        if (::stat(opts.art_snapshot_path.c_str(), &st) == 0) {
            std::string snap_err;
            auto restored = node::prefix::ArtSnapshot::Read(
                opts.art_snapshot_path, nullptr, &snap_err);
            if (restored) {
                art_ = std::move(restored);
            }
        }
    }
    if (!art_ && !art_wal_) art_ = std::make_unique<node::prefix::ArtIndex>();
    events_  = std::make_unique<node::prefix::EventStream>();
    // Phase G-2 — start the refcount-deferred eviction sweeper.
    sweeper_ = std::thread([this] { SweeperLoop(); });
    // The PriorityScheduler lives inside NixlWrapper (Phase E-2); the
    // backend was constructed above so the pinned tier could register
    // its pool with it.
    // Phase G-3 — seed pinned-tier gauges so scrapes before the first
    // Reserve still show meaningful capacity (slots_total constant,
    // slots_in_use = 0).
    BumpPinnedTierGauges(tm_.get());
    return true;
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

int HeadlessNode::Lookup(const char* /*tenant_id*/,
                         uint64_t tenant_hash,
                         uint64_t model_id_hash,
                         const uint32_t* tokens, std::size_t n,
                         kv_locator_t* out_meta,
                         kv_handle_t*  out_handle,
                         uint32_t*     out_matched_tokens) {
    auto span = kvcache::trace::Tracer::Get().StartSpan("kv.lookup");
    span.SetAttribute("kv.tokens_requested", static_cast<int64_t>(n));

    if (!tokens || n == 0 || !out_meta || !out_handle || !out_matched_tokens) {
        span.SetError("invalid argument");
        return KV_E_INVAL;
    }
    // ART is owned either by art_wal_ (Phase D-2) or art_ directly
    // (Phase D-1 / no persistence). Pick whichever is live.
    node::prefix::ArtIndex& art_ref = art_wal_ ? art_wal_->art() : *art_;
    auto g = art_ref.EnterRead();
    // Phase Q-5 — namespace-scoped LPM. Tokens alone are no longer the
    // chunk_path key; the (tenant_hash, model_hash) namespace is the
    // ART subtree this caller is allowed to see.
    auto r = node::prefix::LongestPrefixMatchNS(
        art_ref, {tokens, n}, tenant_hash, model_id_hash, g);
    if (r.matched_tokens == 0 || !r.leaf) {
        span.SetAttribute("kv.hit", false);
        return KV_E_NOT_FOUND;
    }
    span.SetAttribute("kv.hit", true);
    span.SetAttribute("kv.matched_tokens", static_cast<int64_t>(r.matched_tokens));

    // Acquire a hold on the leaf for the caller. Race-safe: if the evictor
    // got there first refcount is zero and we miss.
    if (!r.leaf->refcount.TryAcquireIfNonZero()) return KV_E_NOT_FOUND;

    *out_meta = r.leaf->locator;
    *out_matched_tokens = r.matched_tokens;
    kv_handle_t h = next_handle_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lk(mu_);
        // Phase Q-5 — record the namespace so subsequent Fetch
        // / Release on this handle stay scoped (and a future audit
        // hook can read it without re-deriving).
        handles_[h] = HandleState{HandleKind::kRead, r.leaf->locator,
                                   0, r.leaf, tenant_hash, model_id_hash};
    }
    *out_handle = h;
    return KV_OK;
}

// ---------------------------------------------------------------------------
// Reserve
// ---------------------------------------------------------------------------

int HeadlessNode::Reserve(const kv_locator_t* locator, std::size_t bytes,
                          uint64_t tenant_hash, uint64_t model_hash,
                          kv_handle_t* out_handle, kv_buffer_desc_t* out_slot) {
    if (!locator || bytes == 0 || !out_handle || !out_slot) {
        Rm().invalid_total->Inc(1.0, {});
        return KV_E_INVAL;
    }
    auto ih = buffers_->Reserve();
    if (ih == node::ingest::kInvalidIngestHandle) {
        // Phase G-3 — pool exhausted is the canonical backpressure
        // signal. Counter increment + a fresh gauge snapshot so the
        // saturation is visible to /metrics callers immediately.
        Rm().nomem_total->Inc(1.0, {});
        BumpPinnedTierGauges(tm_.get());
        return KV_E_NOMEM;
    }
    wm_->Track(ih);
    auto slot = buffers_->GetSlot(ih);
    if (!slot) { buffers_->Release(ih); return KV_E_INTERNAL; }
    if (slot->bytes < bytes) {
        buffers_->Release(ih);
        wm_->Drop(ih);
        Rm().nomem_total->Inc(1.0, {});
        BumpPinnedTierGauges(tm_.get());
        return KV_E_NOMEM;
    }
    out_slot->addr     = slot->addr;
    out_slot->len      = bytes;
    out_slot->mem_type = KV_MEM_HOST_PINNED;
    out_slot->mr_key   = slot->mr_key;

    kv_handle_t h = next_handle_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lk(mu_);
        // Phase Q-5 — record (tenant_hash, model_hash) so the matching
        // Seal reproduces the namespace fingerprint we used to insert.
        handles_[h] = HandleState{HandleKind::kIngest, *locator, ih,
                                   nullptr, tenant_hash, model_hash};
    }
    *out_handle = h;
    Rm().reserves_total->Inc(1.0, {});
    BumpPinnedTierGauges(tm_.get());
    return KV_OK;
}

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

int HeadlessNode::Publish(kv_handle_t handle, kv_buffer_desc_t /*src*/,
                          uint64_t watermark) {
    HandleState st;
    {
        std::lock_guard lk(mu_);
        auto it = handles_.find(handle);
        if (it == handles_.end()) return KV_E_INVAL;
        st = it->second;
    }
    if (st.kind != HandleKind::kIngest) return KV_E_INVAL;
    wm_->Publish(st.ingest_handle, watermark);
    return KV_OK;
}

// ---------------------------------------------------------------------------
// Fetch / Wait
// ---------------------------------------------------------------------------

int HeadlessNode::Fetch(kv_handle_t handle, uint64_t tenant_hash,
                        const kv_range_t* ranges, std::size_t n_ranges,
                        kv_buffer_desc_t dst,
                        kv_completion_t* out_completion) {
    // Backward-compat thin wrapper — default class is P1 (engine data path).
    return FetchWithPriority(
        handle, tenant_hash, ranges, n_ranges, dst,
        static_cast<int>(node::transport::Priority::P1), out_completion);
}

int HeadlessNode::FetchWithPriority(kv_handle_t handle, uint64_t tenant_hash,
                        const kv_range_t* /*ranges*/, std::size_t /*n_ranges*/,
                        kv_buffer_desc_t dst,
                        int priority,
                        kv_completion_t* out_completion) {
    // Map the integer hop through to the scheduler enum. Unknown
    // values fall back to P1 so the C ABI can never accidentally
    // submit work as P0 just because of an enum drift.
    node::transport::Priority prio = node::transport::Priority::P1;
    switch (priority) {
        case static_cast<int>(node::transport::Priority::P0):
            prio = node::transport::Priority::P0; break;
        case static_cast<int>(node::transport::Priority::P1):
            prio = node::transport::Priority::P1; break;
        case static_cast<int>(node::transport::Priority::P2):
            prio = node::transport::Priority::P2; break;
        default: prio = node::transport::Priority::P1;
    }
    auto span = kvcache::trace::Tracer::Get().StartSpan("kv.fetch");
    span.SetAttribute("kv.tenant_hash", static_cast<int64_t>(tenant_hash));
    span.SetAttribute("kv.dst_bytes",   static_cast<int64_t>(dst.len));
    span.SetAttribute("kv.priority",    static_cast<int64_t>(priority));

    HandleState st;
    {
        std::lock_guard lk(mu_);
        auto it = handles_.find(handle);
        if (it == handles_.end()) {
            span.SetError("unknown handle");
            return KV_E_INVAL;
        }
        st = it->second;
    }
    if (st.kind != HandleKind::kRead || !st.leaf) {
        span.SetError("not a read handle");
        return KV_E_INVAL;
    }

    // Resolve the bytes via the unified tier Fetch path.
    auto key = LocatorContentKey(st.locator);
    std::string err;
    auto f = tm_->Fetch(key, &err);
    if (f.hit == node::tier::TierManager::FetchHit::kMiss) {
        return KV_E_NOT_FOUND;
    }
    // Loopback NIXL transfer into the caller's dst buffer.
    if (!dst.addr || dst.len < f.data.size()) return KV_E_NOMEM;
    auto src_mr = nixl_->Register(f.data.data(), f.data.size(), &err);
    if (src_mr == node::transport::kInvalidMrKey) return KV_E_TRANSPORT;

    // Phase M-5 — honour a pre-registered destination key. When
    // engines register their fetch buffer once at startup (via
    // kv_register_local_mr) — or when the gRPC handler has imported
    // a remote MR descriptor — `dst.mr_key` is already a key the
    // backend recognises. Re-registering would either fail outright
    // (imported-remote case) or churn keys for no reason
    // (long-lived-local case). We use the supplied key directly and
    // do not unregister it: ownership stays with the caller.
    const bool reuse_dst_mr = (dst.mr_key != 0);
    node::transport::MrKey dst_mr = reuse_dst_mr
        ? static_cast<node::transport::MrKey>(dst.mr_key)
        : nixl_->Register(dst.addr, dst.len, &err);
    if (dst_mr == node::transport::kInvalidMrKey) {
        nixl_->Unregister(src_mr);
        return KV_E_TRANSPORT;
    }

    // Phase M-6 — dispatch Pull vs Push based on whether the dst MR
    // is local (engine in this process, share-everything case) or
    // imported-remote (the gRPC handler just minted it from an
    // incoming dst_remote_mr_descriptor). The remote path goes over
    // the wire via TcpBackend::Push and lands the bytes in the
    // peer's pre-registered MR.
    auto* be = nixl_->backend();
    const bool dst_is_remote = be && be->IsRemote(dst_mr);
    bool xfer_ok = false;
    if (dst_is_remote) {
        // Phase M-7 — Push goes through the same scheduler as Pull so
        // per-tenant QoS reservations and round-robin admission apply
        // uniformly across the data plane.
        node::transport::PushRequest preq{src_mr, 0, dst_mr, 0,
                                            f.data.size()};
        xfer_ok = nixl_->ScheduledPush(preq, prio,
                                        tenant_hash, 5000, &err);
    } else {
        node::transport::PullRequest req{dst_mr, 0, src_mr, 0, f.data.size()};
        // Tenant hash routes each caller into its own per-tenant FIFO
        // inside the class — so two tenants' Fetches interleave under
        // round-robin instead of one tenant's burst monopolising the
        // link. `prio` picks the class itself (Phase S-3).
        xfer_ok = nixl_->ScheduledPull(req, prio,
                                        tenant_hash, 5000, &err);
    }
    if (!xfer_ok) {
        nixl_->Unregister(src_mr);
        if (!reuse_dst_mr) nixl_->Unregister(dst_mr);
        return KV_E_TRANSPORT;
    }
    nixl_->Unregister(src_mr);
    if (!reuse_dst_mr) nixl_->Unregister(dst_mr);

    *out_completion = handle;  // synchronous in loopback; reuse handle as id
    return KV_OK;
}

int HeadlessNode::Wait(kv_completion_t /*cid*/, uint32_t /*timeout_ms*/) {
    return KV_OK;  // Loopback completes synchronously inside Fetch.
}

// ---------------------------------------------------------------------------
// Seal
// ---------------------------------------------------------------------------

int HeadlessNode::Seal(kv_handle_t handle,
                       const uint32_t* tokens, std::size_t n_tokens) {
    HandleState st;
    {
        std::lock_guard lk(mu_);
        auto it = handles_.find(handle);
        if (it == handles_.end()) return KV_E_INVAL;
        st = it->second;
    }
    if (st.kind != HandleKind::kIngest) return KV_E_INVAL;
    if (n_tokens < node::prefix::kChunkTokens) {
        // Nothing to seal (smaller than one chunk).
        wm_->Drop(st.ingest_handle);
        buffers_->Release(st.ingest_handle);
        std::lock_guard lk(mu_);
        handles_.erase(handle);
        return KV_E_INVAL;
    }

    // Phase Q-5 — same namespace prefix the matching Lookup will use.
    const auto ns = node::prefix::NamespaceFingerprint(
        st.tenant_hash, st.model_hash);
    auto chunk_path = node::prefix::ChunkifyNS({tokens, n_tokens}, ns);

    // Stage the slot bytes into DRAM so the next Lookup→Fetch can serve from
    // T2. We don't have a structured KV layout in MVP — the bytes are just
    // the engine's opaque payload up to the watermark.
    const uint64_t watermark = wm_->Read(st.ingest_handle);
    auto slot = buffers_->GetSlot(st.ingest_handle);
    if (slot && watermark > 0) {
        auto key = LocatorContentKey(st.locator);
        tm_->StageToDram(key, static_cast<const uint8_t*>(slot->addr),
                         static_cast<std::size_t>(watermark));
    }

    node::ingest::SealCommitter::Deps deps{
        rocks_.get(), art_.get(), events_.get(), buffers_.get(), wm_.get()};
    node::ingest::SealCommitter committer(deps);

    node::ingest::SealCommitter::Request req{};
    req.handle                 = st.ingest_handle;
    req.locator                = st.locator;
    req.chunk_path             = chunk_path;
    req.tier_residency_bitmap  = 1u << 3;  // DRAM bit

    // Phase G-1 — remember the (DramKey -> chunk_path) mapping so the
    // tier-side evictor can prune the matching ART leaf when these
    // bytes get evicted. Recorded eagerly under our own lock; both
    // headless branches below feed the same map.
    {
        std::lock_guard lk(mu_evict_);
        evict_index_[LocatorContentKey(st.locator)] = chunk_path;
    }

    // The SealCommitter requires RocksDB; in headless mode we may not have
    // it, so wire a no-op short-circuit when rocks_ is null.
    if (!rocks_) {
        // Mirror the seal logic minus RocksDB: ART insert + event publish +
        // book-keeping cleanup.
        auto leaf = std::make_unique<node::prefix::LeafData>();
        leaf->locator               = st.locator;
        leaf->tier_residency_bitmap = req.tier_residency_bitmap;
        leaf->bytes_total           = watermark;
        leaf->refcount.Acquire();
        std::span<const node::prefix::ChunkHash> chunk_path{
            req.chunk_path.data(), req.chunk_path.size()};
        auto ins = art_wal_
            ? art_wal_->Insert(chunk_path, std::move(leaf))
            : art_->Insert(chunk_path, std::move(leaf));
        if (ins == node::prefix::ArtIndex::InsertResult::kPathConflict) {
            return KV_E_INTERNAL;
        }
        node::prefix::Event ev{};
        ev.type    = node::prefix::EventType::Add;
        ev.tier    = node::prefix::Tier::Dram;
        ev.locator = st.locator;
        events_->Publish(ev);
        wm_->Drop(st.ingest_handle);
        buffers_->Release(st.ingest_handle);
    } else {
        auto r = committer.Commit(req);
        if (!r.ok) return KV_E_INTERNAL;
    }
    std::lock_guard lk(mu_);
    handles_.erase(handle);
    return KV_OK;
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

int HeadlessNode::Release(kv_handle_t handle) {
    HandleState st;
    {
        std::lock_guard lk(mu_);
        auto it = handles_.find(handle);
        if (it == handles_.end()) return KV_E_INVAL;
        st = it->second;
        handles_.erase(it);
    }
    if (st.kind == HandleKind::kIngest && st.ingest_handle) {
        wm_->Drop(st.ingest_handle);
        buffers_->Release(st.ingest_handle);
        // Phase G-3 — slot returned to the pool, refresh the gauge
        // so a Prometheus scrape that arrives between Release and
        // the next Reserve sees the freed capacity.
        BumpPinnedTierGauges(tm_.get());
    } else if (st.kind == HandleKind::kRead && st.leaf) {
        st.leaf->refcount.Release();
        // Phase G-2 — releasing a reader may unblock a deferred
        // eviction; nudge the sweeper out of its wait_for tick.
        defer_cv_.notify_one();
    }
    return KV_OK;
}

bool HeadlessNode::WriteArtSnapshot(const std::string& path, std::string* err) {
    // Under Phase D-2 (WAL active) Checkpoint() writes the snapshot
    // AND truncates the WAL atomically — that's the durable next
    // checkpoint. Without WAL we fall back to the D-1 best-effort
    // snapshot path (callers lose any unflushed mutations).
    if (art_wal_) {
        return art_wal_->Checkpoint(path, err);
    }
    if (!art_) {
        if (err) *err = "ART not initialised";
        return false;
    }
    return node::prefix::ArtSnapshot::Write(*art_, path, nullptr, err);
}

// ---------------------------------------------------------------------------
// Event subscription (Phase M-2)
// ---------------------------------------------------------------------------

HeadlessNode::SubscriptionId
HeadlessNode::SubscribeEvents(EventCallback cb, void* user) {
    if (!events_ || !cb) return 0;
    auto sub = std::make_unique<EventSub>();
    sub->cb         = cb;
    sub->user       = user;
    sub->ring_handle = events_->Subscribe();

    const SubscriptionId id =
        next_sub_.fetch_add(1, std::memory_order_relaxed);
    auto* raw = sub.get();
    raw->poller = std::thread([this, raw] {
        // Drain loop: poll the subscriber's ring, invoke cb on each
        // event, sleep briefly when empty. Stops when `stop` is set.
        while (!raw->stop.load(std::memory_order_acquire)) {
            node::prefix::Event ev{};
            if (events_->Poll(raw->ring_handle, &ev)) {
                kv_event_t out{};
                out.type    = static_cast<int32_t>(ev.type);
                out.tier    = static_cast<int32_t>(ev.tier);
                out.locator = ev.locator;
                out.epoch   = ev.epoch;
                raw->cb(&out, raw->user);
            } else {
                // Small backoff so an idle subscription doesn't spin
                // a whole core. Production tuning: switch to a real
                // condvar wake-up on Publish.
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }
    });

    std::lock_guard<std::mutex> lk(sub_mu_);
    subs_.emplace(id, std::move(sub));
    return id;
}

void HeadlessNode::UnsubscribeEvents(SubscriptionId id) {
    std::unique_ptr<EventSub> taken;
    {
        std::lock_guard<std::mutex> lk(sub_mu_);
        auto it = subs_.find(id);
        if (it == subs_.end()) return;
        taken = std::move(it->second);
        subs_.erase(it);
    }
    taken->stop.store(true, std::memory_order_release);
    if (taken->poller.joinable()) taken->poller.join();
    if (events_) events_->Unsubscribe(taken->ring_handle);
}

// ---------------------------------------------------------------------------
// DRAM-eviction bridge (Phase G-1 → G-2)
// ---------------------------------------------------------------------------
//
// G-1 wired DramTier evictions to ART pruning. G-2 makes the pruning
// refcount-aware: if a leaf has live Lookup-acquired holders
// (refcount > 1), we cannot Remove from ART without leaving dangling
// kv_handle_ts behind — so the path is queued and a background
// sweeper retries until TryEvict succeeds (or the leaf is replaced).
//
// Fires synchronously inside DramTier::EvictToFit (DramTier's mutex
// is held). Constraint: never touch tm_ from here.
void HeadlessNode::OnDramEvict(const node::tier::DramKey& key) {
    std::vector<node::prefix::ChunkHash> path;
    {
        std::lock_guard lk(mu_evict_);
        auto it = evict_index_.find(key);
        if (it == evict_index_.end()) return;
        path = std::move(it->second);
        evict_index_.erase(it);
    }
    if (path.empty()) return;

    if (TryEvictNow({path.data(), path.size()})) return;

    // Couldn't claim — there are outstanding holders. Queue for the
    // sweeper; record the current leaf pointer + locator so the
    // sweeper can recognise "leaf replaced" vs "still the same leaf,
    // still has holders".
    node::prefix::ArtIndex& art_ref = art_wal_ ? art_wal_->art() : *art_;
    auto g = art_ref.EnterRead();
    auto* leaf = art_ref.LookupByPath({path.data(), path.size()}, g);
    if (!leaf) return;  // already gone by some other path

    DeferredEvict entry;
    entry.path    = std::move(path);
    entry.leaf    = leaf;
    entry.locator = leaf->locator;
    {
        std::lock_guard lk(mu_defer_);
        deferred_evicts_.push_back(std::move(entry));
    }
    defer_cv_.notify_one();
}

// Atomic claim + Remove + EVICT publish for a single path. Returns
// true on success (leaf was at refcount 1 and we evicted it), false
// if the path no longer maps to a leaf or the leaf has live holders.
bool HeadlessNode::TryEvictNow(
    std::span<const node::prefix::ChunkHash> path) {
    node::prefix::ArtIndex& art_ref = art_wal_ ? art_wal_->art() : *art_;
    node::prefix::LeafData* leaf = nullptr;
    kv_locator_t loc{};
    {
        auto g = art_ref.EnterRead();
        leaf = art_ref.LookupByPath(path, g);
        if (!leaf) return false;
        loc = leaf->locator;
    }
    // Atomic claim: only proceed if no Lookup-acquired holders.
    if (!leaf->refcount.TryEvict()) return false;

    // We now own the right to remove. From this point on a racing
    // Lookup that wants this leaf will see refcount == 0 and bail
    // via TryAcquireIfNonZero.
    bool removed = art_wal_
        ? art_wal_->Remove(path)
        : art_->Remove(path);
    if (!removed) return false;

    if (events_) {
        node::prefix::Event ev{};
        ev.type    = node::prefix::EventType::Evict;
        ev.tier    = node::prefix::Tier::Dram;
        ev.locator = loc;
        events_->Publish(ev);
    }
    return true;
}

// Sweeper: every ~50ms (or sooner on a Release-driven notify), walk
// the deferred queue and retry each entry's TryEvictNow. Drop entries
// whose leaf was already replaced.
void HeadlessNode::SweeperLoop() {
    constexpr auto kTick = std::chrono::milliseconds(50);
    while (!sweeper_stop_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lk(mu_defer_);
            defer_cv_.wait_for(lk, kTick, [&] {
                return sweeper_stop_.load(std::memory_order_acquire) ||
                       !deferred_evicts_.empty();
            });
        }
        if (sweeper_stop_.load(std::memory_order_acquire)) break;

        // Move the current queue out so we don't hold mu_defer_ across
        // ART ops. Anything we can't evict this pass gets re-queued.
        std::vector<DeferredEvict> work;
        {
            std::lock_guard lk(mu_defer_);
            work.swap(deferred_evicts_);
        }
        if (work.empty()) continue;

        std::vector<DeferredEvict> still_pending;
        still_pending.reserve(work.size());
        node::prefix::ArtIndex& art_ref =
            art_wal_ ? art_wal_->art() : *art_;
        for (auto& e : work) {
            // Check identity: if the leaf at path is no longer the one
            // we deferred, a fresh Seal replaced it — drop entry.
            bool drop_replaced = false;
            {
                auto g = art_ref.EnterRead();
                auto* cur = art_ref.LookupByPath(
                    {e.path.data(), e.path.size()}, g);
                if (cur != e.leaf) drop_replaced = true;
            }
            if (drop_replaced) continue;

            if (!TryEvictNow({e.path.data(), e.path.size()})) {
                still_pending.push_back(std::move(e));
            }
        }
        if (!still_pending.empty()) {
            std::lock_guard lk(mu_defer_);
            for (auto& e : still_pending) {
                deferred_evicts_.push_back(std::move(e));
            }
        }
    }
}

}  // namespace kvcache::abi
