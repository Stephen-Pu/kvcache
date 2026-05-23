// HeadlessNode — in-process backend for the Core ABI.
#include "headless_node.h"

#include <sys/stat.h>

#include <cstring>

#include "kvcache/kv_errors.h"
#include "prefix/art_snapshot.h"
#include "prefix/art_wal.h"
#include "trace.h"

namespace kvcache::abi {

namespace {

HeadlessNode*       g_singleton = nullptr;

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
    // The PriorityScheduler lives inside NixlWrapper (Phase E-2); the
    // backend was constructed above so the pinned tier could register
    // its pool with it.
    return true;
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

int HeadlessNode::Lookup(const char* /*tenant_id*/, uint64_t /*model_id_hash*/,
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
    auto r = node::prefix::LongestPrefixMatch(art_ref, {tokens, n}, g);
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
        handles_[h] = HandleState{HandleKind::kRead, r.leaf->locator, 0, r.leaf};
    }
    *out_handle = h;
    return KV_OK;
}

// ---------------------------------------------------------------------------
// Reserve
// ---------------------------------------------------------------------------

int HeadlessNode::Reserve(const kv_locator_t* locator, std::size_t bytes,
                          kv_handle_t* out_handle, kv_buffer_desc_t* out_slot) {
    if (!locator || bytes == 0 || !out_handle || !out_slot) return KV_E_INVAL;
    auto ih = buffers_->Reserve();
    if (ih == node::ingest::kInvalidIngestHandle) return KV_E_NOMEM;
    wm_->Track(ih);
    auto slot = buffers_->GetSlot(ih);
    if (!slot) { buffers_->Release(ih); return KV_E_INTERNAL; }
    if (slot->bytes < bytes) {
        buffers_->Release(ih);
        wm_->Drop(ih);
        return KV_E_NOMEM;
    }
    out_slot->addr     = slot->addr;
    out_slot->len      = bytes;
    out_slot->mem_type = KV_MEM_HOST_PINNED;
    out_slot->mr_key   = slot->mr_key;

    kv_handle_t h = next_handle_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lk(mu_);
        handles_[h] = HandleState{HandleKind::kIngest, *locator, ih, nullptr};
    }
    *out_handle = h;
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
                        const kv_range_t* /*ranges*/, std::size_t /*n_ranges*/,
                        kv_buffer_desc_t dst,
                        kv_completion_t* out_completion) {
    auto span = kvcache::trace::Tracer::Get().StartSpan("kv.fetch");
    span.SetAttribute("kv.tenant_hash", static_cast<int64_t>(tenant_hash));
    span.SetAttribute("kv.dst_bytes",   static_cast<int64_t>(dst.len));

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

    node::transport::PullRequest req{dst_mr, 0, src_mr, 0, f.data.size()};
    // P1 is the default class for ordinary Fetch. The tenant hash routes
    // each caller into its own per-tenant FIFO inside the class — so two
    // tenants' Fetches interleave under round-robin instead of one
    // tenant's burst monopolising the link.
    if (!nixl_->ScheduledPull(req, node::transport::Priority::P1,
                                tenant_hash, 5000, &err)) {
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

    auto chunk_path = node::prefix::Chunkify({tokens, n_tokens});

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
    } else if (st.kind == HandleKind::kRead && st.leaf) {
        st.leaf->refcount.Release();
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

}  // namespace kvcache::abi
