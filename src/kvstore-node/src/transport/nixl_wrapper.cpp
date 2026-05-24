// LLD §3.5 — NIXL transport facade, plus a loopback backend.
//
// The loopback backend is sufficient for:
//   * Unit tests (no RDMA / GPU dependency).
//   * Single-process integration tests.
//   * Bring-up on dev laptops.
//
// Real cross-process / cross-node transfers go through `TcpBackend` (see
// transport/tcp_backend.{h,cpp}). RDMA backends (UCX, GDR, GDS, NVLink)
// plug in behind the same INixlBackend interface once hardware is wired.
#include "transport/nixl_wrapper.h"

#include "trace.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <unordered_map>

#include "transport/tcp_backend.h"

namespace kvcache::node::transport {

// ---------------------------------------------------------------------------
// LoopbackBackend — intra-process memcpy backend.
// ---------------------------------------------------------------------------

class LoopbackBackend final : public INixlBackend {
   public:
    std::string Name() const override { return "loopback"; }

    MrKey RegisterRegion(void* addr, std::size_t bytes, std::string* err) override {
        if (!addr || bytes == 0) {
            if (err) *err = "loopback: invalid region";
            return kInvalidMrKey;
        }
        std::lock_guard lk(mu_);
        const MrKey k = next_key_.fetch_add(1, std::memory_order_relaxed);
        regions_.emplace(k, Region{addr, bytes});
        return k;
    }

    void UnregisterRegion(MrKey key) override {
        std::lock_guard lk(mu_);
        regions_.erase(key);
    }

    bool ResolveRegion(MrKey key, void** addr, std::size_t* bytes) const override {
        std::lock_guard lk(mu_);
        auto it = regions_.find(key);
        if (it == regions_.end()) return false;
        if (addr)  *addr  = it->second.addr;
        if (bytes) *bytes = it->second.bytes;
        return true;
    }

    // Loopback is intra-process by definition: an "exported" descriptor is
    // really just a local-mr handle that the same process can re-import.
    // Useful mostly so tests of the abstract interface exercise both
    // methods without needing the TCP backend.
    bool ExportMr(MrKey local_key,
                  RemoteMrDescriptor* out_desc,
                  std::string* err) override {
        if (!out_desc) { if (err) *err = "loopback: out_desc is null"; return false; }
        std::lock_guard lk(mu_);
        auto it = regions_.find(local_key);
        if (it == regions_.end()) {
            if (err) *err = "loopback: unknown MR key";
            return false;
        }
        // Encode: 4-byte MrKey + 8-byte bytes (host-endian; intra-process).
        out_desc->opaque.resize(sizeof(MrKey) + sizeof(std::size_t));
        std::memcpy(out_desc->opaque.data(), &local_key, sizeof(MrKey));
        std::memcpy(out_desc->opaque.data() + sizeof(MrKey),
                    &it->second.bytes, sizeof(std::size_t));
        return true;
    }

    MrKey ImportRemoteMr(const RemoteMrDescriptor& desc, std::string* err) override {
        if (desc.opaque.size() != sizeof(MrKey) + sizeof(std::size_t)) {
            if (err) *err = "loopback: malformed descriptor";
            return kInvalidMrKey;
        }
        MrKey peer_key = 0;
        std::memcpy(&peer_key, desc.opaque.data(), sizeof(MrKey));
        std::lock_guard lk(mu_);
        // Within the same process the imported key IS the local key (since
        // both ends share `regions_`). For cross-process work use TcpBackend.
        if (regions_.find(peer_key) == regions_.end()) {
            if (err) *err = "loopback: imported MR not present locally";
            return kInvalidMrKey;
        }
        return peer_key;
    }

    CompletionId Pull(const PullRequest& req, std::string* err) override {
        void *src_addr = nullptr, *dst_addr = nullptr;
        std::size_t src_n = 0, dst_n = 0;
        {
            std::lock_guard lk(mu_);
            auto it_s = regions_.find(req.src_mr);
            auto it_d = regions_.find(req.dst_mr);
            if (it_s == regions_.end() || it_d == regions_.end()) {
                if (err) *err = "loopback: unknown MR key";
                return kInvalidCompletionId;
            }
            src_addr = it_s->second.addr; src_n = it_s->second.bytes;
            dst_addr = it_d->second.addr; dst_n = it_d->second.bytes;
        }
        if (req.src_off + req.bytes > src_n || req.dst_off + req.bytes > dst_n) {
            if (err) *err = "loopback: out-of-bounds Pull";
            return kInvalidCompletionId;
        }
        std::memcpy(static_cast<uint8_t*>(dst_addr) + req.dst_off,
                    static_cast<uint8_t*>(src_addr) + req.src_off,
                    req.bytes);
        return next_completion_.fetch_add(1, std::memory_order_relaxed);
    }

    bool Wait(CompletionId cid, uint32_t /*timeout_ms*/, std::string* err) override {
        if (cid == kInvalidCompletionId) {
            if (err) *err = "loopback: invalid completion id";
            return false;
        }
        return true;  // loopback Pulls are synchronous-on-issue.
    }

   private:
    struct Region { void* addr; std::size_t bytes; };
    mutable std::mutex mu_;
    std::unordered_map<MrKey, Region> regions_;
    std::atomic<MrKey>        next_key_{1};
    std::atomic<CompletionId> next_completion_{1};
};

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

std::unique_ptr<INixlBackend> CreateBackend(const BackendOptions& opts, std::string* err) {
    if (opts.name == "loopback") {
        return std::make_unique<LoopbackBackend>();
    }
    if (opts.name == "tcp") {
        return CreateTcpBackend(opts, err);
    }
    // Real RDMA backends plug in behind this same interface:
    //   "ucx"   — UCX over IB / RoCE (LLD §3.5, D-NET-1) — Phase C-2
    //   "gdr"   — GPUDirect RDMA — Phase C-2
    //   "gds"   — GPUDirect Storage (NVMe → GPU direct, LLD §3.3 T3) — Phase C-2
    //   "nvlink"— intra-host NVLink fabric — Phase C-2
    if (err) *err = "nixl: unknown backend '" + opts.name + "'";
    return nullptr;
}

// ---------------------------------------------------------------------------
// NixlWrapper
// ---------------------------------------------------------------------------

NixlWrapper::NixlWrapper(std::unique_ptr<INixlBackend> backend)
    : NixlWrapper(std::move(backend), PriorityScheduler::Options{}) {}

NixlWrapper::NixlWrapper(std::unique_ptr<INixlBackend> backend,
                          const PriorityScheduler::Options& sched_opts)
    : backend_(std::move(backend)),
      name_(backend_ ? backend_->Name() : std::string()),
      sched_(sched_opts) {
    dispatcher_ = std::thread([this] { DispatcherLoop(); });
}

NixlWrapper::~NixlWrapper() {
    stop_.store(true, std::memory_order_release);
    disp_cv_.notify_all();
    if (dispatcher_.joinable()) dispatcher_.join();
}

MrKey NixlWrapper::Register(void* addr, std::size_t bytes, std::string* err) {
    return backend_ ? backend_->RegisterRegion(addr, bytes, err) : kInvalidMrKey;
}
void NixlWrapper::Unregister(MrKey key) {
    if (backend_) backend_->UnregisterRegion(key);
}
bool NixlWrapper::PullSync(const PullRequest& req, uint32_t timeout_ms, std::string* err) {
    if (!backend_) { if (err) *err = "nixl: no backend"; return false; }
    auto cid = backend_->Pull(req, err);
    if (cid == kInvalidCompletionId) return false;
    return backend_->Wait(cid, timeout_ms, err);
}

bool NixlWrapper::ScheduledPull(const PullRequest& req, Priority prio,
                                  uint64_t tenant_hash, uint32_t timeout_ms,
                                  std::string* err) {
    auto span = kvcache::trace::Tracer::Get().StartSpan("nixl.scheduled_pull");
    span.SetAttribute("nixl.bytes",        static_cast<int64_t>(req.bytes));
    span.SetAttribute("nixl.tenant_hash",  static_cast<int64_t>(tenant_hash));
    span.SetAttribute("nixl.priority",     static_cast<int64_t>(prio));
    span.SetAttribute("nixl.backend",      name_);

    if (!backend_) {
        span.SetError("no backend");
        if (err) *err = "nixl: no backend";
        return false;
    }

    PendingXfer pp;
    pp.kind       = PendingXfer::Kind::kPull;
    pp.pull       = req;
    pp.timeout_ms = timeout_ms;
    // The scheduler stores `&pp` as the WorkItem user pointer; the
    // dispatcher casts back to drive the Pull / Push. PendingXfer is
    // owned by this stack frame and outlives the dispatcher call
    // because we wait on pp.cv before returning.
    sched_.Submit(prio, tenant_hash, req.bytes, &pp);

    // Wake the dispatcher.
    {
        std::lock_guard lk(disp_mu_);
        disp_cv_.notify_one();
    }

    std::unique_lock lk(pp.mu);
    pp.cv.wait(lk, [&] { return pp.done; });

    if (err) *err = pp.err;
    if (!pp.ok) span.SetError(pp.err);
    return pp.ok;
}

bool NixlWrapper::ScheduledPush(const PushRequest& req, Priority prio,
                                  uint64_t tenant_hash, uint32_t timeout_ms,
                                  std::string* err) {
    auto span = kvcache::trace::Tracer::Get().StartSpan("nixl.scheduled_push");
    span.SetAttribute("nixl.bytes",        static_cast<int64_t>(req.bytes));
    span.SetAttribute("nixl.tenant_hash",  static_cast<int64_t>(tenant_hash));
    span.SetAttribute("nixl.priority",     static_cast<int64_t>(prio));
    span.SetAttribute("nixl.backend",      name_);

    if (!backend_) {
        span.SetError("no backend");
        if (err) *err = "nixl: no backend";
        return false;
    }

    PendingXfer pp;
    pp.kind       = PendingXfer::Kind::kPush;
    pp.push       = req;
    pp.timeout_ms = timeout_ms;
    sched_.Submit(prio, tenant_hash, req.bytes, &pp);

    {
        std::lock_guard lk(disp_mu_);
        disp_cv_.notify_one();
    }

    std::unique_lock lk(pp.mu);
    pp.cv.wait(lk, [&] { return pp.done; });

    if (err) *err = pp.err;
    if (!pp.ok) span.SetError(pp.err);
    return pp.ok;
}

void NixlWrapper::DispatcherLoop() {
    while (true) {
        // Wait for either work to appear or shutdown.
        {
            std::unique_lock lk(disp_mu_);
            disp_cv_.wait(lk, [&] {
                return stop_.load(std::memory_order_acquire) ||
                       sched_.HasWork();
            });
            if (stop_.load(std::memory_order_acquire) && !sched_.HasWork()) {
                return;
            }
        }

        // Drain whatever the scheduler currently admits.
        while (auto w = sched_.TryNext()) {
            auto* pp = static_cast<PendingXfer*>(w->user);
            std::string local_err;
            CompletionId cid = kInvalidCompletionId;
            if (pp->kind == PendingXfer::Kind::kPull) {
                cid = backend_->Pull(pp->pull, &local_err);
            } else {
                cid = backend_->Push(pp->push, &local_err);
            }
            bool ok = false;
            if (cid != kInvalidCompletionId) {
                ok = backend_->Wait(cid, pp->timeout_ms, &local_err);
            }
            sched_.OnComplete(w->id);

            // Hand the result back to the caller.
            {
                std::lock_guard lk(pp->mu);
                pp->ok   = ok;
                pp->err  = std::move(local_err);
                pp->done = true;
            }
            pp->cv.notify_one();
        }
        // Loop: re-evaluate HasWork (a non-admissible class may still be
        // waiting on credit, in which case we'll sleep until OnComplete on
        // a sibling wakes us — see below). To make that wake-up reliable
        // we notify disp_cv_ from OnComplete via the wakeup hook? Simpler:
        // we re-check HasWork above and let the wait condition pick it up
        // on the next notify_one() from a Submit() or stop_.
    }
}

}  // namespace kvcache::node::transport
