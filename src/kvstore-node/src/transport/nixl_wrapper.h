// LLD §3.5 — NIXL transport facade.
//
// NIXL (NVIDIA's transfer library) is the data plane. It selects an actual
// backend at runtime — UCX over InfiniBand / RoCE, GPUDirect RDMA, NVMe-oF,
// NVLink, GDS, or plain TCP — based on link capabilities.
//
// Our policy (LLD invariants §4.4):
//   * Server-Pull only — the server initiates the DMA, never the client.
//     This is what makes priority scheduling possible: the scheduler sits
//     server-side and decides admission.
//   * Zero-copy on the local hot path — caller memory is registered once as
//     a memory region (MR) and reused across operations.
//
// Distributed model:
//   * Each backend exposes its local memory regions via `ExportMr`, which
//     returns an opaque `RemoteMrDescriptor`. The descriptor is serialised
//     to bytes for transmission (typically inside a gRPC Fetch request).
//   * The peer backend ingests the descriptor via `ImportRemoteMr`, getting
//     back a local MrKey that refers to the *remote* memory.
//   * `Pull` accepts a `src_mr` that may be either a local-registered MR or
//     an imported remote MR; the backend dispatches accordingly (memcpy
//     for local, network transfer for remote).
//
// We don't depend on the upstream NIXL headers in this codebase directly;
// instead we define an INixlBackend abstract and ship concrete backends
// (LoopbackBackend for intra-process tests, TcpBackend for real
// cross-process / cross-node transfers, future UcxBackend for RDMA).
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "transport/priority_scheduler.h"

namespace kvcache::node::transport {

using MrKey         = uint32_t;
using CompletionId  = uint64_t;

inline constexpr MrKey         kInvalidMrKey        = 0;
inline constexpr CompletionId  kInvalidCompletionId = 0;

// Opaque, backend-specific encoding of a memory region that lives on a
// peer process / node. Typical contents: host:port + backend's local MR id
// + bytes. Serialised by the producing backend, deserialised by the
// consuming backend; meaning is private to the backend kind.
struct RemoteMrDescriptor {
    // ASCII or binary; backends agree on the format. Empty = invalid.
    std::vector<uint8_t> opaque;
};

struct PullRequest {
    MrKey    dst_mr;
    uint64_t dst_off;
    MrKey    src_mr;
    uint64_t src_off;
    uint64_t bytes;
};

// Phase M-6 — server-push transfer. Mirror of PullRequest, semantically
// "the server has the bytes (src_mr is local) and wants to deposit them
// into a peer's pre-registered destination MR (dst_mr is imported-
// remote)". For loopback both ends are local and Push degenerates into
// the same memcpy as Pull.
struct PushRequest {
    MrKey    src_mr;
    uint64_t src_off;
    MrKey    dst_mr;
    uint64_t dst_off;
    uint64_t bytes;
};

class INixlBackend {
   public:
    virtual ~INixlBackend() = default;

    virtual std::string Name() const = 0;

    // ---- Local MR management ----

    // Register a host- or device-memory region with the transport. Returns a
    // non-zero key on success; sets `err` on failure.
    virtual MrKey RegisterRegion(void* addr, std::size_t bytes, std::string* err) = 0;

    virtual void  UnregisterRegion(MrKey key) = 0;

    // Resolve a MR key to (addr, bytes). Only meaningful for LOCAL MR keys
    // (those returned by RegisterRegion on this backend). For imported
    // remote MRs the address is not meaningful; callers must inspect
    // `bytes` only or use a backend-specific path. Returns false if unknown.
    virtual bool  ResolveRegion(MrKey key, void** addr, std::size_t* bytes) const = 0;

    // ---- Remote MR exchange (cross-process / cross-node) ----

    // Export a local MR as a descriptor that peer backends can use. Returns
    // false on unknown / non-exportable key. The descriptor is meant to be
    // shipped over the wire (e.g. inside a gRPC FetchRequest) to the peer
    // that will perform the Pull.
    virtual bool ExportMr(MrKey local_key,
                          RemoteMrDescriptor* out_desc,
                          std::string* err) = 0;

    // Import a peer's descriptor into this backend, returning a fresh local
    // MrKey usable as `src_mr` in a subsequent Pull. Returns kInvalidMrKey
    // on failure. The imported key has the same lifetime semantics as a
    // locally-registered MR — call `UnregisterRegion` to release it.
    virtual MrKey ImportRemoteMr(const RemoteMrDescriptor& desc,
                                 std::string* err) = 0;

    // ---- Transfer ----

    // Issue a server-initiated Pull. `src_mr` may be either a local-
    // registered MR or an imported remote MR; the backend dispatches.
    // Returns a completion id; caller calls Wait() to block. Some backends
    // (loopback, TCP synchronous mode) complete inline.
    virtual CompletionId Pull(const PullRequest& req, std::string* err) = 0;

    // Phase M-6 — server-push counterpart of Pull. `src_mr` MUST be
    // local; `dst_mr` MAY be local (intra-process — degenerates to
    // memcpy) or imported-remote (the backend connects to the peer's
    // listener and writes the bytes into the peer's MR). The default
    // implementation falls back to Pull semantics so non-network
    // backends keep working unchanged.
    virtual CompletionId Push(const PushRequest& req, std::string* err) {
        // Default: reuse the Pull path. For loopback this is exactly
        // the right behaviour (both ends are local). For network
        // backends Push is overridden to drive a PUT over the wire.
        PullRequest pr{req.dst_mr, req.dst_off, req.src_mr, req.src_off,
                        req.bytes};
        return Pull(pr, err);
    }

    // Phase M-6 — does this MrKey refer to an imported-remote MR
    // (versus a locally-registered region)? HeadlessNode::Fetch uses
    // this to dispatch Pull vs Push when the gRPC layer hands in a
    // dst_mr that was just minted from an incoming descriptor.
    virtual bool IsRemote(MrKey /*key*/) const { return false; }

    // Wait for a completion. Returns true if completed within `timeout_ms`,
    // false on timeout or unknown id.
    virtual bool Wait(CompletionId cid, uint32_t timeout_ms, std::string* err) = 0;
};

// ---------------------------------------------------------------------------
// Backend factory
// ---------------------------------------------------------------------------

struct BackendOptions {
    std::string name = "loopback";   // "loopback" | "tcp" | "ucx" | "gdr" | ...
    // Backend-specific options below. The factory ignores fields irrelevant
    // to the selected backend.
    std::string device;              // e.g. "mlx5_0" for UCX
    std::string bind_host = "127.0.0.1";  // TcpBackend listener host
    uint32_t    bind_port = 0;            // TcpBackend listener port (0 = OS-picked)
    uint32_t    listen_backlog = 32;      // TcpBackend SOMAXCONN cap
};

std::unique_ptr<INixlBackend> CreateBackend(const BackendOptions& opts, std::string* err);

// ---------------------------------------------------------------------------
// NixlWrapper — convenience around a backend pointer. Owns the registered MR
// table on behalf of higher layers so callers don't have to remember which
// keys belong to which backend.
// ---------------------------------------------------------------------------

class NixlWrapper {
   public:
    explicit NixlWrapper(std::unique_ptr<INixlBackend> backend);
    NixlWrapper(std::unique_ptr<INixlBackend> backend,
                const PriorityScheduler::Options& sched_opts);
    ~NixlWrapper();

    NixlWrapper(const NixlWrapper&)            = delete;
    NixlWrapper& operator=(const NixlWrapper&) = delete;

    INixlBackend* backend() noexcept { return backend_.get(); }
    const std::string& BackendName() const { return name_; }
    PriorityScheduler& scheduler() noexcept { return sched_; }

    MrKey Register(void* addr, std::size_t bytes, std::string* err);
    void  Unregister(MrKey key);

    // Direct path: bypass the scheduler. Kept for low-level callers / tests
    // that want to verify a backend's behavior without QoS interference.
    bool PullSync(const PullRequest& req, uint32_t timeout_ms, std::string* err);

    // Scheduled path (Phase E-2): goes through the in-process
    // PriorityScheduler so multi-tenant QoS reservations and per-tenant
    // round-robin actually take effect on the data plane. Synchronous —
    // the calling thread blocks until the dispatcher has run the Pull (or
    // the timeout has elapsed). For a single-threaded caller this looks
    // identical to PullSync; under contention, higher-priority callers
    // overtake lower-priority ones.
    bool ScheduledPull(const PullRequest& req, Priority prio,
                       uint64_t tenant_hash, uint32_t timeout_ms,
                       std::string* err);

   private:
    // Per-call state for ScheduledPull. The dispatcher thread fills in
    // `ok` / `err` and notifies the caller via `cv`.
    struct PendingPull {
        PullRequest          req;
        uint32_t             timeout_ms = 0;
        bool                 done       = false;
        bool                 ok         = false;
        std::string          err;
        std::mutex           mu;
        std::condition_variable cv;
    };

    void DispatcherLoop();

    std::unique_ptr<INixlBackend> backend_;
    std::string                   name_;

    PriorityScheduler             sched_;

    // Dispatcher thread + its wake-up cv. Submit notifies; the loop sleeps
    // when sched_ has no work and stop_ is false.
    std::mutex                    disp_mu_;
    std::condition_variable       disp_cv_;
    std::atomic<bool>             stop_{false};
    std::thread                   dispatcher_;
};

}  // namespace kvcache::node::transport
