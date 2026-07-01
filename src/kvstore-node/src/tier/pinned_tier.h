// LLD §3.3 T1 — Pinned host memory tier.
//
// Backs the streaming-write mutable buffer pool and the staging area used by
// T2 ↔ NIXL transfers. Memory is page-locked so that NIXL (RDMA, GDR, GDS)
// can DMA directly without bounce buffers.
//
// MVP: a fixed-slot slab allocator. One pool with N slots, each of equal
// `slot_bytes`. The slot size should be chosen at deployment time to roughly
// match the typical streaming chunk size (default 16 MiB). A variable-size
// arena allocator with buddy lists is a Phase-2 concern.
//
// Memory acquisition strategy:
//   * KVCACHE_ENABLE_CUDA off (default): mmap + mlock the pool.
//   * KVCACHE_ENABLE_CUDA on            : cudaHostAlloc with PORTABLE | MAPPED
//     so the same memory is also pinnable by RDMA verbs registration.
//     (Implementation under KVCACHE_HAVE_CUDA — TODO(stephen).)
//
// NIXL memory-region registration is exposed via the `mr_key` field on each
// slot descriptor. The actual registration call is a callback supplied at
// pool construction (kept abstract here so the tier doesn't depend on NIXL
// headers directly).
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace kvcache::node::tier {

// Identifier handed out by PinnedTier::Acquire and returned to Release. Opaque
// 32-bit value; the high bit is reserved for "invalid".
struct SlotId {
    uint32_t v = 0xFFFFFFFFu;
    bool valid() const noexcept { return v != 0xFFFFFFFFu; }
    static SlotId Invalid() noexcept { return {}; }
};

// One slot — pointer + size + NIXL key.
struct SlotDesc {
    void*    addr     = nullptr;
    uint64_t bytes    = 0;
    uint32_t mr_key   = 0;     // NIXL memory key; 0 if unregistered
    SlotId   id;
};

class PinnedTier {
   public:
    // Optional callback used to register the whole pool with NIXL. If null,
    // mr_key on all slots will be 0 (transport will fall back to local copies).
    using RegisterRegionFn = std::function<uint32_t(void* base, uint64_t bytes)>;

    struct Options {
        uint64_t  pool_bytes  = 1ull << 30;   // 1 GiB default
        uint64_t  slot_bytes  = 16ull << 20;  // 16 MiB default
        bool      use_mlock   = true;
        // Caller-supplied NIXL MR registration. Optional.
        RegisterRegionFn register_region;
    };

    static std::unique_ptr<PinnedTier> Create(const Options& opts, std::string* err);

    ~PinnedTier();
    PinnedTier(const PinnedTier&)            = delete;
    PinnedTier& operator=(const PinnedTier&) = delete;

    // Acquire a free slot. Returns nullopt if the pool is exhausted.
    std::optional<SlotDesc> Acquire();

    // Release a previously-acquired slot back to the pool.
    void Release(SlotId id);

    // Stats — meant for /metrics; not for hot-path use.
    uint64_t Capacity()  const noexcept { return pool_bytes_; }
    uint64_t SlotBytes() const noexcept { return slot_bytes_; }
    uint32_t SlotCount() const noexcept { return slot_count_; }
    uint32_t SlotsInUse() const noexcept {
        return in_use_.load(std::memory_order_relaxed);
    }

   private:
    PinnedTier() = default;

    void*    base_       = nullptr;
    uint64_t pool_bytes_ = 0;
    uint64_t slot_bytes_ = 0;
    uint32_t slot_count_ = 0;
    uint32_t mr_key_     = 0;
    bool     mlocked_    = false;
    // Phase A2 — true when the pool was allocated via cudaHostAlloc (built with
    // -DKVCACHE_ENABLE_CUDA=ON); selects cudaFreeHost over munmap at teardown.
    bool     cuda_alloc_ = false;

    // Free-list is a vector used as a stack; protected by mu_. Allocation is
    // O(1). The handful of mutex acquisitions per chunk is well within the
    // hot-path budget (mutable-buffer allocation is at most once per seal).
    std::mutex             mu_;
    std::vector<uint32_t>  free_stack_;
    std::atomic<uint32_t>  in_use_{0};
};

}  // namespace kvcache::node::tier
