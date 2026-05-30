// LLD §3.2 — Per-node, non-distributed refcount.
//
// Each ART leaf carries a refcount that is held by:
//   - In-flight Lookup → Fetch → Release cycles (caller-side hold).
//   - Streaming-write reservation (Reserve → Seal | TTL expiry).
//   - Tier migration (promotion / demotion holds while bytes are in motion).
//
// Eviction is blocked while refcount > 0. The counter is per-node only — the
// LLD explicitly rejects a distributed refcount (§3.2 "节点本地 refcount, 不分布式").
//
// Cluster-level visibility of "this chunk is in use somewhere" rides on the
// bloom-sketch view (§4.2), not on this counter.
#pragma once

#include <atomic>
#include <cstdint>

namespace kvcache::node::prefix {

class Refcount {
   public:
    Refcount() noexcept = default;
    explicit Refcount(uint32_t initial) noexcept : v_(initial) {}

    // Non-copyable; never want accidental sharing of the atomic.
    Refcount(const Refcount&)            = delete;
    Refcount& operator=(const Refcount&) = delete;

    // Returns the new value after the increment.
    uint32_t Acquire() noexcept {
        return v_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    // Returns the new value after the decrement. Asserts in debug builds that
    // the caller is not under-flowing.
    uint32_t Release() noexcept {
        uint32_t prev = v_.fetch_sub(1, std::memory_order_acq_rel);
        // Phase O-2: surface refcount under-flow through the logging
        // facade. Under-flow means a missing Acquire/Release pairing
        // somewhere — we don't abort (the atomic has already wrapped, the
        // damage is done) but we do shout about it so the operator can
        // grep their logs and the test suite's UBSan / TSan runs notice.
        // ReportUnderflow is out-of-line in refcount.cpp so refcount.h
        // doesn't have to drag logging.h into every TU that holds a leaf.
        if (prev == 0) [[unlikely]] {
            ReportUnderflow(this);
        }
        return prev - 1;
    }

    // Try to increment iff the current value is non-zero. Used by lookup paths
    // that must not resurrect a leaf the evictor has already claimed.
    bool TryAcquireIfNonZero() noexcept {
        uint32_t cur = v_.load(std::memory_order_acquire);
        while (cur != 0) {
            if (v_.compare_exchange_weak(cur, cur + 1,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    uint32_t Load() const noexcept {
        return v_.load(std::memory_order_acquire);
    }

    // Used by snapshot restore: blow away the current count and replace
    // it with `v`. Not safe to call while other threads hold references —
    // restore runs against a freshly-constructed leaf that no caller has
    // observed yet.
    void Reset(uint32_t v) noexcept {
        v_.store(v, std::memory_order_release);
    }

    bool IsZero() const noexcept { return Load() == 0; }

    // Phase G-2 — eviction claim. Atomically CAS 1 -> 0, succeeding
    // only when the only outstanding reference is the ART-owned baseline
    // installed at Seal. If a Lookup races and bumps the count to 2+
    // between an external check and the CAS, this primitive fails and
    // the evictor defers — the leaf stays reachable until the holders
    // Release. Mirror of TryAcquireIfNonZero on the producer side.
    bool TryEvict() noexcept {
        uint32_t expected = 1;
        return v_.compare_exchange_strong(expected, 0,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire);
    }

   private:
    // Out-of-line so the only TU that has to include logging.h is
    // refcount.cpp — the hot path stays header-only and zero-include.
    static void ReportUnderflow(const Refcount* self) noexcept;

    std::atomic<uint32_t> v_{0};
};

}  // namespace kvcache::node::prefix
