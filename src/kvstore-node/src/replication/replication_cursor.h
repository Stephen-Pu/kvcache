// Task 4 — A9 DR warm-standby: ReplicationCursor.
//
// Header-only monotonic epoch cursor: tracks the last-applied epoch on the
// standby so it can dedup/resume replication on reconnect. ShouldApply(epoch)
// returns true iff epoch > last_; Advance(epoch) moves last_ forward monotonically
// (max(last_, epoch), never backwards).
//
// last_ is atomic so that CursorEpoch() / Last() can be read safely from the
// owner thread (or observer threads such as test polling loops) while Advance()
// and ShouldApply() are being called from the HeadlessNode event-callback thread.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>

namespace kvcache::node::replication {

class ReplicationCursor {
 public:
  // Returns true iff epoch > last_ (not yet applied).
  bool ShouldApply(uint64_t epoch) const {
    return epoch > last_.load(std::memory_order_acquire);
  }

  // Advances the cursor monotonically: last_ = max(last_, epoch).
  // Only the single event-callback thread calls this, so a simple
  // load-then-store (no CAS loop) is sufficient for correctness; the acquire
  // load pairs with the release store to make the new value visible to
  // observers calling Last() / ShouldApply().
  void Advance(uint64_t epoch) {
    const uint64_t cur = last_.load(std::memory_order_acquire);
    if (epoch > cur) {
      last_.store(epoch, std::memory_order_release);
    }
  }

  // Returns the last-applied epoch.  Safe to call from any thread.
  uint64_t Last() const { return last_.load(std::memory_order_acquire); }

 private:
  std::atomic<uint64_t> last_{0};
};

}  // namespace kvcache::node::replication
