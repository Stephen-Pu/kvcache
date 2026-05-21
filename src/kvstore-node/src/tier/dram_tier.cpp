// LLD §3.3 T2 — DRAM 2Q tier.
#include "tier/dram_tier.h"

#include <cstring>

namespace kvcache::node::tier {

DramTier::DramTier(const Options& opts)
    : capacity_bytes_(opts.capacity_bytes),
      a1in_capacity_(opts.capacity_bytes / 4),
      a1out_max_   (opts.a1out_max_entries) {}

DramTier::~DramTier() = default;

uint64_t DramTier::UsedBytes() const noexcept {
    std::lock_guard lk(mu_);
    return a1in_bytes_used_ + am_bytes_used_;
}
uint64_t DramTier::A1inBytes() const noexcept {
    std::lock_guard lk(mu_); return a1in_bytes_used_;
}
uint64_t DramTier::AmBytes() const noexcept {
    std::lock_guard lk(mu_); return am_bytes_used_;
}
std::size_t DramTier::GhostSize() const noexcept {
    std::lock_guard lk(mu_); return a1out_.size();
}

DramTier::LookupResult DramTier::Lookup(const DramKey& key) {
    std::lock_guard lk(mu_);
    auto it = index_.find(key);
    if (it != index_.end()) {
        Entry& e = *it->second;
        LookupResult r{};
        r.data       = e.data.data();
        r.data_bytes = e.data.size();
        if (e.q == Queue::Am) {
            // LRU promote within Am.
            am_.splice(am_.begin(), am_, it->second);
            it->second = am_.begin();
            r.where = HitWhere::kAm;
        } else {
            // 2Q: hit while in A1in does NOT promote.
            r.where = HitWhere::kA1in;
        }
        return r;
    }
    // Not in cache; check ghost.
    if (ghost_index_.find(key) != ghost_index_.end()) {
        LookupResult r{};
        r.where = HitWhere::kGhost;
        return r;  // caller will re-fetch and Insert(); admission will route to Am
    }
    return {};
}

void DramTier::Insert(const DramKey& key, const uint8_t* data, std::size_t n) {
    std::lock_guard lk(mu_);
    // If already resident, replace in place (keep its queue).
    auto cur = index_.find(key);
    if (cur != index_.end()) {
        Entry& e = *cur->second;
        const std::size_t old = e.data.size();
        if (e.q == Queue::A1in) {
            a1in_bytes_used_ = a1in_bytes_used_ - old + n;
        } else {
            am_bytes_used_   = am_bytes_used_   - old + n;
        }
        e.data.assign(data, data + n);
        EvictToFit(0);
        return;
    }
    // Determine target queue per 2Q admission.
    auto gh = ghost_index_.find(key);
    Queue target = Queue::A1in;
    if (gh != ghost_index_.end()) {
        // Promote from ghost → Am head.
        a1out_.erase(gh->second);
        ghost_index_.erase(gh);
        target = Queue::Am;
    }
    EvictToFit(n);
    if (target == Queue::Am) {
        am_.push_front(Entry{key, Queue::Am, std::vector<uint8_t>(data, data + n)});
        index_[key] = am_.begin();
        am_bytes_used_ += n;
    } else {
        a1in_.push_front(Entry{key, Queue::A1in, std::vector<uint8_t>(data, data + n)});
        index_[key] = a1in_.begin();
        a1in_bytes_used_ += n;
    }
}

bool DramTier::Erase(const DramKey& key) {
    std::lock_guard lk(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) {
        auto g = ghost_index_.find(key);
        if (g == ghost_index_.end()) return false;
        a1out_.erase(g->second);
        ghost_index_.erase(g);
        return true;
    }
    Entry& e = *it->second;
    if (e.q == Queue::A1in) {
        a1in_bytes_used_ -= e.data.size();
        a1in_.erase(it->second);
    } else {
        am_bytes_used_   -= e.data.size();
        am_.erase(it->second);
    }
    index_.erase(it);
    return true;
}

void DramTier::GhostInsert(const DramKey& key) {
    a1out_.push_front(key);
    ghost_index_[key] = a1out_.begin();
    while (a1out_.size() > a1out_max_) {
        ghost_index_.erase(a1out_.back());
        a1out_.pop_back();
    }
}

void DramTier::EvictToFit(std::size_t incoming_bytes) {
    // Bring A1in within budget first (it's where new entries land; if the new
    // entry is going to Am it still has to share the global capacity).
    // We add `incoming_bytes` to the budget check so that adding a new entry
    // can push existing A1in tails into the ghost queue.
    while (!a1in_.empty() &&
           a1in_bytes_used_ + incoming_bytes > a1in_capacity_) {
        // Evict the FIFO tail of A1in into the ghost queue.
        const Entry& victim = a1in_.back();
        a1in_bytes_used_ -= victim.data.size();
        GhostInsert(victim.key);
        index_.erase(victim.key);
        a1in_.pop_back();
    }
    // Now ensure overall capacity is honored. Prefer evicting from Am tail
    // (true LRU) once A1in is at its budget.
    while (a1in_bytes_used_ + am_bytes_used_ + incoming_bytes > capacity_bytes_) {
        if (!am_.empty()) {
            const Entry& victim = am_.back();
            am_bytes_used_ -= victim.data.size();
            index_.erase(victim.key);
            am_.pop_back();
        } else if (!a1in_.empty()) {
            const Entry& victim = a1in_.back();
            a1in_bytes_used_ -= victim.data.size();
            GhostInsert(victim.key);
            index_.erase(victim.key);
            a1in_.pop_back();
        } else {
            break;  // nothing left to evict
        }
    }
}

}  // namespace kvcache::node::tier
