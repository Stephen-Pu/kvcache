// LLD §3.3 — TierManager.
#include "tier/tier_manager.h"

#include <cstdio>

#include "logging.h"

namespace kvcache::node::tier {

std::unique_ptr<TierManager> TierManager::Create(const Options& opts, std::string* err) {
    auto tm = std::unique_ptr<TierManager>(new TierManager());
    tm->opts_   = opts;
    tm->pinned_ = PinnedTier::Create(opts.pinned, err);
    if (!tm->pinned_) return nullptr;
    tm->dram_   = std::make_unique<DramTier>(opts.dram);
    if (opts.enable_nvme) {
        tm->nvme_ = NvmeTier::Create(opts.nvme, err);
        if (!tm->nvme_) return nullptr;
    }
    if (opts.enable_cold) {
        tm->cold_ = CreateColdTier(opts.cold, err);
        if (!tm->cold_) return nullptr;
    }
    return tm;
}

// ---- T1 ----
std::optional<SlotDesc> TierManager::AcquirePinnedSlot() { return pinned_->Acquire(); }
void TierManager::ReleasePinnedSlot(SlotId id)            { pinned_->Release(id); }

// ---- T2 ----
void TierManager::StageToDram(const DramKey& key, const uint8_t* data, std::size_t n) {
    dram_->Insert(key, data, n);
}
DramTier::LookupResult TierManager::LookupDram(const DramKey& key) {
    return dram_->Lookup(key);
}
bool TierManager::EraseDram(const DramKey& key) { return dram_->Erase(key); }

// ---- T3 ----
bool TierManager::PutNvme(const DramKey& k, const uint8_t* d, std::size_t n, std::string* err) {
    if (!nvme_) { if (err) *err = "tier_manager: NVMe disabled"; return false; }
    return nvme_->Put(k, d, n, err);
}
bool TierManager::GetNvme(const DramKey& k, std::vector<uint8_t>* out, std::string* err) {
    if (!nvme_) { if (err) *err = "tier_manager: NVMe disabled"; return false; }
    return nvme_->Get(k, out, err);
}
bool TierManager::EraseNvme(const DramKey& k) { return nvme_ && nvme_->Erase(k); }

// ---- T4 ----
bool TierManager::PutCold(const DramKey& k, const uint8_t* d, std::size_t n, std::string* err) {
    if (!cold_) { if (err) *err = "tier_manager: Cold disabled"; return false; }
    return cold_->Put(k, d, n, err);
}
bool TierManager::GetCold(const DramKey& k, std::vector<uint8_t>* out, std::string* err) {
    if (!cold_) { if (err) *err = "tier_manager: Cold disabled"; return false; }
    return cold_->Get(k, out, err);
}
bool TierManager::EraseCold(const DramKey& k, std::string* err) {
    return cold_ && cold_->Delete(k, err);
}

// ---- Unified fetch (LLD §3.3 — Lazy promotion on access) ----

TierManager::FetchResult TierManager::Fetch(const DramKey& key, std::string* err) {
    FetchResult r{};
    // T2 first.
    auto dram_hit = dram_->Lookup(key);
    if (dram_hit.where == DramTier::HitWhere::kA1in ||
        dram_hit.where == DramTier::HitWhere::kAm) {
        r.hit  = FetchHit::kDram;
        r.data.assign(dram_hit.data, dram_hit.data + dram_hit.data_bytes);
        return r;
    }
    // T3.
    if (nvme_) {
        std::vector<uint8_t> buf;
        std::string ignored;
        if (nvme_->Get(key, &buf, &ignored)) {
            r.hit  = FetchHit::kNvme;
            r.data = buf;
            if (opts_.promote_nvme_to_dram) {
                dram_->Insert(key, r.data.data(), r.data.size());
            }
            return r;
        }
    }
    // T4.
    if (cold_) {
        std::vector<uint8_t> buf;
        std::string subsystem_err;
        if (cold_->Get(key, &buf, &subsystem_err)) {
            r.hit  = FetchHit::kCold;
            r.data = buf;
            if (opts_.promote_cold_to_nvme && nvme_) {
                std::string nvme_err;
                if (!nvme_->Put(key, r.data.data(), r.data.size(), &nvme_err)) {
                    // Phase O-2: promotion failure is non-fatal — the
                    // bytes were already served from cold and the next
                    // lookup will retry — but a sustained failure mode
                    // here means the NVMe tier is full / wedged / lost
                    // its mount, and the cluster will keep paying cold-
                    // tier latency on every Fetch. Warn so the operator
                    // sees it.
                    char buf[160];
                    std::snprintf(buf, sizeof(buf),
                                  "cold->nvme promotion failed for key "
                                  "size=%zu: %s",
                                  r.data.size(), nvme_err.c_str());
                    ::kvcache::log::Get("tier_manager").Warn(buf);
                }
            }
            if (opts_.promote_cold_to_dram) {
                dram_->Insert(key, r.data.data(), r.data.size());
            }
            return r;
        }
        if (err) *err = subsystem_err;
    }
    r.hit = FetchHit::kMiss;
    return r;
}

}  // namespace kvcache::node::tier
