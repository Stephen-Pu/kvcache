// LLD §5.2 — Rbac.
#include "security/rbac.h"

#include "qos/tenant.h"

namespace kvcache::node::security {

const char* ActionName(Action a) {
    switch (a) {
        case Action::kLookup:       return "lookup";
        case Action::kReserve:      return "reserve";
        case Action::kPublish:      return "publish";
        case Action::kFetch:        return "fetch";
        case Action::kSeal:         return "seal";
        case Action::kRelease:      return "release";
        case Action::kAdminRead:    return "admin_read";
        case Action::kAdminWrite:   return "admin_write";
        case Action::kClusterAdmin: return "cluster_admin";
    }
    return "?";
}

Rbac::Rbac() : Rbac(Options{}) {}
Rbac::Rbac(const Options& opts) : ttl_(opts.cache_ttl) {}

Decision Rbac::Evaluate(const Identity& peer, Action a, uint64_t target_tenant,
                        bool deletion_pending) const {
    // Step 1: identity present.
    if (peer.kind == IdentityKind::kUnknown) return Decision::kDeny;

    // Step 2: identity kind eligible.
    const bool is_data_op =
        a == Action::kLookup || a == Action::kReserve || a == Action::kPublish ||
        a == Action::kFetch  || a == Action::kSeal    || a == Action::kRelease;

    if (is_data_op) {
        // Tenants and internal/service principals may execute data-plane ops.
        if (peer.kind != IdentityKind::kTenant &&
            peer.kind != IdentityKind::kInternal) {
            return Decision::kDeny;
        }
    } else if (a == Action::kAdminRead) {
        if (peer.kind != IdentityKind::kAdmin) return Decision::kDeny;
    } else if (a == Action::kAdminWrite || a == Action::kClusterAdmin) {
        if (peer.kind != IdentityKind::kAdmin) return Decision::kDeny;
    } else {
        return Decision::kDeny;
    }

    // Step 3-4: scope check for tenant principals.
    if (peer.kind == IdentityKind::kTenant) {
        const uint64_t self_hash = qos::TenantHash(peer.tenant_id);
        if (target_tenant != self_hash) return Decision::kDeny;
    }

    // Step 5: tenant deletion blocks writes (publish / reserve / seal).
    if (deletion_pending) {
        if (a == Action::kReserve || a == Action::kPublish ||
            a == Action::kSeal) {
            return Decision::kDeny;
        }
    }
    return Decision::kAllow;
}

Decision Rbac::Check(const Identity& peer, Action action,
                     uint64_t target_tenant_hash, bool tenant_deletion_pending) {
    const auto now = std::chrono::steady_clock::now();
    const CacheKey k{peer.cn, action, target_tenant_hash};
    {
        std::lock_guard lk(mu_);
        auto it = cache_.find(k);
        if (it != cache_.end() && it->second.expiry > now) {
            cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return it->second.decision;
        }
    }
    cache_misses_.fetch_add(1, std::memory_order_relaxed);
    const Decision d = Evaluate(peer, action, target_tenant_hash,
                                 tenant_deletion_pending);
    if (d == Decision::kDeny) denials_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lk(mu_);
        cache_[k] = CacheEntry{d, now + ttl_};
    }
    return d;
}

}  // namespace kvcache::node::security
