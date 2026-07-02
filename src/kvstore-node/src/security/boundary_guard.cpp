// src/kvstore-node/src/security/boundary_guard.cpp
#include "security/boundary_guard.h"
#include <array>
namespace kvcache::node::security {
namespace {
// A rule's purpose gate: kOther means "any purpose".
bool PurposeMatches(Purpose rule, Purpose ep) { return rule == Purpose::kOther || rule == ep; }
}  // namespace

bool HostMatchesGlob(std::string_view host, std::string_view glob) {
    if (host.empty() || glob.empty()) return false;
    if (glob.front() == '*') {
        // "*.example.com" matches any strict subdomain of example.com.
        std::string_view suffix = glob.substr(1);  // ".example.com"
        if (host.size() <= suffix.size()) return false;
        return host.substr(host.size() - suffix.size()) == suffix;
    }
    return host == glob;  // exact match
}

bool IpInCidr(std::string_view ip, std::string_view cidr) {
    auto parse_v4 = [](std::string_view s, uint32_t* out) -> bool {
        std::array<int, 4> oct{};
        int idx = 0; long cur = 0; bool any = false;
        for (size_t i = 0; i <= s.size(); ++i) {
            if (i == s.size() || s[i] == '.') {
                if (!any || cur < 0 || cur > 255 || idx > 3) return false;
                oct[idx++] = static_cast<int>(cur); cur = 0; any = false;
            } else if (s[i] >= '0' && s[i] <= '9') {
                cur = cur * 10 + (s[i] - '0'); any = true;
                if (cur > 255) return false;
            } else return false;
        }
        if (idx != 4) return false;
        *out = (uint32_t(oct[0]) << 24) | (uint32_t(oct[1]) << 16) |
               (uint32_t(oct[2]) << 8) | uint32_t(oct[3]);
        return true;
    };
    auto slash = cidr.find('/');
    if (slash == std::string_view::npos) return false;
    uint32_t net = 0, addr = 0;
    if (!parse_v4(cidr.substr(0, slash), &net)) return false;
    if (!parse_v4(ip, &addr)) return false;
    int bits = 0; auto pfx = cidr.substr(slash + 1);
    if (pfx.empty() || pfx.size() > 2) return false;
    for (char c : pfx) { if (c < '0' || c > '9') return false; bits = bits * 10 + (c - '0'); }
    if (bits < 0 || bits > 32) return false;
    uint32_t mask = bits == 0 ? 0u : (0xFFFFFFFFu << (32 - bits));
    return (net & mask) == (addr & mask);
}

Decision BoundaryGuard::Check(const Endpoint& ep) const {
    if (ep.host.empty()) return {false, "empty host (fail-closed)"};
    for (const auto& r : policy_.allow) {
        if (!PurposeMatches(r.purpose, ep.purpose)) continue;
        if (!r.host_glob.empty() && HostMatchesGlob(ep.host, r.host_glob))
            return {true, ""};
        if (!r.cidr.empty() && IpInCidr(ep.host, r.cidr))
            return {true, ""};
    }
    if (policy_.default_deny)
        return {false, "endpoint '" + ep.host + "' not in allowlist (default-deny)"};
    return {true, ""};   // default_deny == false: allow-by-default
}
}  // namespace kvcache::node::security
