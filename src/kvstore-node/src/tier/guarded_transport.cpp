// src/kvstore-node/src/tier/guarded_transport.cpp
//
// A10 Regulated Mode — GuardedHttpTransport implementation.
//
// HostFromUrl and GuardedHttpTransport::Request are implemented here;
// the rest of the type is inlined in the header.
#include "tier/guarded_transport.h"

namespace kvcache::node::tier {

// ---------------------------------------------------------------------------
// HostFromUrl
// ---------------------------------------------------------------------------

std::string HostFromUrl(std::string_view url, uint16_t* port_out) {
    // Determine scheme and strip it, recording the default port.
    uint16_t    default_port = 0;
    std::string_view rest;
    if (url.size() >= 8 && url.substr(0, 8) == "https://") {
        default_port = 443;
        rest         = url.substr(8);
    } else if (url.size() >= 7 && url.substr(0, 7) == "http://") {
        default_port = 80;
        rest         = url.substr(7);
    } else {
        // Unknown or missing scheme — fail-closed.
        if (port_out) *port_out = 0;
        return "";
    }

    // Isolate the authority (host[:port]), stripping any path/query/fragment.
    auto slash     = rest.find('/');
    std::string_view authority =
        (slash == std::string_view::npos) ? rest : rest.substr(0, slash);
    if (authority.empty()) {
        if (port_out) *port_out = 0;
        return "";
    }

    // Split host and optional port.
    auto colon = authority.find(':');
    std::string host{authority.substr(0, colon)};
    uint16_t    port = default_port;

    if (colon != std::string_view::npos) {
        // Parse the port number strictly — any non-digit character is malformed.
        std::string_view port_str = authority.substr(colon + 1);
        if (port_str.empty()) {
            if (port_out) *port_out = 0;
            return "";
        }
        long p = 0;
        for (char c : port_str) {
            if (c < '0' || c > '9') {
                if (port_out) *port_out = 0;
                return "";
            }
            p = p * 10 + (c - '0');
        }
        if (p <= 0 || p > 65535) {
            if (port_out) *port_out = 0;
            return "";
        }
        port = static_cast<uint16_t>(p);
    }

    if (host.empty()) {
        if (port_out) *port_out = 0;
        return "";
    }

    if (port_out) *port_out = port;
    return host;
}

// ---------------------------------------------------------------------------
// GuardedHttpTransport::Request
// ---------------------------------------------------------------------------

HttpResult GuardedHttpTransport::Request(const std::string&              method,
                                          const std::string&              url,
                                          const std::vector<std::string>& headers,
                                          const uint8_t*                  body,
                                          std::size_t                     n) {
    uint16_t    port = 0;
    std::string host = HostFromUrl(url, &port);

    // Build the endpoint regardless of whether host is empty so the observer
    // receives a populated (if incomplete) endpoint on malformed-URL denials.
    security::Endpoint ep{host, port, security::Purpose::kColdTier};

    // Evaluate the boundary decision.  An empty host means the URL was
    // unparseable — fail-closed without even consulting the guard.
    security::Decision d = host.empty()
        ? security::Decision{false, "unparseable URL (fail-closed)"}
        : guard_->Check(ep);

    if (!d.allow) {
        if (on_deny_) on_deny_(ep, d.reason);
        HttpResult r;
        r.status        = 0;
        r.transport_err = "boundary-denied: " + d.reason;
        return r;  // inner transport is never called
    }

    return inner_->Request(method, url, headers, body, n);
}

}  // namespace kvcache::node::tier
