// src/kvstore-node/src/tier/guarded_transport.h
//
// A10 Regulated Mode — GuardedHttpTransport
//
// Decorator that wraps an IHttpTransport and enforces a BoundaryGuard on
// every outbound request. The guard is evaluated against the URL host before
// the inner transport is ever called. Fail-closed: an unparseable URL (no
// http:// or https:// scheme, empty authority, etc.) is treated as a DENY
// without consulting the guard.
//
// Usage pattern (mirrors SigV4Transport, MetricsColdTier, EncryptingColdTier):
//
//   auto inner = MakeCurlHttpTransport();
//   auto guarded = std::make_shared<GuardedHttpTransport>(
//       inner, policy_guard,
//       [](const Endpoint& ep, std::string_view reason) {
//           LOG_WARN("boundary denied %s:%u — %s", ep.host.c_str(), ep.port,
//                    std::string(reason).c_str());
//       });
//   auto tier = RestColdTier::CreateWithTransport(opts, guarded, &err);
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "security/boundary_guard.h"  // BoundaryGuard, Endpoint, Purpose
#include "tier/rest_cold_tier.h"      // IHttpTransport, HttpResult

namespace kvcache::node::tier {

// Extract the host (no scheme, no port, no path) from an http(s) URL.
// On success sets *port_out to the explicit port (or the scheme default: 80/443)
// and returns the hostname string.
// Returns "" on malformed input — callers treat empty host as fail-closed deny.
std::string HostFromUrl(std::string_view url, uint16_t* port_out);

// Decorator: enforces a BoundaryGuard on each HTTP request's URL host.
// On DENY: invokes the optional DenyObserver, then returns an HttpResult with
//   status=0 and transport_err="boundary-denied: <reason>" — the inner
//   transport is never called.
// On ALLOW: delegates the call to the wrapped inner transport unchanged.
class GuardedHttpTransport final : public IHttpTransport {
 public:
  using DenyObserver =
      std::function<void(const security::Endpoint&, std::string_view reason)>;

  GuardedHttpTransport(std::shared_ptr<IHttpTransport>              inner,
                       std::shared_ptr<const security::BoundaryGuard> guard,
                       DenyObserver                                 on_deny = {})
      : inner_(std::move(inner)),
        guard_(std::move(guard)),
        on_deny_(std::move(on_deny)) {}

  HttpResult Request(const std::string&              method,
                     const std::string&              url,
                     const std::vector<std::string>& headers,
                     const uint8_t*                  body,
                     std::size_t                     n) override;

 private:
  std::shared_ptr<IHttpTransport>              inner_;
  std::shared_ptr<const security::BoundaryGuard> guard_;
  DenyObserver                                 on_deny_;
};

}  // namespace kvcache::node::tier
