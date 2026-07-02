// src/kvstore-node/src/security/boundary_guard.h
#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace kvcache::node::security {
enum class Purpose { kColdTier, kKms, kTelemetry, kReplication, kEtcd, kOther };
struct Endpoint { std::string host; uint16_t port = 0; Purpose purpose = Purpose::kOther; };
struct Rule { std::string host_glob; std::string cidr; Purpose purpose = Purpose::kOther; };
struct BoundaryPolicy { std::vector<Rule> allow; bool default_deny = true; };
struct Decision { bool allow = false; std::string reason; };

bool HostMatchesGlob(std::string_view host, std::string_view glob);
bool IpInCidr(std::string_view ip, std::string_view cidr);

class BoundaryGuard {
 public:
  explicit BoundaryGuard(BoundaryPolicy policy) : policy_(std::move(policy)) {}
  Decision Check(const Endpoint& ep) const;
 private:
  BoundaryPolicy policy_;
};
}  // namespace kvcache::node::security
