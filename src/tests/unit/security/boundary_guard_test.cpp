// src/tests/unit/security/boundary_guard_test.cpp
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
using namespace kvcache::node::security;

static BoundaryGuard MakeGuard(std::vector<Rule> allow, bool default_deny = true) {
    return BoundaryGuard(BoundaryPolicy{std::move(allow), default_deny});
}

TEST(BoundaryGuard, HostSuffixGlobAllows) {
    auto g = MakeGuard({{.host_glob = "*.svc.cluster.local"}});
    EXPECT_TRUE(g.Check({.host = "node-1.svc.cluster.local", .port = 443}).allow);
    auto d = g.Check({.host = "evil.example.com", .port = 443});
    EXPECT_FALSE(d.allow);
    EXPECT_FALSE(d.reason.empty()) << "deny must explain why";
}

TEST(BoundaryGuard, ExactHostAllows) {
    auto g = MakeGuard({{.host_glob = "s3.us-gov-west-1.amazonaws.com"}});
    EXPECT_TRUE(g.Check({.host = "s3.us-gov-west-1.amazonaws.com"}).allow);
    EXPECT_FALSE(g.Check({.host = "s3.us-east-1.amazonaws.com"}).allow);
}

TEST(BoundaryGuard, CidrAllows) {
    auto g = MakeGuard({{.cidr = "10.0.0.0/8"}});
    EXPECT_TRUE(g.Check({.host = "10.5.6.7"}).allow);
    EXPECT_FALSE(g.Check({.host = "192.168.1.1"}).allow);
}

TEST(BoundaryGuard, EmptyAllowlistIsAirGap) {
    auto g = MakeGuard({});  // default_deny = true, no rules
    EXPECT_FALSE(g.Check({.host = "anything.local"}).allow);
    EXPECT_FALSE(g.Check({.host = "10.0.0.1"}).allow);
}

TEST(BoundaryGuard, PerPurposeRuleScoping) {
    // A rule scoped to kKms only allows KMS-purpose endpoints on that host.
    auto g = MakeGuard({{.host_glob = "kms.local", .purpose = Purpose::kKms}});
    EXPECT_TRUE(g.Check({.host = "kms.local", .purpose = Purpose::kKms}).allow);
    EXPECT_FALSE(g.Check({.host = "kms.local", .purpose = Purpose::kColdTier}).allow)
        << "purpose-scoped rule must not admit a different purpose";
}

TEST(BoundaryGuard, MalformedEndpointDenies) {
    auto g = MakeGuard({{.host_glob = "*.local"}});
    EXPECT_FALSE(g.Check({.host = ""}).allow) << "empty host is fail-closed deny";
}

TEST(HostMatchesGlob, SuffixWildcard) {
    EXPECT_TRUE(HostMatchesGlob("a.b.example.com", "*.example.com"));
    EXPECT_TRUE(HostMatchesGlob("example.com", "example.com"));
    EXPECT_FALSE(HostMatchesGlob("example.com.evil.net", "*.example.com"));
    EXPECT_FALSE(HostMatchesGlob("", "*.example.com"));
}

TEST(IpInCidr, Ipv4Ranges) {
    EXPECT_TRUE(IpInCidr("10.1.2.3", "10.0.0.0/8"));
    EXPECT_FALSE(IpInCidr("11.0.0.1", "10.0.0.0/8"));
    EXPECT_TRUE(IpInCidr("192.168.1.5", "192.168.1.0/24"));
    EXPECT_FALSE(IpInCidr("192.168.2.5", "192.168.1.0/24"));
    EXPECT_FALSE(IpInCidr("not-an-ip", "10.0.0.0/8")) << "malformed → false (fail-closed)";
    EXPECT_FALSE(IpInCidr("10.0.0.1", "garbage")) << "malformed cidr → false";
}
