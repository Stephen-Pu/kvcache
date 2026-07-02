// src/tests/unit/tier/guarded_transport_test.cpp
#include "tier/guarded_transport.h"
#include "tier/rest_cold_tier.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
#include <memory>
using namespace kvcache::node::tier;
using namespace kvcache::node::security;

namespace {
// Records whether the inner transport was ever dialed.
struct SpyTransport : IHttpTransport {
    int calls = 0;
    HttpResult Request(const std::string&, const std::string&,
                       const std::vector<std::string>&, const uint8_t*, std::size_t) override {
        ++calls; return HttpResult{200, "ok", ""};
    }
};
}  // namespace

TEST(GuardedHttpTransport, AllowsInBoundaryHostAndDelegates) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*.svc.local"}}, true});
    GuardedHttpTransport g(spy, guard);
    auto r = g.Request("GET", "https://store.svc.local/bucket/obj.kv", {}, nullptr, 0);
    EXPECT_EQ(r.status, 200);
    EXPECT_EQ(spy->calls, 1) << "in-boundary request must reach inner transport";
}

TEST(GuardedHttpTransport, BlocksOutOfBoundaryHostAndNeverDials) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*.svc.local"}}, true});
    bool denied = false; std::string reason;
    GuardedHttpTransport g(spy, guard, [&](const Endpoint& ep, std::string_view why) {
        denied = true; reason = std::string(why); EXPECT_EQ(ep.purpose, Purpose::kColdTier);
    });
    auto r = g.Request("GET", "https://exfil.evil.com/x", {}, nullptr, 0);
    EXPECT_EQ(spy->calls, 0) << "out-of-boundary request must NOT dial";
    EXPECT_NE(r.transport_err, "") << "deny surfaces as transport_err (fail-closed)";
    EXPECT_TRUE(denied);
    EXPECT_FALSE(reason.empty());
}

TEST(GuardedHttpTransport, MalformedUrlIsDenied) {
    auto spy = std::make_shared<SpyTransport>();
    auto guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{{.host_glob = "*"}}, true});
    GuardedHttpTransport g(spy, guard);
    auto r = g.Request("GET", "not-a-url", {}, nullptr, 0);
    EXPECT_EQ(spy->calls, 0);
    EXPECT_NE(r.transport_err, "");
}

TEST(HostFromUrl, ParsesHostAndPort) {
    uint16_t port = 0;
    EXPECT_EQ(HostFromUrl("https://a.b.com:8443/x/y", &port), "a.b.com");
    EXPECT_EQ(port, 8443);
    port = 0;
    EXPECT_EQ(HostFromUrl("http://host.local/x", &port), "host.local");
    EXPECT_EQ(port, 80);
    EXPECT_EQ(HostFromUrl("garbage", &port), "");
}
