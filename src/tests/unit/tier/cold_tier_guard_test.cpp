// A10 — BoundaryGuard wired into the cold-tier factory.
//
// Verifies that a native-rest cold tier built with a deny-all guard refuses
// egress without dialing — the deny observer fires, proving GuardedHttpTransport
// wrapped the inner transport before any network call was made.
#include "tier/cold_tier.h"
#include "security/boundary_guard.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

using namespace kvcache::node::tier;
using namespace kvcache::node::security;

// With a deny-all guard, a native-rest cold tier must refuse egress WITHOUT
// dialing — Get returns not-found/error and no network occurs (the guard
// short-circuits before libcurl). This proves the factory wrapped the transport.
TEST(ColdTierGuard, NativeRestWithDenyAllGuardBlocksEgress) {
    ColdTierOptions opts;
    opts.type = "native-rest";
    opts.rest.base_url = "https://out-of-boundary.example.com/bucket";
    // Empty allowlist + default_deny = air-gap: denies everything.
    opts.guard = std::make_shared<BoundaryGuard>(BoundaryPolicy{{}, /*default_deny=*/true});
    bool denied = false;
    opts.deny_observer = [&](const Endpoint& ep, std::string_view) {
        denied = true;
        EXPECT_EQ(ep.purpose, Purpose::kColdTier);
    };

    std::string err;
    auto tier = CreateColdTier(opts, &err);
    ASSERT_NE(tier, nullptr) << err;   // factory still constructs the tier

    // A read must be denied by the guard (no network). RestColdTier maps a
    // transport error to a miss/error; the key assertion is the observer fired
    // and the call returned quickly without a real DNS/connect.
    std::vector<uint8_t> out;
    DramKey k{};                       // any key; content irrelevant
    (void)tier->Get(k, &out, &err);    // IColdTier::Get(const DramKey&, std::vector<uint8_t>*, std::string*)
    EXPECT_TRUE(denied) << "guard must have denied the out-of-boundary egress";
}
