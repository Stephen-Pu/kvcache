#include "security/mtls.h"

#include <gtest/gtest.h>

using namespace kvcache::node::security;

TEST(MtlsRegistryTest, UpsertResolveRemove) {
    MtlsRegistry r;
    Identity id;
    id.kind = IdentityKind::kTenant;
    id.cn = "tenant-A";
    id.display_name = "Tenant Alpha";
    EXPECT_TRUE(r.UpsertMapping("tenant-A", id));
    auto got = r.Resolve("tenant-A");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->kind, IdentityKind::kTenant);
    EXPECT_EQ(got->display_name, "Tenant Alpha");
    EXPECT_TRUE(r.RemoveMapping("tenant-A"));
    EXPECT_FALSE(r.Resolve("tenant-A").has_value());
}

TEST(MtlsRegistryTest, ExtractCnFromFakeSubjectString) {
    // This fake "Subject:" line is NOT a valid PEM cert. The fallback
    // string-scan parser extracts the CN= token from it; the real
    // OpenSSL parser correctly rejects it (it's not a cert). Branch
    // the expectation on which parser is compiled in.
    auto cn = MtlsRegistry::ExtractCnFromPem(
        "Subject: O=Acme, CN=kvagent-node-1, OU=infra\n");
    if (MtlsRegistry::HasRealParser()) {
        EXPECT_FALSE(cn.has_value())
            << "real X.509 parser must reject a non-cert blob";
    } else {
        ASSERT_TRUE(cn.has_value());
        EXPECT_EQ(*cn, "kvagent-node-1");
    }
}

TEST(MtlsRegistryTest, ExtractCnReturnsNulloptOnMissing) {
    // No CN= marker AND not a valid cert — both parsers return nullopt.
    auto cn = MtlsRegistry::ExtractCnFromPem("Subject: O=Acme\n");
    EXPECT_FALSE(cn.has_value());
}

// Phase B8 — a real, self-signed leaf cert (RSA-2048) with:
//   Subject: O=Acme, CN=kvagent-node-1
//   subjectAltName: DNS:node-1.kvcache.svc,
//                   URI:spiffe://kvcache.example/node/node-1
// Generated once with `openssl req -x509` and pinned here so the test
// is hermetic (no openssl(1) at test time). Valid until 2036.
static const char* kTestLeafPem =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDRDCCAiygAwIBAgIUN582WEUR3GJ9qBwodgaX2qRUw0wwDQYJKoZIhvcNAQEL\n"
    "BQAwKDENMAsGA1UECgwEQWNtZTEXMBUGA1UEAwwOa3ZhZ2VudC1ub2RlLTEwHhcN\n"
    "MjYwNjAyMTgwMjAwWhcNMzYwNTMwMTgwMjAwWjAoMQ0wCwYDVQQKDARBY21lMRcw\n"
    "FQYDVQQDDA5rdmFnZW50LW5vZGUtMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC\n"
    "AQoCggEBAK1qaOzITxkzOAaa3JYdaLz3b5ORNSNK8K0nJ3o80rTREuIh1ubwcJdl\n"
    "tlgGeKPxAXJ+D8ZlL/3hJKhZqh8YGOZT0hyZF7r+KzBf9aYj+PA2GOX6LaG/N7rn\n"
    "ufhF+gIe5wWvenoHtNWzB+KN6XCe1sfWrmuKqvzif5HrtuYIdrwDH1XOgTxR/5N4\n"
    "ZJ1V6GpNJFt0DrRHOYfRFkNHVzcpzEj76TuknK/1hVMpMthKJnqADMYpcCBwuPip\n"
    "g+GLWaxG4jN59BwD9cmu14ztRUWHMtpq+RG29/9L55jLSUAtMdJQPKDYFGUOxCOB\n"
    "+86fbiT/lR9M3GdmCR9fexa77SLrvAECAwEAAaNmMGQwQwYDVR0RBDwwOoISbm9k\n"
    "ZS0xLmt2Y2FjaGUuc3ZjhiRzcGlmZmU6Ly9rdmNhY2hlLmV4YW1wbGUvbm9kZS9u\n"
    "b2RlLTEwHQYDVR0OBBYEFO2WcdUn40+xC5XKGibxrhoWW38RMA0GCSqGSIb3DQEB\n"
    "CwUAA4IBAQBZJ3FOq29En2l7Z+iKeO2W2AbQm0RHPSqEN78u6BsD1uhR8Q9P97EX\n"
    "eqydYnoLzXJ3djYgpA57KtdJJR6DvHF3j3tg1NiUMMoLQu7Fu+S1CllsCI1kCiFP\n"
    "XdfM2ue2T463SqfZ+Z1nvbB5dxb17lCfIDc89DG9B6yv9EmjrDDGCeoR/56/6KXr\n"
    "0+Vf++5c/uIKwt/zE8zSPHyZBP4wjvE6IIEEAHlD3gJPKgur9ChS0Whh1F/0PrUX\n"
    "SHB1OyL7PHwhd+g6ynXxeTsefo5O/TCNti88RSDuAiHu4geV1Wnl4A0D8EmpLNDe\n"
    "4remVuPFy4rhUAJG4ouBj/RwcZjoPUbF\n"
    "-----END CERTIFICATE-----\n";

TEST(MtlsRegistryTest, ParsePemExtractsCnFromRealCert) {
    if (!MtlsRegistry::HasRealParser()) {
        GTEST_SKIP() << "built without OpenSSL — real X.509 parse unavailable";
    }
    auto info = MtlsRegistry::ParsePem(kTestLeafPem);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->cn, "kvagent-node-1");
}

TEST(MtlsRegistryTest, ParsePemExtractsDnsAndUriSans) {
    if (!MtlsRegistry::HasRealParser()) {
        GTEST_SKIP() << "built without OpenSSL";
    }
    auto info = MtlsRegistry::ParsePem(kTestLeafPem);
    ASSERT_TRUE(info.has_value());
    ASSERT_EQ(info->dns_sans.size(), 1u);
    EXPECT_EQ(info->dns_sans[0], "node-1.kvcache.svc");
    ASSERT_EQ(info->uri_sans.size(), 1u);
    EXPECT_EQ(info->uri_sans[0], "spiffe://kvcache.example/node/node-1");
}

TEST(MtlsRegistryTest, ParsePemExtractsSpiffeId) {
    if (!MtlsRegistry::HasRealParser()) {
        GTEST_SKIP() << "built without OpenSSL";
    }
    auto info = MtlsRegistry::ParsePem(kTestLeafPem);
    ASSERT_TRUE(info.has_value());
    ASSERT_TRUE(info->spiffe_id.has_value());
    EXPECT_EQ(*info->spiffe_id, "spiffe://kvcache.example/node/node-1");
}

TEST(MtlsRegistryTest, ExtractCnFromRealCertViaWrapper) {
    if (!MtlsRegistry::HasRealParser()) {
        GTEST_SKIP() << "built without OpenSSL";
    }
    auto cn = MtlsRegistry::ExtractCnFromPem(kTestLeafPem);
    ASSERT_TRUE(cn.has_value());
    EXPECT_EQ(*cn, "kvagent-node-1");
}

TEST(MtlsRegistryTest, ParsePemRejectsGarbage) {
    if (!MtlsRegistry::HasRealParser()) {
        GTEST_SKIP() << "built without OpenSSL";
    }
    // A PEM-ish blob that isn't a valid cert — real parse must reject it.
    auto info = MtlsRegistry::ParsePem(
        "-----BEGIN CERTIFICATE-----\nnot base64 at all!!!\n"
        "-----END CERTIFICATE-----\n");
    EXPECT_FALSE(info.has_value());
}

// ===== Phase B8.1 — SPIFFE-first authz =====================================

TEST(SpiffeParseTest, ParsesWellFormedId) {
    auto id = MtlsRegistry::ParseSpiffeId("spiffe://kvcache.example/node/node-1");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(id->trust_domain, "kvcache.example");
    EXPECT_EQ(id->path, "/node/node-1");
}

TEST(SpiffeParseTest, ParsesBareDomain) {
    auto id = MtlsRegistry::ParseSpiffeId("spiffe://kvcache.example");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(id->trust_domain, "kvcache.example");
    EXPECT_TRUE(id->path.empty());
}

TEST(SpiffeParseTest, RejectsMalformed) {
    EXPECT_FALSE(MtlsRegistry::ParseSpiffeId("").has_value());
    EXPECT_FALSE(MtlsRegistry::ParseSpiffeId("https://kvcache.example/x").has_value());
    EXPECT_FALSE(MtlsRegistry::ParseSpiffeId("spiffe://").has_value());      // empty td
    EXPECT_FALSE(MtlsRegistry::ParseSpiffeId("spiffe:///path").has_value()); // empty td
    EXPECT_FALSE(MtlsRegistry::ParseSpiffeId("spiffe://td/").has_value());   // bare slash
}

TEST(SpiffeAuthzTest, UpsertResolveRemoveBySpiffe) {
    MtlsRegistry r;
    Identity id;
    id.kind = IdentityKind::kInternal;
    id.cn = "ignored";
    id.display_name = "Node One";
    const std::string sid = "spiffe://kvcache.example/node/node-1";
    EXPECT_TRUE(r.UpsertSpiffeMapping(sid, id));
    EXPECT_EQ(r.SpiffeSize(), 1u);
    auto got = r.ResolveBySpiffe(sid);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "Node One");
    EXPECT_TRUE(r.RemoveSpiffeMapping(sid));
    EXPECT_FALSE(r.ResolveBySpiffe(sid).has_value());
}

TEST(SpiffeAuthzTest, ResolveCertPrefersSpiffeOverCn) {
    MtlsRegistry r;
    Identity cnId;   cnId.display_name = "via-CN";
    Identity sidId;  sidId.display_name = "via-SPIFFE";
    r.UpsertMapping("kvagent-node-1", cnId);
    r.UpsertSpiffeMapping("spiffe://kvcache.example/node/node-1", sidId);

    CertInfo cert;
    cert.cn = "kvagent-node-1";
    cert.spiffe_id = "spiffe://kvcache.example/node/node-1";
    auto got = r.ResolveCert(cert);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "via-SPIFFE") << "SPIFFE must win over CN";
}

TEST(SpiffeAuthzTest, ResolveCertFallsBackToCnWhenSpiffeUnmapped) {
    MtlsRegistry r;
    Identity cnId; cnId.display_name = "via-CN";
    r.UpsertMapping("kvagent-node-1", cnId);
    CertInfo cert;
    cert.cn = "kvagent-node-1";
    cert.spiffe_id = "spiffe://kvcache.example/node/unmapped";  // valid, not in map
    auto got = r.ResolveCert(cert);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "via-CN");
}

TEST(SpiffeAuthzTest, ResolveCertCnOnlyWhenNoSpiffe) {
    MtlsRegistry r;
    Identity cnId; cnId.display_name = "via-CN";
    r.UpsertMapping("kvagent-node-1", cnId);
    CertInfo cert;
    cert.cn = "kvagent-node-1";  // no spiffe_id
    auto got = r.ResolveCert(cert);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "via-CN");
}

TEST(SpiffeAuthzTest, TrustDomainMismatchRefusesAndDoesNotFallBack) {
    MtlsRegistry r;
    r.SetRequiredTrustDomain("kvcache.example");
    Identity cnId; cnId.display_name = "via-CN";
    r.UpsertMapping("kvagent-node-1", cnId);  // CN would otherwise match

    CertInfo cert;
    cert.cn = "kvagent-node-1";
    cert.spiffe_id = "spiffe://rogue.evil/node/node-1";  // foreign trust domain
    auto got = r.ResolveCert(cert);
    EXPECT_FALSE(got.has_value())
        << "foreign trust domain must refuse, not fall back to a CN match";
}

TEST(SpiffeAuthzTest, TrustDomainMatchResolves) {
    MtlsRegistry r;
    r.SetRequiredTrustDomain("kvcache.example");
    Identity sidId; sidId.display_name = "via-SPIFFE";
    r.UpsertSpiffeMapping("spiffe://kvcache.example/node/node-1", sidId);
    CertInfo cert;
    cert.spiffe_id = "spiffe://kvcache.example/node/node-1";
    auto got = r.ResolveCert(cert);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "via-SPIFFE");
}

TEST(SpiffeAuthzTest, MalformedSpiffeIgnoredFallsBackToCn) {
    MtlsRegistry r;
    Identity cnId; cnId.display_name = "via-CN";
    r.UpsertMapping("kvagent-node-1", cnId);
    CertInfo cert;
    cert.cn = "kvagent-node-1";
    cert.spiffe_id = "not-a-spiffe-uri";  // malformed → ignored
    auto got = r.ResolveCert(cert);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->display_name, "via-CN");
}

// ===== Phase B8.2 — ResolveTenant (binding decision) =======================

TEST(TenantFromSpiffePathTest, ExtractsTenantSegment) {
    EXPECT_EQ(*MtlsRegistry::TenantFromSpiffePath(
                  "spiffe://kvcache.example/tenant/acme"), "acme");
    EXPECT_EQ(*MtlsRegistry::TenantFromSpiffePath(
                  "spiffe://kvcache.example/ns/x/tenant/acme/workload/w"), "acme");
}

TEST(TenantFromSpiffePathTest, NulloptWhenNoTenantSegment) {
    EXPECT_FALSE(MtlsRegistry::TenantFromSpiffePath(
                     "spiffe://kvcache.example/workload/w").has_value());
    EXPECT_FALSE(MtlsRegistry::TenantFromSpiffePath("not-a-spiffe").has_value());
    EXPECT_FALSE(MtlsRegistry::TenantFromSpiffePath(
                     "spiffe://kvcache.example/tenant/").has_value());  // empty id
}

TEST(ResolveTenantTest, CnFallbackWhenNoRegistryNoSpiffe) {
    CertInfo cert;
    cert.cn = "tenant-a";
    auto t = MtlsRegistry::ResolveTenant(cert, /*registry=*/nullptr);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, "tenant-a");  // historical Scheme-C: CN IS the tenant
}

TEST(ResolveTenantTest, SpiffePathBeatsCnWhenNoRegistry) {
    CertInfo cert;
    cert.cn = "cn-tenant";
    cert.spiffe_id = "spiffe://kvcache.example/tenant/spiffe-tenant";
    auto t = MtlsRegistry::ResolveTenant(cert, nullptr);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, "spiffe-tenant") << "SPIFFE path must win over CN";
}

TEST(ResolveTenantTest, RegistryTableBeatsSpiffePathAndCn) {
    MtlsRegistry r;
    Identity id;
    id.tenant = "table-tenant";
    r.UpsertSpiffeMapping("spiffe://kvcache.example/node/n1", id);

    CertInfo cert;
    cert.cn = "cn-tenant";
    cert.spiffe_id = "spiffe://kvcache.example/node/n1";  // mapped, no /tenant/ seg
    auto t = MtlsRegistry::ResolveTenant(cert, &r);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, "table-tenant") << "registry table is highest precedence";
}

TEST(ResolveTenantTest, FallsThroughTableMissToSpiffePath) {
    MtlsRegistry r;  // empty table
    CertInfo cert;
    cert.cn = "cn-tenant";
    cert.spiffe_id = "spiffe://kvcache.example/tenant/path-tenant";
    auto t = MtlsRegistry::ResolveTenant(cert, &r);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, "path-tenant") << "table miss → SPIFFE path";
}

TEST(ResolveTenantTest, NulloptWhenNothingResolves) {
    CertInfo cert;  // no cn, no spiffe
    EXPECT_FALSE(MtlsRegistry::ResolveTenant(cert, nullptr).has_value());
}

TEST(ResolveTenantTest, TableEntryWithoutTenantFallsThrough) {
    MtlsRegistry r;
    Identity id;  // .tenant left empty (e.g. an internal/admin identity)
    r.UpsertSpiffeMapping("spiffe://kvcache.example/node/n1", id);
    CertInfo cert;
    cert.cn = "cn-tenant";
    cert.spiffe_id = "spiffe://kvcache.example/node/n1";
    auto t = MtlsRegistry::ResolveTenant(cert, &r);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, "cn-tenant") << "empty .tenant → fall through (no /tenant/ seg → CN)";
}
