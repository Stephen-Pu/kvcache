// LLD §5.2 — mTLS termination + CN → Tenant lookup (Scheme C).
//
// At connection time the gRPC server peer's TLS certificate is parsed and its
// CN string extracted. The CN may map to:
//   * a Tenant identity (service-to-service traffic on behalf of a tenant)
//   * an internal-component identity (cert-manager-issued, rotating every 30d)
//   * an admin identity (a small set of operator certs)
//
// Scheme C: we do NOT force the PKI to encode the tenant_id inside the cert.
// Instead the runtime keeps a CN → Tenant lookup table that the CP refreshes.
#pragma once

#include <array>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvcache::node::security {

// Parsed identity material lifted out of a peer's X.509 leaf cert.
// CN is the historical Scheme-C lookup key; SANs (esp. the URI SAN
// carrying a SPIFFE ID) are the modern, SAN-first identity surface
// the CN-table maps over. Phase B8 makes this a real OpenSSL parse;
// the legacy string-scan stays as a fallback when the binary is
// built without OpenSSL.
struct CertInfo {
    std::string                cn;        // subject commonName ("" if absent)
    std::vector<std::string>   dns_sans;  // DNS: entries from subjectAltName
    std::vector<std::string>   uri_sans;  // URI: entries from subjectAltName
    // First uri_san that begins with "spiffe://", if any. This is the
    // identity a SPIFFE-aware deployment authenticates on.
    std::optional<std::string> spiffe_id;
};

enum class IdentityKind : uint8_t {
    kUnknown  = 0,
    kTenant   = 1,
    kInternal = 2,
    kAdmin    = 3,
};

const char* IdentityKindName(IdentityKind k);

struct Identity {
    IdentityKind            kind = IdentityKind::kUnknown;
    std::string             cn;
    std::array<uint8_t, 16> tenant_id{};
    std::string             display_name;
    // Phase B8.2 — the authoritative tenant string this identity is
    // entitled to act as (the value an engine passes as the request's
    // tenant_id). The CP populates it in the spiffe_id→Identity /
    // CN→Identity tables. Empty for non-tenant identities (internal /
    // admin certs).
    std::string             tenant;
};

class MtlsRegistry {
   public:
    MtlsRegistry()  = default;
    ~MtlsRegistry() = default;

    bool UpsertMapping(const std::string& cn, const Identity& id);
    bool RemoveMapping(const std::string& cn);
    std::optional<Identity> Resolve(const std::string& cn) const;
    std::size_t Size() const noexcept;

    // ----- Phase B8.1 — SPIFFE-first authz --------------------------------
    //
    // A SPIFFE ID (the URI SAN `spiffe://<trust-domain>/<path>`) is the
    // modern, SAN-bound identity. The CP publishes a spiffe_id → Tenant
    // mapping the same way it publishes the CN table; the runtime
    // authenticates on the SPIFFE ID when the peer cert carries one,
    // falling back to CN only when it doesn't. SPIFFE-first because the
    // ID is bound into the cert's SAN extension (can't be spoofed by a
    // misissued CN) and carries the trust domain explicitly.

    bool UpsertSpiffeMapping(const std::string& spiffe_id, const Identity& id);
    bool RemoveSpiffeMapping(const std::string& spiffe_id);
    std::optional<Identity> ResolveBySpiffe(const std::string& spiffe_id) const;
    std::size_t SpiffeSize() const noexcept;

    // Optional trust-domain enforcement. When set (non-empty), a SPIFFE
    // ID whose trust domain != this is rejected by ResolveCert even if
    // its full id happens to be in the map — defends against a cert
    // minted under a foreign/rogue trust domain. Empty = accept any
    // trust domain present in the map.
    void SetRequiredTrustDomain(std::string trust_domain);

    // SPIFFE-first authz entry point. Given a parsed CertInfo:
    //   1. if it carries a spiffe_id that (a) parses, (b) passes the
    //      trust-domain check, and (c) is in the SPIFFE map → that Identity.
    //   2. else fall back to the CN table.
    //   3. else nullopt (unauthenticated).
    std::optional<Identity> ResolveCert(const CertInfo& cert) const;

    // Parse + validate a SPIFFE ID. Returns {trust_domain, path} on a
    // well-formed `spiffe://<trust-domain>/<path>` (non-empty trust
    // domain; path may be empty for a bare-domain id), nullopt otherwise.
    struct SpiffeId {
        std::string trust_domain;
        std::string path;  // leading '/' included; empty for bare domain
    };
    static std::optional<SpiffeId> ParseSpiffeId(const std::string& uri);

    // Phase B8.2 — SPIFFE workload-path tenant convention. Given a
    // SPIFFE id whose path embeds a tenant as `…/tenant/<id>[/…]`,
    // return <id>. Used as a no-table-needed resolution path: a SPIFFE
    // id of `spiffe://td/ns/x/tenant/acme/workload/w` resolves to tenant
    // "acme". Returns nullopt when the path carries no `/tenant/<id>`
    // segment. The `uri` is the full `spiffe://…` string (it parses it).
    static std::optional<std::string> TenantFromSpiffePath(const std::string& uri);

    // Phase B8.2 — resolve the authoritative tenant a peer cert is
    // entitled to act as, in precedence order:
    //   1. registry table   — registry!=nullptr && ResolveCert(cert)
    //                          yields an Identity with a non-empty
    //                          .tenant (the CP-published mapping).
    //   2. SPIFFE path       — cert.spiffe_id embeds `/tenant/<id>`.
    //   3. CN                — cert.cn (the historical Scheme-C form).
    // Returns nullopt when none resolve (cert carries no usable
    // identity → caller rejects as UNAUTHENTICATED). Static + free of
    // grpc so the binding decision is unit-testable without a TLS
    // handshake.
    static std::optional<std::string> ResolveTenant(const CertInfo& cert,
                                                     const MtlsRegistry* registry);

    // Phase B8 — full leaf-cert parse. When built with OpenSSL
    // (KVCACHE_HAVE_OPENSSL), this does a real PEM → X509 decode and
    // pulls the subject CN + every DNS/URI subjectAltName + the first
    // SPIFFE URI. Without OpenSSL it degrades to a CN-only string scan
    // (dns_sans/uri_sans empty, spiffe_id nullopt) so non-OpenSSL
    // builds still compile + the CN path still works. Returns nullopt
    // when the PEM can't be parsed at all (OpenSSL build) or contains
    // no "CN=" marker (fallback build).
    static std::optional<CertInfo> ParsePem(const std::string& pem);

    // True iff this binary was built with real OpenSSL X.509 parsing.
    // Tests gate SAN/SPIFFE assertions on this; the CN-only path is
    // always exercised.
    static bool HasRealParser() noexcept;

    // Convenience wrapper preserved for existing callers — returns
    // just the CN. Delegates to ParsePem.
    static std::optional<std::string> ExtractCnFromPem(const std::string& pem);

   private:
    mutable std::mutex                          mu_;
    std::unordered_map<std::string, Identity>   by_cn_;
    std::unordered_map<std::string, Identity>   by_spiffe_;   // Phase B8.1
    std::string                                 required_trust_domain_;
};

}  // namespace kvcache::node::security
