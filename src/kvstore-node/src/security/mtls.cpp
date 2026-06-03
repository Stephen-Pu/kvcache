// LLD §5.2 — MtlsRegistry implementation.
#include "security/mtls.h"

#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>

#ifdef KVCACHE_HAVE_OPENSSL
#  include <openssl/bio.h>
#  include <openssl/pem.h>
#  include <openssl/x509.h>
#  include <openssl/x509v3.h>
#endif

namespace kvcache::node::security {

const char* IdentityKindName(IdentityKind k) {
    switch (k) {
        case IdentityKind::kUnknown:  return "unknown";
        case IdentityKind::kTenant:   return "tenant";
        case IdentityKind::kInternal: return "internal";
        case IdentityKind::kAdmin:    return "admin";
    }
    return "?";
}

bool MtlsRegistry::UpsertMapping(const std::string& cn, const Identity& id) {
    std::lock_guard lk(mu_);
    auto [it, inserted] = by_cn_.emplace(cn, id);
    if (!inserted) it->second = id;
    return inserted;
}

bool MtlsRegistry::RemoveMapping(const std::string& cn) {
    std::lock_guard lk(mu_);
    return by_cn_.erase(cn) > 0;
}

std::optional<Identity> MtlsRegistry::Resolve(const std::string& cn) const {
    std::lock_guard lk(mu_);
    auto it = by_cn_.find(cn);
    if (it == by_cn_.end()) return std::nullopt;
    return it->second;
}

std::size_t MtlsRegistry::Size() const noexcept {
    std::lock_guard lk(mu_);
    return by_cn_.size();
}

// ----- Phase B8.1 — SPIFFE-first authz -------------------------------------

bool MtlsRegistry::UpsertSpiffeMapping(const std::string& spiffe_id,
                                       const Identity& id) {
    std::lock_guard lk(mu_);
    auto [it, inserted] = by_spiffe_.emplace(spiffe_id, id);
    if (!inserted) it->second = id;
    return inserted;
}

bool MtlsRegistry::RemoveSpiffeMapping(const std::string& spiffe_id) {
    std::lock_guard lk(mu_);
    return by_spiffe_.erase(spiffe_id) > 0;
}

std::optional<Identity> MtlsRegistry::ResolveBySpiffe(
    const std::string& spiffe_id) const {
    std::lock_guard lk(mu_);
    auto it = by_spiffe_.find(spiffe_id);
    if (it == by_spiffe_.end()) return std::nullopt;
    return it->second;
}

std::size_t MtlsRegistry::SpiffeSize() const noexcept {
    std::lock_guard lk(mu_);
    return by_spiffe_.size();
}

void MtlsRegistry::SetRequiredTrustDomain(std::string trust_domain) {
    std::lock_guard lk(mu_);
    required_trust_domain_ = std::move(trust_domain);
}

std::optional<MtlsRegistry::SpiffeId> MtlsRegistry::ParseSpiffeId(
    const std::string& uri) {
    static constexpr std::string_view kScheme = "spiffe://";
    if (uri.size() <= kScheme.size() ||
        uri.compare(0, kScheme.size(), kScheme) != 0) {
        return std::nullopt;
    }
    const std::size_t after = kScheme.size();
    // Trust domain runs up to the first '/' (or end of string for a
    // bare-domain id). Must be non-empty.
    const std::size_t slash = uri.find('/', after);
    SpiffeId out;
    if (slash == std::string::npos) {
        out.trust_domain = uri.substr(after);
        // path stays empty
    } else {
        out.trust_domain = uri.substr(after, slash - after);
        out.path = uri.substr(slash);  // includes leading '/'
    }
    if (out.trust_domain.empty()) return std::nullopt;
    // A trailing-slash-only path ("spiffe://td/") is a non-id — reject
    // a path that is just "/" with nothing after it as malformed.
    if (out.path == "/") return std::nullopt;
    return out;
}

std::optional<Identity> MtlsRegistry::ResolveCert(const CertInfo& cert) const {
    // SPIFFE-first. Only consult the SPIFFE map when the cert carries a
    // well-formed id that passes the (optional) trust-domain check.
    if (cert.spiffe_id.has_value()) {
        if (auto parsed = ParseSpiffeId(*cert.spiffe_id)) {
            std::lock_guard lk(mu_);
            if (!required_trust_domain_.empty() &&
                parsed->trust_domain != required_trust_domain_) {
                // Foreign trust domain → SPIFFE path refuses. Do NOT fall
                // back to CN here: a cert minted under a rogue domain
                // must not slip through on a CN coincidence.
                return std::nullopt;
            }
            auto it = by_spiffe_.find(*cert.spiffe_id);
            if (it != by_spiffe_.end()) return it->second;
            // SPIFFE id present + valid + right domain but unmapped →
            // fall through to CN (the deployment may not have migrated
            // this identity to the SPIFFE table yet).
        }
        // Malformed spiffe id → ignore it, try CN.
    }
    if (!cert.cn.empty()) {
        std::lock_guard lk(mu_);
        auto it = by_cn_.find(cert.cn);
        if (it != by_cn_.end()) return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> MtlsRegistry::TenantFromSpiffePath(
    const std::string& uri) {
    auto parsed = ParseSpiffeId(uri);
    if (!parsed) return std::nullopt;
    // Look for "/tenant/" in the path; the tenant is the segment that
    // follows it, up to the next '/' (or end).
    static constexpr std::string_view kMarker = "/tenant/";
    const std::string& path = parsed->path;
    const std::size_t pos = path.find(kMarker);
    if (pos == std::string::npos) return std::nullopt;
    const std::size_t start = pos + kMarker.size();
    const std::size_t end = path.find('/', start);
    const std::string id = (end == std::string::npos)
                               ? path.substr(start)
                               : path.substr(start, end - start);
    if (id.empty()) return std::nullopt;
    return id;
}

std::optional<std::string> MtlsRegistry::ResolveTenant(
    const CertInfo& cert, const MtlsRegistry* registry) {
    // 1. CP-published table (SPIFFE-first inside ResolveCert).
    if (registry != nullptr) {
        if (auto id = registry->ResolveCert(cert);
            id.has_value() && !id->tenant.empty()) {
            return id->tenant;
        }
    }
    // 2. SPIFFE workload-path convention.
    if (cert.spiffe_id.has_value()) {
        if (auto t = TenantFromSpiffePath(*cert.spiffe_id)) return t;
    }
    // 3. CN (historical Scheme-C: the CN string IS the tenant).
    if (!cert.cn.empty()) return cert.cn;
    return std::nullopt;
}

bool MtlsRegistry::HasRealParser() noexcept {
#ifdef KVCACHE_HAVE_OPENSSL
    return true;
#else
    return false;
#endif
}

#ifdef KVCACHE_HAVE_OPENSSL

namespace {

// RAII wrappers — OpenSSL frees are noexcept; nullptr-safe.
struct BioFree   { void operator()(BIO* b)   const noexcept { if (b) BIO_free(b); } };
struct X509Free  { void operator()(X509* x)  const noexcept { if (x) X509_free(x); } };

// Pull the subject CN out of an X509 via the NID path (handles
// multi-RDN subjects + UTF-8 correctly, which the string scan can't).
std::string SubjectCn(X509* cert) {
    X509_NAME* subj = X509_get_subject_name(cert);
    if (!subj) return {};
    // First pass: ask for required buffer size.
    int len = X509_NAME_get_text_by_NID(subj, NID_commonName, nullptr, 0);
    if (len <= 0) return {};
    std::string cn(static_cast<std::size_t>(len), '\0');
    // +1 for the NUL OpenSSL writes; then trim it back off.
    int written = X509_NAME_get_text_by_NID(subj, NID_commonName,
                                            cn.data(),
                                            static_cast<int>(cn.size()) + 1);
    if (written < 0) return {};
    cn.resize(static_cast<std::size_t>(written));
    return cn;
}

// Walk the subjectAltName extension, collecting DNS + URI entries.
void CollectSans(X509* cert, CertInfo* out) {
    auto* gens = static_cast<GENERAL_NAMES*>(
        X509_get_ext_d2i(cert, NID_subject_alt_name, nullptr, nullptr));
    if (!gens) return;
    const int n = sk_GENERAL_NAME_num(gens);
    for (int i = 0; i < n; ++i) {
        const GENERAL_NAME* gn = sk_GENERAL_NAME_value(gens, i);
        if (!gn) continue;
        if (gn->type == GEN_DNS && gn->d.dNSName) {
            const unsigned char* p = ASN1_STRING_get0_data(gn->d.dNSName);
            const int l = ASN1_STRING_length(gn->d.dNSName);
            if (p && l > 0) {
                out->dns_sans.emplace_back(reinterpret_cast<const char*>(p),
                                           static_cast<std::size_t>(l));
            }
        } else if (gn->type == GEN_URI && gn->d.uniformResourceIdentifier) {
            const unsigned char* p =
                ASN1_STRING_get0_data(gn->d.uniformResourceIdentifier);
            const int l = ASN1_STRING_length(gn->d.uniformResourceIdentifier);
            if (p && l > 0) {
                std::string uri(reinterpret_cast<const char*>(p),
                                static_cast<std::size_t>(l));
                if (!out->spiffe_id && uri.rfind("spiffe://", 0) == 0) {
                    out->spiffe_id = uri;
                }
                out->uri_sans.push_back(std::move(uri));
            }
        }
    }
    GENERAL_NAMES_free(gens);
}

}  // namespace

std::optional<CertInfo> MtlsRegistry::ParsePem(const std::string& pem) {
    std::unique_ptr<BIO, BioFree> bio(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())));
    if (!bio) return std::nullopt;
    std::unique_ptr<X509, X509Free> cert(
        PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
    if (!cert) return std::nullopt;  // not a parseable cert
    CertInfo info;
    info.cn = SubjectCn(cert.get());
    CollectSans(cert.get(), &info);
    // A cert with neither a CN nor any SAN carries no usable identity.
    if (info.cn.empty() && info.dns_sans.empty() && info.uri_sans.empty()) {
        return std::nullopt;
    }
    return info;
}

#else  // !KVCACHE_HAVE_OPENSSL

// Fallback: CN-only string scan. dns_sans / uri_sans / spiffe_id stay
// empty — callers that need SANs must build with OpenSSL.
std::optional<CertInfo> MtlsRegistry::ParsePem(const std::string& pem) {
    static constexpr const char* kMarker = "CN=";
    const auto pos = pem.find(kMarker);
    if (pos == std::string::npos) return std::nullopt;
    const std::size_t start = pos + 3;
    const std::size_t end = pem.find_first_of(",/\n\r", start);
    std::string cn = (end == std::string::npos)
                         ? pem.substr(start)
                         : pem.substr(start, end - start);
    if (cn.empty()) return std::nullopt;
    CertInfo info;
    info.cn = std::move(cn);
    return info;
}

#endif  // KVCACHE_HAVE_OPENSSL

std::optional<std::string> MtlsRegistry::ExtractCnFromPem(const std::string& pem) {
    auto info = ParsePem(pem);
    if (!info || info->cn.empty()) return std::nullopt;
    return info->cn;
}

}  // namespace kvcache::node::security
