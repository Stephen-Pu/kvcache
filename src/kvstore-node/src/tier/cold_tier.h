// LLD §3.3 T4 — Cold tier via a pluggable multi-cloud UFS.
//
// The cold tier is the only tier that crosses cloud boundaries. We outsource
// the multi-cloud problem to a pluggable UFS — see "Vendor-Neutral /
// UFS-as-Enabler" framing in HLD §2.3.
//
// MVP integration strategy:
//   * Abstract interface (IColdTier) — Put / Get / Delete / Exists.
//   * Concrete `FilesystemColdTier` — backed by a directory path. This is
//     intentionally generic: the same implementation works for
//       (a) local disk staging (tests, dev)
//       (b) a multi-cloud UFS mounted via FUSE (production default)
//       (c) any POSIX-mounted UFS
//   * Native REST / HTTP object-store client — `RestColdTier` (Phase B3,
//     rest_cold_tier.h), for hosts without a FUSE mount or when the data
//     plane should own the UFS connection directly.
//
// Layout under the root directory:
//   {root}/{first_2_hex(key)}/{rest_of_hex(key)}.kv
// The 2-hex shard byte keeps any single directory under ~65k files, which is
// well within ext4 / xfs single-directory comfort.
//
// Compression / encryption live in a pluggable middleware layer wrapped
// around the IColdTier interface, selected via ColdTierOptions:
//   * compression — `CompressingColdTier` (Phase B3.1): identity / zstd.
//   * encryption  — `EncryptingColdTier` (Phase B3.2): AES-256-GCM.
// Stacked compress-outer / encrypt-inner so data is compressed then sealed.
// (Future cut: per-blob KMS-envelope key rotation; SigV4 transport decorator
// for direct AWS S3 — see rest_cold_tier.h.)
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "security/boundary_guard.h"  // BoundaryGuard, Endpoint
#include "tier/dram_tier.h"           // DramKey reused as content key

namespace kvcache::metrics { class Registry; }  // Phase O-4 — cold metrics

namespace kvcache::node::tier {

class IColdTier {
   public:
    virtual ~IColdTier() = default;

    virtual std::string Name() const = 0;

    virtual bool Put   (const DramKey&, const uint8_t* data, std::size_t n,
                        std::string* err) = 0;
    virtual bool Get   (const DramKey&, std::vector<uint8_t>* out,
                        std::string* err) = 0;
    virtual bool Delete(const DramKey&, std::string* err) = 0;
    virtual bool Exists(const DramKey&) const = 0;
};

// Filesystem-backed cold tier. Works with any POSIX path — local disk,
// FUSE-mounted UFS, or another UFS mount.
class FilesystemColdTier final : public IColdTier {
   public:
    struct Options {
        std::string root;                         // required
        bool        create_if_missing = true;
        bool        fsync_on_put      = true;
    };

    static std::unique_ptr<FilesystemColdTier> Create(const Options& opts,
                                                       std::string* err);
    ~FilesystemColdTier() override = default;

    std::string Name() const override { return "filesystem"; }

    bool Put   (const DramKey&, const uint8_t* data, std::size_t n, std::string* err) override;
    bool Get   (const DramKey&, std::vector<uint8_t>* out, std::string* err) override;
    bool Delete(const DramKey&, std::string* err) override;
    bool Exists(const DramKey&) const override;

   private:
    FilesystemColdTier() = default;

    // Resolve a key to its on-disk path. The shard directory is created lazily
    // on first Put for that shard.
    std::string PathFor(const DramKey& key, bool ensure_shard_exists) const;

    Options opts_;
};

// Factory for selecting a backend by name.
//   "fs" / "fuse-mount" -> FilesystemColdTier (POSIX path / FUSE mount)
//   "native-rest"       -> RestColdTier       (direct HTTP object store; B3)
struct ColdTierOptions {
    std::string                 type = "fs";
    FilesystemColdTier::Options fs;
    // REST backend options. A self-contained POD mirror of
    // RestColdTier::Options — kept here (rather than including
    // rest_cold_tier.h) so this header stays free of the transport seam.
    // Only set/read when type == "native-rest".
    struct Rest {
        std::string base_url;
        std::string key_prefix = "kvcache/";
        std::string bearer_token;
        std::string ca_pem_path;
        std::string client_cert_pem_path;
        std::string client_key_pem_path;
        long        timeout_ms = 30000;
    } rest;
    // Compression middleware (Phase B3.1). When codec != "none" the selected
    // backend is wrapped in a CompressingColdTier. "none"|"identity"|"zstd"
    // ("zstd" requires a KVCACHE_ENABLE_ZSTD build).
    struct Compression {
        std::string codec = "none";
        int         level = 3;  // zstd level (ignored by identity)
    } compression;
    // Encryption middleware (Phase B3.2). When enabled the backend is wrapped
    // in an EncryptingColdTier (AES-256-GCM; requires a KVCACHE_HAVE_OPENSSL
    // build). `key` must be exactly 32 bytes. Stacked INSIDE compression, so
    // data is compressed then encrypted.
    struct Encryption {
        bool                 enabled = false;
        std::vector<uint8_t> key;  // 32 bytes when enabled
    } encryption;
    // Observability (Phase O-4). When non-null, the built tier is wrapped
    // OUTERMOST in a MetricsColdTier that records kv_cold_* counters to this
    // registry. nullptr = no metrics (zero overhead).
    metrics::Registry* metrics_registry = nullptr;

    // A10 — Regulated Mode egress guard (optional).
    // When set, the native-rest backend's HTTP transport is wrapped in a
    // GuardedHttpTransport so every object-store request is boundary-checked
    // before dialing. Unset (nullptr) => unchanged behavior (no wrapping).
    std::shared_ptr<const security::BoundaryGuard> guard;          // default nullptr
    std::function<void(const security::Endpoint&, std::string_view)>
                                                   deny_observer;  // default empty
};
std::unique_ptr<IColdTier> CreateColdTier(const ColdTierOptions& opts, std::string* err);

}  // namespace kvcache::node::tier
