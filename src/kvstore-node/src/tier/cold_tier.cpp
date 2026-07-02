// LLD §3.3 T4 — FilesystemColdTier implementation.
#include "tier/cold_tier.h"

#include "tier/rest_cold_tier.h"          // B3  — native-rest backend
#include "tier/guarded_transport.h"       // A10 — egress boundary guard decorator
#include "tier/compressing_cold_tier.h"   // B3.1 — compression middleware
#include "tier/encrypting_cold_tier.h"    // B3.2 — encryption middleware
#include "tier/metrics_cold_tier.h"       // O-4  — observability middleware
#include "tier/block_codec.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <unistd.h>

namespace kvcache::node::tier {

namespace {

std::string HexOf(const DramKey& k) {
    static const char* kHex = "0123456789abcdef";
    std::string s(k.bytes.size() * 2, '0');
    for (std::size_t i = 0; i < k.bytes.size(); ++i) {
        s[2 * i]     = kHex[(k.bytes[i] >> 4) & 0xf];
        s[2 * i + 1] = kHex[(k.bytes[i]     ) & 0xf];
    }
    return s;
}

ssize_t WriteAll(int fd, const void* buf, std::size_t n) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    std::size_t left = n;
    while (left) {
        ssize_t w = ::write(fd, p, left);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        left -= w; p += w;
    }
    return n;
}

}  // namespace

std::unique_ptr<FilesystemColdTier> FilesystemColdTier::Create(
    const Options& opts, std::string* err) {
    if (opts.root.empty()) {
        if (err) *err = "cold_tier/fs: root path is empty";
        return nullptr;
    }
    std::error_code ec;
    if (opts.create_if_missing) {
        std::filesystem::create_directories(opts.root, ec);
        if (ec) {
            if (err) *err = "cold_tier/fs: mkdir: " + ec.message();
            return nullptr;
        }
    } else if (!std::filesystem::is_directory(opts.root, ec)) {
        if (err) *err = "cold_tier/fs: root is not a directory";
        return nullptr;
    }
    auto t = std::unique_ptr<FilesystemColdTier>(new FilesystemColdTier());
    t->opts_ = opts;
    return t;
}

std::string FilesystemColdTier::PathFor(const DramKey& key,
                                         bool ensure_shard_exists) const {
    const std::string hex = HexOf(key);
    const std::string shard = hex.substr(0, 2);
    const std::string tail  = hex.substr(2);
    if (ensure_shard_exists) {
        std::error_code ec;
        std::filesystem::create_directory(opts_.root + "/" + shard, ec);
        // ignore "already exists"; other errors surface at open()
    }
    return opts_.root + "/" + shard + "/" + tail + ".kv";
}

bool FilesystemColdTier::Put(const DramKey& key, const uint8_t* data,
                             std::size_t n, std::string* err) {
    const std::string final_path = PathFor(key, /*ensure_shard_exists=*/true);
    const std::string tmp_path   = final_path + ".tmp";

    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        if (err) *err = std::string("cold_tier/fs: open tmp: ") + std::strerror(errno);
        return false;
    }
    if (WriteAll(fd, data, n) != static_cast<ssize_t>(n)) {
        if (err) *err = std::string("cold_tier/fs: write: ") + std::strerror(errno);
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return false;
    }
    if (opts_.fsync_on_put && ::fsync(fd) != 0) {
        if (err) *err = std::string("cold_tier/fs: fsync: ") + std::strerror(errno);
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return false;
    }
    ::close(fd);
    if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        if (err) *err = std::string("cold_tier/fs: rename: ") + std::strerror(errno);
        ::unlink(tmp_path.c_str());
        return false;
    }
    return true;
}

bool FilesystemColdTier::Get(const DramKey& key, std::vector<uint8_t>* out,
                             std::string* err) {
    const std::string p = PathFor(key, /*ensure_shard_exists=*/false);
    int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) {
        if (err) *err = std::string("cold_tier/fs: open: ") + std::strerror(errno);
        return false;
    }
    struct stat st;
    if (::fstat(fd, &st) != 0) {
        if (err) *err = std::string("cold_tier/fs: fstat: ") + std::strerror(errno);
        ::close(fd);
        return false;
    }
    out->resize(static_cast<std::size_t>(st.st_size));
    uint8_t* p_out = out->data();
    std::size_t left = out->size();
    while (left) {
        ssize_t r = ::read(fd, p_out, left);
        if (r < 0) {
            if (errno == EINTR) continue;
            if (err) *err = std::string("cold_tier/fs: read: ") + std::strerror(errno);
            ::close(fd);
            return false;
        }
        if (r == 0) break;
        left -= r; p_out += r;
    }
    ::close(fd);
    return left == 0;
}

bool FilesystemColdTier::Delete(const DramKey& key, std::string* err) {
    const std::string p = PathFor(key, false);
    if (::unlink(p.c_str()) != 0) {
        if (errno == ENOENT) return false;
        if (err) *err = std::string("cold_tier/fs: unlink: ") + std::strerror(errno);
        return false;
    }
    return true;
}

bool FilesystemColdTier::Exists(const DramKey& key) const {
    const std::string p = PathFor(key, false);
    struct stat st;
    return ::stat(p.c_str(), &st) == 0;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

namespace {

// Build the base backend selected by opts.type, before any middleware.
std::unique_ptr<IColdTier> CreateBaseColdTier(const ColdTierOptions& opts,
                                              std::string* err) {
    if (opts.type == "fs" || opts.type == "fuse-mount") {
        // fuse-mount is "just" a POSIX mount; same impl.
        return FilesystemColdTier::Create(opts.fs, err);
    }
    if (opts.type == "native-rest") {
        // B3 — direct HTTP object-store / REST UFS client.
        RestColdTier::Options ro;
        ro.base_url             = opts.rest.base_url;
        ro.key_prefix           = opts.rest.key_prefix;
        ro.bearer_token         = opts.rest.bearer_token;
        ro.ca_pem_path          = opts.rest.ca_pem_path;
        ro.client_cert_pem_path = opts.rest.client_cert_pem_path;
        ro.client_key_pem_path  = opts.rest.client_key_pem_path;
        ro.timeout_ms           = opts.rest.timeout_ms;
        if (!opts.guard) {
            // Null guard: delegate to the standard factory path unchanged.
            return RestColdTier::Create(ro, err);
        }
        // A10 — Regulated Mode: wrap the transport in a GuardedHttpTransport
        // so every object-store request is boundary-checked before dialing.
        std::shared_ptr<IHttpTransport> transport = MakeCurlHttpTransport();
        transport = std::make_shared<GuardedHttpTransport>(
            std::move(transport), opts.guard, opts.deny_observer);
        return RestColdTier::CreateWithTransport(ro, std::move(transport), err);
    }
    if (err) *err = "cold_tier: unknown backend type '" + opts.type + "'";
    return nullptr;
}

}  // namespace

std::unique_ptr<IColdTier> CreateColdTier(const ColdTierOptions& opts, std::string* err) {
    std::unique_ptr<IColdTier> tier = CreateBaseColdTier(opts, err);
    if (!tier) return nullptr;

    // B3.2 — optional encryption, wrapped FIRST (innermost) so that with
    // compression also on the data path is compress-then-encrypt: the
    // compressor (outer) shrinks plaintext before the encryptor (inner)
    // seals it, since ciphertext doesn't compress.
    if (opts.encryption.enabled) {
        EncryptingColdTier::Options eo;
        eo.key = opts.encryption.key;
        tier = EncryptingColdTier::Create(std::move(tier), eo, err);
        if (!tier) return nullptr;  // bad key / no OpenSSL — *err set
    }

    // B3.1 — optional compression middleware (outermost). "none" leaves the
    // tier as-is (zero overhead); any other codec wraps it.
    if (!opts.compression.codec.empty() &&
        opts.compression.codec != "none") {
        auto codec = MakeCodec(opts.compression.codec, opts.compression.level, err);
        if (!codec) return nullptr;  // unknown / uncompiled codec — *err set
        tier = CompressingColdTier::Create(std::move(tier), std::move(codec), err);
        if (!tier) return nullptr;
    }

    // O-4 — optional observability, wrapped OUTERMOST so its counters report
    // logical API-level ops (above compression/encryption byte transforms).
    if (opts.metrics_registry) {
        tier = MetricsColdTier::Create(std::move(tier), *opts.metrics_registry, err);
        if (!tier) return nullptr;
    }
    return tier;
}

}  // namespace kvcache::node::tier
