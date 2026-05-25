// Phase K-5 — BloomPublisher implementation.
#include "cluster/bloom_publisher.h"

#include <cstring>

namespace kvcache::node::cluster {

namespace {

routing::BloomParams ResolveParams(const BloomPublisher::Options& o) {
    return routing::BloomParams::ForCapacity(o.expected_chunks, o.target_fpr);
}

}  // namespace

BloomPublisher::BloomPublisher(IEtcdClient* etcd, const Options& opts)
    : etcd_(etcd), opts_(opts), bloom_(ResolveParams(opts)) {
    key_ = opts_.key_prefix + opts_.node_id;
}

BloomPublisher::~BloomPublisher() { Stop(); }

void BloomPublisher::Add(std::span<const uint8_t> key) { bloom_.Add(key); }

void BloomPublisher::AddTokens(uint64_t        tenant_hash,
                                uint64_t        model_hash,
                                const uint32_t* tokens,
                                std::size_t     n) {
    const auto k = SketchKeyForTokens(tenant_hash, model_hash, tokens, n);
    bloom_.Add(k);
}

std::vector<uint8_t> SketchKeyForTokens(uint64_t        tenant_hash,
                                         uint64_t        model_hash,
                                         const uint32_t* tokens,
                                         std::size_t     n) {
    std::vector<uint8_t> out;
    out.reserve(16 + n * 4);
    auto append_u64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
        }
    };
    auto append_u32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
        }
    };
    append_u64(tenant_hash);
    append_u64(model_hash);
    for (std::size_t i = 0; i < n; ++i) append_u32(tokens[i]);
    return out;
}

bool BloomPublisher::Start(std::string* err) {
    if (running_.exchange(true)) return true;
    if (!etcd_ || opts_.node_id.empty()) {
        if (err) *err = "bloom_publisher: incomplete config";
        running_.store(false);
        return false;
    }
    // First publish synchronously so a peer's seed-GetPrefix sees a
    // non-empty sketch from this node as soon as Start returns.
    if (!PublishNow(err)) {
        running_.store(false);
        return false;
    }
    stop_.store(false);
    thread_ = std::thread([this] { PublishLoop(); });
    return true;
}

void BloomPublisher::Stop() {
    if (!running_.exchange(false)) return;
    {
        std::lock_guard lk(cv_mu_);
        stop_.store(true);
        cv_.notify_all();
    }
    if (thread_.joinable()) thread_.join();
}

bool BloomPublisher::PublishNow(std::string* err) {
    if (!etcd_) {
        if (err) *err = "bloom_publisher: null etcd";
        return false;
    }
    const auto blob = EncodeSnapshot();
    const std::string value(blob.begin(), blob.end());
    if (!etcd_->Put(key_, value, opts_.lease, nullptr, err)) return false;
    publish_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

std::vector<uint8_t> BloomPublisher::EncodeSnapshot() const {
    const auto raw   = bloom_.Snapshot();
    const auto par   = bloom_.Params();
    std::vector<uint8_t> out;
    out.reserve(8 + raw.size());
    auto append32 = [&](uint32_t v) {
        // Little-endian.
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
        }
    };
    append32(par.m_bits);
    append32(par.k_hashes);
    out.insert(out.end(), raw.begin(), raw.end());
    return out;
}

void BloomPublisher::PublishLoop() {
    while (!stop_.load()) {
        {
            std::unique_lock lk(cv_mu_);
            if (cv_.wait_for(lk, opts_.publish_period,
                             [&] { return stop_.load(); })) {
                return;
            }
        }
        std::string ignore;
        // Non-fatal: a single failed PUT just delays peers by one
        // period. Next tick retries with a fresh snapshot.
        (void)PublishNow(&ignore);
    }
}

bool DecodeBloomSnapshot(const std::string&    encoded,
                          routing::BloomParams* out_params,
                          std::vector<uint8_t>* out_bytes) {
    if (encoded.size() < 8) return false;
    auto read32 = [&](std::size_t off) -> uint32_t {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= static_cast<uint32_t>(
                static_cast<uint8_t>(encoded[off + i])) << (i * 8);
        }
        return v;
    };
    const uint32_t m  = read32(0);
    const uint32_t k  = read32(4);
    if (m == 0 || k == 0) return false;
    if (encoded.size() < 8u + (m + 7) / 8) return false;
    if (out_params) *out_params = routing::BloomParams{m, k};
    if (out_bytes) {
        out_bytes->assign(encoded.begin() + 8, encoded.end());
    }
    return true;
}

}  // namespace kvcache::node::cluster
