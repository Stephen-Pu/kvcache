// LLD §3.3 T3 — NVMe tier implementation (blocking pread/pwrite path).
#include "tier/nvme_tier.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kvcache::node::tier {

namespace {
ssize_t PwriteAll(int fd, const void* buf, std::size_t n, off_t off) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    std::size_t left = n;
    while (left) {
        ssize_t w = ::pwrite(fd, p, left, off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= w; p += w; off += w;
    }
    return n;
}
ssize_t PreadAll(int fd, void* buf, std::size_t n, off_t off) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    std::size_t left = n;
    while (left) {
        ssize_t r = ::pread(fd, p, left, off);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) return n - left;  // short read at EOF
        left -= r; p += r; off += r;
    }
    return n;
}
}  // namespace

std::unique_ptr<NvmeTier> NvmeTier::Create(const Options& opts, std::string* err) {
    if (opts.path.empty() || opts.pool_bytes == 0 || opts.slot_bytes == 0 ||
        opts.pool_bytes < opts.slot_bytes ||
        opts.pool_bytes % opts.slot_bytes != 0) {
        if (err) *err = "nvme_tier: invalid options";
        return nullptr;
    }

    int flags = O_RDWR | (opts.create_if_missing ? O_CREAT : 0);
    int fd = ::open(opts.path.c_str(), flags, 0644);
    if (fd < 0) {
        if (err) *err = std::string("nvme_tier: open failed: ") + std::strerror(errno);
        return nullptr;
    }

    // Pre-allocate the backing file. ftruncate is the portable choice; the
    // first slot write will fault in extents on POSIX-conforming filesystems.
    if (::ftruncate(fd, static_cast<off_t>(opts.pool_bytes)) != 0) {
        if (err) *err = std::string("nvme_tier: ftruncate: ") + std::strerror(errno);
        ::close(fd);
        return nullptr;
    }

    auto t = std::unique_ptr<NvmeTier>(new NvmeTier());
    t->fd_         = fd;
    t->path_       = opts.path;
    t->pool_bytes_ = opts.pool_bytes;
    t->slot_bytes_ = opts.slot_bytes;
    t->slot_count_ = static_cast<uint32_t>(opts.pool_bytes / opts.slot_bytes);
    t->fdatasync_  = opts.fdatasync_on_put;

    t->free_stack_.reserve(t->slot_count_);
    for (uint32_t i = t->slot_count_; i > 0; --i) {
        t->free_stack_.push_back(i - 1);
    }
    return t;
}

NvmeTier::~NvmeTier() {
    if (fd_ >= 0) ::close(fd_);
}

bool NvmeTier::Put(const DramKey& key, const uint8_t* data, std::size_t n,
                   std::string* err) {
    if (n == 0 || n > slot_bytes_) {
        if (err) *err = "nvme_tier: payload exceeds slot size";
        return false;
    }
    uint32_t slot;
    {
        std::lock_guard lk(mu_);
        // If key already present, reuse its slot (overwrite path).
        auto it = index_.find(key);
        if (it != index_.end()) {
            slot = it->second.slot_id;
            it->second.bytes = n;
        } else {
            if (free_stack_.empty()) {
                if (err) *err = "nvme_tier: pool full";
                return false;
            }
            slot = free_stack_.back();
            free_stack_.pop_back();
            index_.emplace(key, Entry{slot, n});
            in_use_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    const off_t off = static_cast<off_t>(slot) * static_cast<off_t>(slot_bytes_);
    if (PwriteAll(fd_, data, n, off) != static_cast<ssize_t>(n)) {
        if (err) *err = std::string("nvme_tier: pwrite: ") + std::strerror(errno);
        // Restore index on failure.
        std::lock_guard lk(mu_);
        auto it = index_.find(key);
        if (it != index_.end()) {
            free_stack_.push_back(it->second.slot_id);
            index_.erase(it);
            in_use_.fetch_sub(1, std::memory_order_relaxed);
        }
        return false;
    }
    if (fdatasync_) {
        // fdatasync is Linux-only; macOS falls back to fsync (slightly more
        // work — also flushes inode metadata — but functionally correct).
#if defined(__APPLE__)
        const int rc = ::fsync(fd_);
#else
        const int rc = ::fdatasync(fd_);
#endif
        if (rc != 0) {
            if (err) *err = std::string("nvme_tier: fdatasync: ") + std::strerror(errno);
            return false;
        }
    }
    return true;
}

bool NvmeTier::Get(const DramKey& key, uint8_t* dst, std::size_t dst_capacity,
                   std::size_t* out_bytes, std::string* err) const {
    Entry e;
    {
        std::lock_guard lk(mu_);
        auto it = index_.find(key);
        if (it == index_.end()) {
            if (err) *err = "nvme_tier: not found";
            return false;
        }
        e = it->second;
    }
    if (dst_capacity < e.bytes) {
        if (err) *err = "nvme_tier: destination buffer too small";
        return false;
    }
    const off_t off = static_cast<off_t>(e.slot_id) * static_cast<off_t>(slot_bytes_);
    if (PreadAll(fd_, dst, e.bytes, off) != static_cast<ssize_t>(e.bytes)) {
        if (err) *err = std::string("nvme_tier: pread: ") + std::strerror(errno);
        return false;
    }
    if (out_bytes) *out_bytes = e.bytes;
    return true;
}

bool NvmeTier::Get(const DramKey& key, std::vector<uint8_t>* out, std::string* err) const {
    Entry e;
    {
        std::lock_guard lk(mu_);
        auto it = index_.find(key);
        if (it == index_.end()) {
            if (err) *err = "nvme_tier: not found";
            return false;
        }
        e = it->second;
    }
    out->resize(e.bytes);
    std::size_t got = 0;
    return Get(key, out->data(), out->size(), &got, err);
}

bool NvmeTier::Erase(const DramKey& key) {
    std::lock_guard lk(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) return false;
    free_stack_.push_back(it->second.slot_id);
    index_.erase(it);
    in_use_.fetch_sub(1, std::memory_order_relaxed);
    return true;
}

bool NvmeTier::Contains(const DramKey& key) const {
    std::lock_guard lk(mu_);
    return index_.find(key) != index_.end();
}

uint64_t NvmeTier::UsedBytes() const noexcept {
    std::lock_guard lk(mu_);
    uint64_t total = 0;
    for (const auto& [_, e] : index_) total += e.bytes;
    return total;
}

}  // namespace kvcache::node::tier
