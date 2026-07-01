// LLD §3.3 T3 — NVMe tier. Blocking pread/pwrite is always available;
// the io_uring path lives behind KVCACHE_ENABLE_URING (Phase B1).
#include "tier/nvme_tier.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef KVCACHE_ENABLE_URING
#  include <liburing.h>
#endif

#if KVCACHE_HAVE_SPDK
#  include <spdk/env.h>
#  include <spdk/nvme.h>
#endif

#include "logging.h"  // Phase B1 — Warn at Create() when use_uring is
                      // ignored because the build doesn't include it.

namespace kvcache::node::tier {

// ----- UringImpl --------------------------------------------------------
//
// Defined unconditionally so the header's std::unique_ptr<UringImpl>
// dtor has a complete type at every TU boundary. On non-uring builds
// the body is just a placeholder — Create() will never construct one.
//
// Phase B1.1 — async path:
//   * submit_mu is held ONLY around io_uring_get_sqe + prep + submit.
//     Brief — no waiting under the lock.
//   * Each call heap-allocates a Completion holding a std::promise<int>
//     and tags the SQE's user_data with its address.
//   * A dedicated reaper thread blocks on io_uring_wait_cqe and, on
//     each CQE, recovers the Completion, sets the promise with
//     cqe->res, and decrements in_flight. Callers wait on the future
//     — no syscalls, just a condvar.
//   * On shutdown the dtor flips running=false, submits a NOP SQE
//     tagged with a static sentinel address, and joins the reaper.
//     The reaper drains pending real I/O before exiting (loop
//     condition: running || in_flight > 0).

#ifdef KVCACHE_ENABLE_URING
#include <future>
#endif

struct NvmeTier::UringImpl {
#ifdef KVCACHE_ENABLE_URING
    struct io_uring                ring;
    std::mutex                     submit_mu;     // SQE alloc + submit
    std::atomic<bool>              running{true};
    std::atomic<uint64_t>          in_flight{0};
    std::atomic<uint64_t>          peak_in_flight{0};  // observability
    std::thread                    reaper;
    bool                           ring_init = false;

    struct Completion {
        std::promise<int> p;  // resolved with cqe->res by the reaper
    };

    // Distinct address used as the NOP SQE's user_data on shutdown.
    // Reaper compares cqe user_data to &wake_sentinel_ to recognise
    // the shutdown wake (a heap Completion's address can't collide
    // because static storage and heap don't overlap).
    static inline int wake_sentinel_ = 0;

    void StartReaper() {
        reaper = std::thread([this] {
            for (;;) {
                struct io_uring_cqe* cqe = nullptr;
                int rc = io_uring_wait_cqe(&ring, &cqe);
                if (rc < 0) {
                    // EINTR / shutdown — re-check loop condition.
                    if (!running.load(std::memory_order_acquire) &&
                        in_flight.load(std::memory_order_acquire) == 0) {
                        return;
                    }
                    continue;
                }
                void* ud = io_uring_cqe_get_data(cqe);
                if (ud == static_cast<void*>(&wake_sentinel_)) {
                    io_uring_cqe_seen(&ring, cqe);
                    // Wake-up NOP: re-check the loop condition.
                    if (!running.load(std::memory_order_acquire) &&
                        in_flight.load(std::memory_order_acquire) == 0) {
                        return;
                    }
                    continue;
                }
                auto* c = static_cast<Completion*>(ud);
                const int res = cqe->res;
                io_uring_cqe_seen(&ring, cqe);
                c->p.set_value(res);
                // Decrement AFTER set_value so the caller's
                // future.get() return is sequenced after the counter
                // drop — keeps the shutdown drain invariant clean.
                in_flight.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }

    ~UringImpl() {
        if (!ring_init) return;
        // Stop accepting new I/O.
        running.store(false, std::memory_order_release);
        // Wake the reaper with a NOP so it gets out of wait_cqe and
        // re-checks the loop condition. If real I/O is still pending,
        // the reaper drains it (loop guard: running || in_flight > 0).
        {
            std::lock_guard lk(submit_mu);
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            if (sqe) {
                io_uring_prep_nop(sqe);
                io_uring_sqe_set_data(sqe, static_cast<void*>(&wake_sentinel_));
                (void)io_uring_submit(&ring);
            }
        }
        if (reaper.joinable()) reaper.join();
        io_uring_queue_exit(&ring);
    }

    bool DoOp(int fd, void* buf, std::size_t n, off_t off, bool is_write,
              std::string* err) {
        if (!running.load(std::memory_order_acquire)) {
            if (err) *err = "nvme_tier(uring): tier is shutting down";
            return false;
        }
        // Heap-allocate so the address is stable across the submit /
        // reap rendezvous. The reaper deletes it after set_value.
        // Actually — we own it on the caller side and pass by pointer.
        // Simpler: stack-allocate; the future lives at least until
        // fut.get() returns, which is after the reaper signalled.
        Completion c;
        auto fut = c.p.get_future();
        {
            std::lock_guard lk(submit_mu);
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                if (err) *err = "nvme_tier(uring): no SQE available";
                return false;
            }
            if (is_write) {
                io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(n),
                                     static_cast<__u64>(off));
            } else {
                io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(n),
                                    static_cast<__u64>(off));
            }
            io_uring_sqe_set_data(sqe, static_cast<void*>(&c));
            // Bump in_flight BEFORE submit so a fast-completing CQE
            // can't make the reaper see an underflow. The matching
            // decrement happens in the reaper on CQE.
            const uint64_t now = in_flight.fetch_add(1, std::memory_order_acq_rel) + 1;
            // Track peak for observability (single-writer-on-the-fast-
            // path; relaxed CAS is fine).
            uint64_t peak = peak_in_flight.load(std::memory_order_relaxed);
            while (peak < now && !peak_in_flight.compare_exchange_weak(
                       peak, now, std::memory_order_relaxed)) {}
            int rc = io_uring_submit(&ring);
            if (rc < 0) {
                in_flight.fetch_sub(1, std::memory_order_acq_rel);
                if (err) *err = std::string("nvme_tier(uring): submit: ")
                              + std::strerror(-rc);
                return false;
            }
        }
        // Block on the reaper. No syscall — just a condvar inside the
        // promise machinery.
        const int res = fut.get();
        if (res < 0) {
            if (err) *err = std::string("nvme_tier(uring): op: ")
                          + std::strerror(-res);
            return false;
        }
        if (static_cast<std::size_t>(res) != n) {
            if (err) {
                char buf2[128];
                std::snprintf(buf2, sizeof(buf2),
                              "nvme_tier(uring): short I/O: %d of %zu bytes",
                              res, n);
                *err = buf2;
            }
            return false;
        }
        return true;
    }
#endif
};

// ----- SpdkImpl (Phase A3) ----------------------------------------------
//
// User-space NVMe via SPDK's low-level driver API (spdk_nvme_*), NOT the
// bdev/app framework — so no reactor/poller thread is needed. One IO qpair;
// Put/Get submit a single command and poll it to completion inline. The qpair
// is not thread-safe, so io_mu serialises all device I/O. spdk_env_init is a
// process singleton (guarded by a once_flag). Defined unconditionally so the
// header's unique_ptr<SpdkImpl> dtor has a complete type; the body is gated.
struct NvmeTier::SpdkImpl {
#if KVCACHE_HAVE_SPDK
    spdk_nvme_ctrlr* ctrlr       = nullptr;
    spdk_nvme_ns*    ns          = nullptr;
    spdk_nvme_qpair* qpair       = nullptr;
    uint32_t         sector_size = 0;
    std::mutex       io_mu;

    struct CbArg { int done = 0; bool error = false; };
    static void OpDone(void* arg, const struct spdk_nvme_cpl* cpl) {
        auto* a = static_cast<CbArg*>(arg);
        a->error = spdk_nvme_cpl_is_error(cpl);
        a->done  = 1;
    }
    // Attach the first controller the probe surfaces (we probed a specific
    // PCIe traddr, so there's exactly one).
    static bool ProbeCb(void*, const struct spdk_nvme_transport_id*,
                        struct spdk_nvme_ctrlr_opts*) { return true; }
    static void AttachCb(void* cb_ctx, const struct spdk_nvme_transport_id*,
                         struct spdk_nvme_ctrlr* c,
                         const struct spdk_nvme_ctrlr_opts*) {
        auto* self = static_cast<SpdkImpl*>(cb_ctx);
        if (!self->ctrlr) self->ctrlr = c;
    }

    ~SpdkImpl() {
        if (qpair) spdk_nvme_ctrlr_free_io_qpair(qpair);
        if (ctrlr) {
            struct spdk_nvme_detach_ctx* dctx = nullptr;
            spdk_nvme_detach_async(ctrlr, &dctx);
            if (dctx) { while (spdk_nvme_detach_poll_async(dctx) == -EAGAIN) {} }
        }
    }

    bool Init(const std::string& pci_addr, std::string* err) {
        static std::once_flag env_once;
        static bool env_ok = false;
        std::call_once(env_once, [] {
            struct spdk_env_opts opts;
            spdk_env_opts_init(&opts);
            opts.name = "kvcache_nvme_tier";
            env_ok = (spdk_env_init(&opts) == 0);
        });
        if (!env_ok) { if (err) *err = "nvme_tier(spdk): spdk_env_init failed"; return false; }

        struct spdk_nvme_transport_id trid;
        std::memset(&trid, 0, sizeof(trid));
        spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
        std::snprintf(trid.traddr, sizeof(trid.traddr), "%s", pci_addr.c_str());

        if (spdk_nvme_probe(&trid, this, ProbeCb, AttachCb, nullptr) != 0 || !ctrlr) {
            if (err) *err = "nvme_tier(spdk): probe/attach failed for '" + pci_addr + "'";
            return false;
        }
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, 1);
        if (!ns || !spdk_nvme_ns_is_active(ns)) {
            if (err) *err = "nvme_tier(spdk): namespace 1 not active";
            return false;
        }
        sector_size = spdk_nvme_ns_get_sector_size(ns);
        qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, nullptr, 0);
        if (!qpair) { if (err) *err = "nvme_tier(spdk): alloc_io_qpair failed"; return false; }
        return true;
    }

    // Read/write `n` bytes at device byte-offset `byte_off` (must be sector-
    // aligned). Uses a DMA bounce buffer sized up to the sector; short tails
    // are zero-padded on write and truncated on read.
    bool DoOp(uint64_t byte_off, void* buf, std::size_t n, bool is_write,
              std::string* err) {
        std::lock_guard<std::mutex> lk(io_mu);
        if (sector_size == 0 || byte_off % sector_size != 0) {
            if (err) *err = "nvme_tier(spdk): unaligned/zero-sector offset";
            return false;
        }
        const uint64_t lba = byte_off / sector_size;
        const uint32_t lba_count =
            static_cast<uint32_t>((n + sector_size - 1) / sector_size);
        const std::size_t dma_len =
            static_cast<std::size_t>(lba_count) * sector_size;
        void* dma = spdk_dma_zmalloc(dma_len, sector_size, nullptr);
        if (!dma) { if (err) *err = "nvme_tier(spdk): dma_zmalloc failed"; return false; }
        if (is_write) std::memcpy(dma, buf, n);

        CbArg cb;
        const int rc = is_write
            ? spdk_nvme_ns_cmd_write(ns, qpair, dma, lba, lba_count, OpDone, &cb, 0)
            : spdk_nvme_ns_cmd_read(ns, qpair, dma, lba, lba_count, OpDone, &cb, 0);
        if (rc != 0) {
            spdk_dma_free(dma);
            if (err) *err = "nvme_tier(spdk): submit failed";
            return false;
        }
        while (cb.done == 0) spdk_nvme_qpair_process_completions(qpair, 0);
        const bool ok = !cb.error;
        if (ok && !is_write) std::memcpy(buf, dma, n);
        spdk_dma_free(dma);
        if (!ok && err) *err = "nvme_tier(spdk): I/O completion error";
        return ok;
    }
#endif
};

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
    // path is the backing file for the blocking/uring paths; the SPDK path
    // addresses the device directly and needs spdk_pci_addr instead.
    if ((!opts.use_spdk && opts.path.empty()) ||
        opts.pool_bytes == 0 || opts.slot_bytes == 0 ||
        opts.pool_bytes < opts.slot_bytes ||
        opts.pool_bytes % opts.slot_bytes != 0) {
        if (err) *err = "nvme_tier: invalid options";
        return nullptr;
    }

    // Open + pre-allocate the backing file — but NOT for the SPDK path, which
    // bypasses the filesystem and writes device LBAs directly.
    int fd = -1;
    if (!opts.use_spdk) {
        int flags = O_RDWR | (opts.create_if_missing ? O_CREAT : 0);
        fd = ::open(opts.path.c_str(), flags, 0644);
        if (fd < 0) {
            if (err) *err = std::string("nvme_tier: open failed: ") + std::strerror(errno);
            return nullptr;
        }
        // ftruncate is the portable choice; the first slot write faults in
        // extents on POSIX-conforming filesystems.
        if (::ftruncate(fd, static_cast<off_t>(opts.pool_bytes)) != 0) {
            if (err) *err = std::string("nvme_tier: ftruncate: ") + std::strerror(errno);
            ::close(fd);
            return nullptr;
        }
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

    // Phase A3 — wire the SPDK user-space NVMe backend when asked + available.
    // Takes precedence over io_uring (both can't own the device).
    if (opts.use_spdk) {
#if KVCACHE_HAVE_SPDK
        if (opts.spdk_pci_addr.empty()) {
            if (err) *err = "nvme_tier(spdk): use_spdk=true requires spdk_pci_addr";
            return nullptr;
        }
        auto s = std::make_unique<SpdkImpl>();
        if (!s->Init(opts.spdk_pci_addr, err)) return nullptr;  // *err set
        // Slot offsets must be sector-aligned for direct LBA I/O.
        if (opts.slot_bytes % s->sector_size != 0) {
            if (err) *err = "nvme_tier(spdk): slot_bytes must be a multiple of "
                            "the device sector size";
            return nullptr;
        }
        t->spdk_ = std::move(s);
        return t;  // SPDK path owns the device; skip the uring/file wiring.
#else
        ::kvcache::log::Get("nvme_tier").Warn(
            "Options::use_spdk=true ignored: built without "
            "KVCACHE_ENABLE_SPDK — falling back to blocking/uring file path");
#endif
    }

    // Phase B1 — wire io_uring when asked + available.
    if (opts.use_uring) {
#ifdef KVCACHE_ENABLE_URING
        auto u = std::make_unique<UringImpl>();
        int rc = io_uring_queue_init(opts.uring_queue_depth, &u->ring,
                                       /*flags=*/0);
        if (rc < 0) {
            if (err) *err = std::string("nvme_tier(uring): queue_init: ")
                          + std::strerror(-rc);
            // ftruncate + open already happened — let the std::unique_ptr
            // dtor clean fd_ via ~NvmeTier when we return nullptr.
            return nullptr;
        }
        u->ring_init = true;
        u->StartReaper();
        t->uring_ = std::move(u);
#else
        // Build doesn't include io_uring; honour the request as a warn
        // rather than a hard fail so config can be uniform across hosts
        // (Linux gets the fast path, others get the blocking path).
        ::kvcache::log::Get("nvme_tier").Warn(
            "Options::use_uring=true ignored: built without "
            "KVCACHE_ENABLE_URING — falling back to blocking pread/pwrite");
#endif
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
    bool write_ok = false;
#if KVCACHE_HAVE_SPDK
    if (spdk_) {
        write_ok = spdk_->DoOp(static_cast<uint64_t>(off),
                               const_cast<uint8_t*>(data), n, /*is_write=*/true, err);
    } else
#endif
#ifdef KVCACHE_ENABLE_URING
    if (uring_) {
        // const_cast: io_uring_prep_write takes void*, but we never
        // mutate the buffer — the kernel only reads it.
        write_ok = uring_->DoOp(fd_, const_cast<uint8_t*>(data), n, off,
                                  /*is_write=*/true, err);
    } else
#endif
    {
        write_ok = (PwriteAll(fd_, data, n, off) == static_cast<ssize_t>(n));
        if (!write_ok && err) {
            *err = std::string("nvme_tier: pwrite: ") + std::strerror(errno);
        }
    }
    if (!write_ok) {
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
    if (fdatasync_ && fd_ >= 0) {  // no fd on the SPDK path
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
#if KVCACHE_HAVE_SPDK
    if (spdk_) {
        if (!spdk_->DoOp(static_cast<uint64_t>(off), dst, e.bytes,
                         /*is_write=*/false, err)) {
            return false;
        }
    } else
#endif
#ifdef KVCACHE_ENABLE_URING
    if (uring_) {
        if (!uring_->DoOp(fd_, dst, e.bytes, off, /*is_write=*/false, err)) {
            return false;
        }
    } else
#endif
    {
        if (PreadAll(fd_, dst, e.bytes, off) != static_cast<ssize_t>(e.bytes)) {
            if (err) *err = std::string("nvme_tier: pread: ") + std::strerror(errno);
            return false;
        }
    }
    if (out_bytes) *out_bytes = e.bytes;
    return true;
}

bool NvmeTier::UsingUring() const noexcept {
    return uring_ != nullptr;
}

bool NvmeTier::UsingSpdk() const noexcept {
    return spdk_ != nullptr;
}

uint64_t NvmeTier::UringPeakInFlight() const noexcept {
#ifdef KVCACHE_ENABLE_URING
    if (uring_) return uring_->peak_in_flight.load(std::memory_order_relaxed);
#endif
    return 0;
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
