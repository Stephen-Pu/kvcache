// LLD §3.5 D-NET-1 — UcxBackend implementation (Phase A1).
#include "transport/ucx_backend.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#if KVCACHE_HAVE_UCX
#include <ucp/api/ucp.h>
#endif

namespace kvcache::node::transport {

#if KVCACHE_HAVE_UCX

namespace {

// Little-endian append/read helpers for the opaque descriptor.
void AppendU64(std::vector<uint8_t>* v, uint64_t x) {
    for (int i = 0; i < 8; ++i) v->push_back(static_cast<uint8_t>(x >> (8 * i)));
}
void AppendBytes(std::vector<uint8_t>* v, const void* p, std::size_t n) {
    AppendU64(v, n);
    const auto* b = static_cast<const uint8_t*>(p);
    v->insert(v->end(), b, b + n);
}
bool ReadU64(const uint8_t** p, const uint8_t* end, uint64_t* out) {
    if (end - *p < 8) return false;
    uint64_t x = 0;
    for (int i = 0; i < 8; ++i) x |= static_cast<uint64_t>((*p)[i]) << (8 * i);
    *p += 8;
    *out = x;
    return true;
}
bool ReadBytes(const uint8_t** p, const uint8_t* end, std::vector<uint8_t>* out) {
    uint64_t n = 0;
    if (!ReadU64(p, end, &n)) return false;
    if (static_cast<uint64_t>(end - *p) < n) return false;
    out->assign(*p, *p + n);
    *p += n;
    return true;
}

// Per-op completion, signalled from the progress thread's callback.
struct OpCompletion {
    std::mutex              m;
    std::condition_variable cv;
    bool                    done = false;
    ucs_status_t            status = UCS_OK;
};

void SendCompletionCb(void* request, ucs_status_t status, void* user_data) {
    auto* c = static_cast<OpCompletion*>(user_data);
    {
        std::lock_guard<std::mutex> lk(c->m);
        c->status = status;
        c->done = true;
    }
    c->cv.notify_all();
    ucp_request_free(request);
}

class UcxBackend final : public INixlBackend {
   public:
    static std::unique_ptr<UcxBackend> Create(std::string* err) {
        auto b = std::unique_ptr<UcxBackend>(new UcxBackend());
        if (!b->Init(err)) return nullptr;
        return b;
    }

    ~UcxBackend() override {
        // Stop the progress thread first, then tear down single-threaded so the
        // close requests below can be driven to completion by hand.
        stop_.store(true, std::memory_order_release);
        if (progress_thread_.joinable()) progress_thread_.join();
        {
            std::lock_guard<std::mutex> lk(mu_);
            for (auto& [k, r] : remote_mrs_) {
                if (r.rkey) ucp_rkey_destroy(r.rkey);
                if (r.ep) CloseEp(r.ep, /*drive_progress=*/true);
            }
            for (auto& [k, m] : local_mrs_) {
                if (m.memh) ucp_mem_unmap(context_, m.memh);
            }
        }
        if (worker_) ucp_worker_destroy(worker_);
        if (context_) ucp_cleanup(context_);
    }

    std::string Name() const override { return "ucx"; }

    MrKey RegisterRegion(void* addr, std::size_t bytes, std::string* err) override {
        if (!addr || bytes == 0) {
            if (err) *err = "ucx: invalid region";
            return kInvalidMrKey;
        }
        ucp_mem_map_params_t p;
        std::memset(&p, 0, sizeof(p));
        p.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        p.address = addr;
        p.length = bytes;
        ucp_mem_h memh = nullptr;
        ucs_status_t st = ucp_mem_map(context_, &p, &memh);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: mem_map: ") + ucs_status_string(st);
            return kInvalidMrKey;
        }
        std::lock_guard<std::mutex> lk(mu_);
        const MrKey k = next_key_.fetch_add(1, std::memory_order_relaxed);
        local_mrs_.emplace(k, LocalMr{addr, bytes, memh});
        return k;
    }

    void UnregisterRegion(MrKey key) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = local_mrs_.find(key);
        if (it != local_mrs_.end()) {
            if (it->second.memh) ucp_mem_unmap(context_, it->second.memh);
            local_mrs_.erase(it);
            return;
        }
        auto rit = remote_mrs_.find(key);
        if (rit != remote_mrs_.end()) {
            if (rit->second.rkey) ucp_rkey_destroy(rit->second.rkey);
            // Progress thread is still running here, so it drives the close.
            if (rit->second.ep) CloseEp(rit->second.ep, /*drive_progress=*/false);
            remote_mrs_.erase(rit);
        }
    }

    bool ResolveRegion(MrKey key, void** addr, std::size_t* bytes) const override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = local_mrs_.find(key);
        if (it != local_mrs_.end()) {
            if (addr) *addr = it->second.addr;
            if (bytes) *bytes = it->second.bytes;
            return true;
        }
        auto rit = remote_mrs_.find(key);
        if (rit != remote_mrs_.end()) {
            if (addr) *addr = nullptr;
            if (bytes) *bytes = rit->second.bytes;
            return true;
        }
        return false;
    }

    bool ExportMr(MrKey local_key, RemoteMrDescriptor* out_desc,
                  std::string* err) override {
        if (!out_desc) { if (err) *err = "ucx: out_desc null"; return false; }
        void* addr = nullptr;
        std::size_t bytes = 0;
        ucp_mem_h memh = nullptr;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = local_mrs_.find(local_key);
            if (it == local_mrs_.end()) {
                if (err) *err = "ucx: unknown local MR";
                return false;
            }
            addr = it->second.addr;
            bytes = it->second.bytes;
            memh = it->second.memh;
        }
        void* rkey_buf = nullptr;
        std::size_t rkey_len = 0;
        ucs_status_t st = ucp_rkey_pack(context_, memh, &rkey_buf, &rkey_len);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: rkey_pack: ") + ucs_status_string(st);
            return false;
        }
        out_desc->opaque.clear();
        AppendBytes(&out_desc->opaque, worker_addr_.data(), worker_addr_.size());
        AppendBytes(&out_desc->opaque, rkey_buf, rkey_len);
        AppendU64(&out_desc->opaque, reinterpret_cast<uint64_t>(addr));
        AppendU64(&out_desc->opaque, bytes);
        ucp_rkey_buffer_release(rkey_buf);
        return true;
    }

    MrKey ImportRemoteMr(const RemoteMrDescriptor& desc, std::string* err) override {
        const uint8_t* p = desc.opaque.data();
        const uint8_t* end = p + desc.opaque.size();
        std::vector<uint8_t> waddr, rkey_buf;
        uint64_t remote_va = 0, bytes = 0;
        if (!ReadBytes(&p, end, &waddr) || !ReadBytes(&p, end, &rkey_buf) ||
            !ReadU64(&p, end, &remote_va) || !ReadU64(&p, end, &bytes)) {
            if (err) *err = "ucx: malformed descriptor";
            return kInvalidMrKey;
        }
        ucp_ep_params_t ep_params;
        std::memset(&ep_params, 0, sizeof(ep_params));
        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
        ep_params.address = reinterpret_cast<const ucp_address_t*>(waddr.data());
        ucp_ep_h ep = nullptr;
        ucs_status_t st = ucp_ep_create(worker_, &ep_params, &ep);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: ep_create: ") + ucs_status_string(st);
            return kInvalidMrKey;
        }
        ucp_rkey_h rkey = nullptr;
        st = ucp_ep_rkey_unpack(ep, rkey_buf.data(), &rkey);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: rkey_unpack: ") + ucs_status_string(st);
            ucp_ep_destroy(ep);
            return kInvalidMrKey;
        }
        std::lock_guard<std::mutex> lk(mu_);
        const MrKey k = next_key_.fetch_add(1, std::memory_order_relaxed);
        remote_mrs_.emplace(k, RemoteMr{ep, rkey, remote_va, bytes});
        return k;
    }

    CompletionId Pull(const PullRequest& req, std::string* err) override {
        return Rma(/*is_get=*/true, req.dst_mr, req.dst_off, req.src_mr,
                   req.src_off, req.bytes, err);
    }

    CompletionId Push(const PushRequest& req, std::string* err) override {
        return Rma(/*is_get=*/false, req.dst_mr, req.dst_off, req.src_mr,
                   req.src_off, req.bytes, err);
    }

    bool IsRemote(MrKey key) const override {
        std::lock_guard<std::mutex> lk(mu_);
        return remote_mrs_.find(key) != remote_mrs_.end();
    }

    bool Wait(CompletionId cid, uint32_t /*timeout_ms*/, std::string* err) override {
        if (cid == kInvalidCompletionId) {
            if (err) *err = "ucx: invalid completion id";
            return false;
        }
        return true;  // Rma() already blocked to completion.
    }

   private:
    struct LocalMr {
        void*       addr = nullptr;
        std::size_t bytes = 0;
        ucp_mem_h   memh = nullptr;
    };
    struct RemoteMr {
        ucp_ep_h  ep = nullptr;
        ucp_rkey_h rkey = nullptr;
        uint64_t  remote_va = 0;
        uint64_t  bytes = 0;
    };

    UcxBackend() = default;

    // Close a UCP endpoint and wait for the close to complete. ucp_ep_close_nbx
    // REQUIRES a valid (zeroed) request-param — passing NULL segfaults inside
    // UCX. When drive_progress is true (destructor, progress thread stopped) we
    // pump the worker ourselves; otherwise the running progress thread does.
    void CloseEp(ucp_ep_h ep, bool drive_progress) {
        ucp_request_param_t p;
        std::memset(&p, 0, sizeof(p));
        ucs_status_ptr_t req = ucp_ep_close_nbx(ep, &p);
        if (req == nullptr || UCS_PTR_IS_ERR(req)) return;  // done / no-op
        while (ucp_request_check_status(req) == UCS_INPROGRESS) {
            if (drive_progress) ucp_worker_progress(worker_);
            else std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        ucp_request_free(req);
    }

    bool Init(std::string* err) {
        ucp_params_t params;
        std::memset(&params, 0, sizeof(params));
        params.field_mask = UCP_PARAM_FIELD_FEATURES;
        params.features = UCP_FEATURE_RMA;
        ucp_config_t* config = nullptr;
        ucs_status_t st = ucp_config_read(nullptr, nullptr, &config);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: config_read: ") + ucs_status_string(st);
            return false;
        }
        st = ucp_init(&params, config, &context_);
        ucp_config_release(config);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: ucp_init: ") + ucs_status_string(st);
            return false;
        }
        ucp_worker_params_t wparams;
        std::memset(&wparams, 0, sizeof(wparams));
        wparams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
        wparams.thread_mode = UCS_THREAD_MODE_MULTI;
        st = ucp_worker_create(context_, &wparams, &worker_);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: worker_create: ") + ucs_status_string(st);
            return false;
        }
        ucp_address_t* waddr = nullptr;
        std::size_t waddr_len = 0;
        st = ucp_worker_get_address(worker_, &waddr, &waddr_len);
        if (st != UCS_OK) {
            if (err) *err = std::string("ucx: worker_get_address: ") + ucs_status_string(st);
            return false;
        }
        worker_addr_.assign(reinterpret_cast<uint8_t*>(waddr),
                            reinterpret_cast<uint8_t*>(waddr) + waddr_len);
        ucp_worker_release_address(worker_, waddr);

        progress_thread_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                ucp_worker_progress(worker_);
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        });
        return true;
    }

    // Shared GET/PUT path. dst = where bytes land (local for GET, remote for
    // PUT); src = where they come from. Blocks until the op completes.
    CompletionId Rma(bool is_get, MrKey local_side_mr, uint64_t local_off,
                     MrKey remote_side_mr, uint64_t remote_off, uint64_t bytes,
                     std::string* err) {
        // GET: local = dst (must be local), remote = src (imported).
        // PUT: local = src (must be local, passed as remote_side_mr's opposite)
        // To keep one code path: the LOCAL buffer is the KV node's own region;
        // the REMOTE region is the peer's. For GET local_side_mr=dst_mr,
        // remote_side_mr=src_mr. For PUT the request gives src_mr(local),
        // dst_mr(remote): map local_side_mr=src_mr, remote_side_mr=dst_mr.
        void* local_addr = nullptr;
        std::size_t local_bytes = 0;
        ucp_ep_h ep = nullptr;
        ucp_rkey_h rkey = nullptr;
        uint64_t remote_va = 0, remote_bytes = 0;
        MrKey lkey = is_get ? local_side_mr : remote_side_mr;  // local buffer key
        MrKey rkey_id = is_get ? remote_side_mr : local_side_mr;  // remote region key
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto lit = local_mrs_.find(lkey);
            if (lit == local_mrs_.end()) {
                if (err) *err = "ucx: local MR not found (RMA needs a local buffer)";
                return kInvalidCompletionId;
            }
            local_addr = lit->second.addr;
            local_bytes = lit->second.bytes;
            auto rit = remote_mrs_.find(rkey_id);
            if (rit == remote_mrs_.end()) {
                // Intra-process both-local fast path: degenerate to memcpy.
                auto lit2 = local_mrs_.find(rkey_id);
                if (lit2 == local_mrs_.end()) {
                    if (err) *err = "ucx: remote MR not found";
                    return kInvalidCompletionId;
                }
                // local<-local copy
                uint64_t src_off = is_get ? remote_off : local_off;
                uint64_t dst_off = is_get ? local_off : remote_off;
                void* a = is_get ? local_addr : lit2->second.addr;
                void* b = is_get ? lit2->second.addr : local_addr;
                if (dst_off + bytes > (is_get ? local_bytes : lit2->second.bytes) ||
                    src_off + bytes > (is_get ? lit2->second.bytes : local_bytes)) {
                    if (err) *err = "ucx: out-of-bounds (local memcpy)";
                    return kInvalidCompletionId;
                }
                std::memcpy(static_cast<uint8_t*>(a) + (is_get ? local_off : remote_off),
                            static_cast<uint8_t*>(b) + (is_get ? remote_off : local_off),
                            bytes);
                return next_completion_.fetch_add(1, std::memory_order_relaxed);
            }
            ep = rit->second.ep;
            rkey = rit->second.rkey;
            remote_va = rit->second.remote_va;
            remote_bytes = rit->second.bytes;
        }
        if (local_off + bytes > local_bytes) {
            if (err) *err = "ucx: local out-of-bounds";
            return kInvalidCompletionId;
        }
        if (remote_off + bytes > remote_bytes) {
            if (err) *err = "ucx: remote out-of-bounds";
            return kInvalidCompletionId;
        }

        OpCompletion comp;
        ucp_request_param_t rp;
        std::memset(&rp, 0, sizeof(rp));
        rp.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
        rp.cb.send = SendCompletionCb;
        rp.user_data = &comp;

        void* buffer = static_cast<uint8_t*>(local_addr) + local_off;
        uint64_t raddr = remote_va + remote_off;
        ucs_status_ptr_t req = is_get
            ? ucp_get_nbx(ep, buffer, bytes, raddr, rkey, &rp)
            : ucp_put_nbx(ep, buffer, bytes, raddr, rkey, &rp);

        ucs_status_t final_st = UCS_OK;
        if (req == nullptr) {
            final_st = UCS_OK;  // completed inline; callback not invoked
        } else if (UCS_PTR_IS_ERR(req)) {
            final_st = UCS_PTR_STATUS(req);
        } else {
            std::unique_lock<std::mutex> lk(comp.m);
            comp.cv.wait_for(lk, std::chrono::seconds(30), [&] { return comp.done; });
            final_st = comp.done ? comp.status : UCS_ERR_TIMED_OUT;
        }
        if (final_st != UCS_OK) {
            if (err) *err = std::string("ucx: rma: ") + ucs_status_string(final_st);
            return kInvalidCompletionId;
        }
        // PUT: ensure remote visibility before returning.
        if (!is_get) {
            ucp_request_param_t fp;
            std::memset(&fp, 0, sizeof(fp));
            ucs_status_ptr_t freq = ucp_ep_flush_nbx(ep, &fp);
            if (UCS_PTR_IS_PTR(freq)) {
                ucs_status_t fst;
                do { ucp_worker_progress(worker_); fst = ucp_request_check_status(freq); }
                while (fst == UCS_INPROGRESS);
                ucp_request_free(freq);
            }
        }
        return next_completion_.fetch_add(1, std::memory_order_relaxed);
    }

    ucp_context_h context_ = nullptr;
    ucp_worker_h  worker_ = nullptr;
    std::vector<uint8_t> worker_addr_;

    mutable std::mutex                    mu_;
    std::unordered_map<MrKey, LocalMr>    local_mrs_;
    std::unordered_map<MrKey, RemoteMr>   remote_mrs_;
    std::atomic<MrKey>                    next_key_{1};
    std::atomic<CompletionId>             next_completion_{1};

    std::atomic<bool>                     stop_{false};
    std::thread                           progress_thread_;
};

}  // namespace

std::unique_ptr<INixlBackend> CreateUcxBackend(const BackendOptions& /*opts*/,
                                               std::string* err) {
    return UcxBackend::Create(err);
}

#else  // !KVCACHE_HAVE_UCX

std::unique_ptr<INixlBackend> CreateUcxBackend(const BackendOptions& /*opts*/,
                                               std::string* err) {
    if (err) *err = "ucx: not compiled in (build with -DKVCACHE_ENABLE_UCX=ON)";
    return nullptr;
}

#endif  // KVCACHE_HAVE_UCX

}  // namespace kvcache::node::transport
