// LLD §3.5 D-NET-1 — TCP backend for the NIXL transport abstraction.
//
// Real cross-process / cross-node Pull transport, no RDMA dependency. Each
// TcpBackend instance:
//
//   * Binds a TCP listener on construction (host/port from BackendOptions;
//     port=0 means OS-picked, retrievable via `ListenerEndpoint`).
//   * Runs an accept-loop in a background thread that handles incoming
//     fetch requests from peer backends.
//   * Exports local MRs as opaque descriptors that encode the listener
//     endpoint + a stable per-MR token. Peers import these descriptors
//     and use the resulting local MrKey as `src_mr` in their Pull calls.
//   * On a remote-source Pull, opens a TCP connection (per-call for now —
//     connection pooling is a Phase-2 optimisation) to the peer's listener
//     and reads the requested byte range into the local destination MR.
//
// Wire protocol (host-endian; backends agree because both ends are the
// same TcpBackend code):
//
//   Request   = [op:u8=GET, peer_mr_id:u32, offset:u64, length:u64]   = 21 B
//   Response  = [status:u8, length:u64, bytes...]                     = 9 + N B
//
// `status`: 0 = OK, non-zero = error (no bytes follow).
//
// Wait() is a no-op in this synchronous-on-issue mode; the Pull call
// blocks until the bytes are in `dst_mr` (or it returns an error).
//
// Not a goal: encryption, authentication, congestion control, multiplexed
// streams. This is the development / on-prem-trusted-network transport;
// for untrusted networks layer TLS via stunnel or use a UCX TLS transport.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "transport/nixl_wrapper.h"

namespace kvcache::node::transport {

class TcpBackend final : public INixlBackend {
   public:
    // Static factory so users go through CreateBackend("tcp", ...). The
    // public ctor exists for tests that want to pin a specific port.
    explicit TcpBackend(const BackendOptions& opts);
    ~TcpBackend() override;

    TcpBackend(const TcpBackend&)            = delete;
    TcpBackend& operator=(const TcpBackend&) = delete;

    bool Ok() const noexcept { return listener_fd_ >= 0; }

    // Bound (host, port). Useful for tests / discovery — read after ctor
    // returns, since OS-picked ports are resolved during bind.
    std::string ListenerHost() const noexcept { return bind_host_; }
    uint16_t    ListenerPort() const noexcept {
        return port_.load(std::memory_order_acquire);
    }

    // ---- INixlBackend ----
    std::string Name() const override { return "tcp"; }

    MrKey RegisterRegion(void* addr, std::size_t bytes, std::string* err) override;
    void  UnregisterRegion(MrKey key) override;
    bool  ResolveRegion(MrKey key, void** addr, std::size_t* bytes) const override;

    bool  ExportMr(MrKey local_key, RemoteMrDescriptor* out_desc,
                   std::string* err) override;
    MrKey ImportRemoteMr(const RemoteMrDescriptor& desc,
                          std::string* err) override;

    CompletionId Pull(const PullRequest& req, std::string* err) override;
    CompletionId Push(const PushRequest& req, std::string* err) override;
    bool         IsRemote(MrKey key) const override;
    bool         Wait(CompletionId cid, uint32_t timeout_ms,
                      std::string* err) override;

   private:
    // ---- internals ----

    // Local MR: real memory owned by this process.
    struct LocalMr {
        void*       addr  = nullptr;
        std::size_t bytes = 0;
    };
    // Remote MR: an imported descriptor pointing at a peer's listener +
    // peer-side MR id.
    struct RemoteMr {
        std::string host;
        uint16_t    port = 0;
        uint32_t    peer_mr_id = 0;
        uint64_t    bytes = 0;
    };

    void AcceptLoop();
    void HandleConnection(int conn_fd);

    // Open a fresh TCP connection to peer; on success returns the socket fd
    // (caller closes). Sets `err` on failure.
    int  ConnectPeer(const RemoteMr& peer, std::string* err) const;

    // Wire protocol helpers (host-endian, internal).
    static bool WriteAll(int fd, const void* buf, std::size_t n) noexcept;
    static bool ReadAll(int fd, void* buf, std::size_t n) noexcept;

    std::string                          bind_host_;
    std::atomic<uint16_t>                port_{0};
    int                                  listener_fd_ = -1;

    mutable std::mutex                   mu_;
    std::unordered_map<MrKey, LocalMr>   local_mrs_;
    std::unordered_map<MrKey, RemoteMr>  remote_mrs_;
    std::atomic<MrKey>                   next_key_{1};
    std::atomic<CompletionId>            next_completion_{1};

    std::atomic<bool>                    stop_{false};
    std::thread                          accept_thread_;
};

// Factory entry exposed to CreateBackend (defined in nixl_wrapper.cpp).
std::unique_ptr<INixlBackend> CreateTcpBackend(const BackendOptions& opts,
                                                std::string* err);

}  // namespace kvcache::node::transport
