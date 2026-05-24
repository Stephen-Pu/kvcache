// LLD §3.5 D-NET-1 — TcpBackend implementation.
#include "transport/tcp_backend.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <utility>
#include <vector>

namespace kvcache::node::transport {

namespace {

constexpr uint8_t kOpGet = 1;
constexpr uint8_t kOpPut = 2;  // Phase M-6 — server-push.

// Request: op(1) + peer_mr_id(4) + offset(8) + length(8) = 21 bytes.
// For PUT, `length` bytes of payload follow the 21-byte header.
constexpr std::size_t kRequestSize = 1 + 4 + 8 + 8;
// Response header: status(1) + length(8) = 9 bytes; bytes follow (GET only).
// PUT response = status(1) only.
constexpr std::size_t kResponseHeaderSize = 1 + 8;

inline int CloseFd(int fd) {
    if (fd >= 0) ::close(fd);
    return -1;
}

}  // namespace

// ---------------------------------------------------------------------------
// Wire I/O
// ---------------------------------------------------------------------------

bool TcpBackend::WriteAll(int fd, const void* buf, std::size_t n) noexcept {
    const auto* p = static_cast<const uint8_t*>(buf);
    while (n) {
        const ssize_t w = ::send(fd, p, n, 0);
        if (w < 0) { if (errno == EINTR) continue; return false; }
        if (w == 0) return false;
        p += w; n -= w;
    }
    return true;
}

bool TcpBackend::ReadAll(int fd, void* buf, std::size_t n) noexcept {
    auto* p = static_cast<uint8_t*>(buf);
    while (n) {
        const ssize_t r = ::recv(fd, p, n, 0);
        if (r < 0) { if (errno == EINTR) continue; return false; }
        if (r == 0) return false;  // peer closed mid-read
        p += r; n -= r;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Constructor / destructor
// ---------------------------------------------------------------------------

TcpBackend::TcpBackend(const BackendOptions& opts)
    : bind_host_(opts.bind_host.empty() ? std::string("127.0.0.1")
                                         : opts.bind_host) {
    // Create socket.
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(opts.bind_port));
    if (::inet_pton(AF_INET, bind_host_.c_str(), &addr.sin_addr) != 1) {
        CloseFd(fd);
        return;
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        CloseFd(fd);
        return;
    }
    // Resolve actual port (in case caller passed 0).
    sockaddr_in actual{};
    socklen_t len = sizeof(actual);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &len) < 0) {
        CloseFd(fd);
        return;
    }
    port_.store(ntohs(actual.sin_port), std::memory_order_release);

    if (::listen(fd, static_cast<int>(opts.listen_backlog)) < 0) {
        CloseFd(fd);
        return;
    }

    listener_fd_ = fd;
    accept_thread_ = std::thread([this] { AcceptLoop(); });
}

TcpBackend::~TcpBackend() {
    stop_.store(true, std::memory_order_release);
    if (listener_fd_ >= 0) {
        // shutdown unblocks any accept() spinning in the background thread.
        ::shutdown(listener_fd_, SHUT_RDWR);
        ::close(listener_fd_);
        listener_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();
}

// ---------------------------------------------------------------------------
// Accept loop + per-connection handler
// ---------------------------------------------------------------------------

void TcpBackend::AcceptLoop() {
    while (!stop_.load(std::memory_order_acquire)) {
        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        const int conn = ::accept(listener_fd_,
                                  reinterpret_cast<sockaddr*>(&peer), &plen);
        if (conn < 0) {
            if (errno == EINTR) continue;
            // Listener closed during shutdown — exit cleanly.
            return;
        }
        // For Phase C-1 we serve one request per connection synchronously
        // on the same accept thread. A real production backend would hand
        // off to a worker pool — Phase C-2 optimisation.
        HandleConnection(conn);
        ::close(conn);
    }
}

void TcpBackend::HandleConnection(int conn_fd) {
    uint8_t req[kRequestSize];
    if (!ReadAll(conn_fd, req, sizeof(req))) return;

    const uint8_t op = req[0];
    if (op != kOpGet && op != kOpPut) {
        // Unknown op — send GET-shaped error and bail.
        const uint8_t status = 1;
        const uint64_t len = 0;
        WriteAll(conn_fd, &status, 1);
        WriteAll(conn_fd, &len, 8);
        return;
    }

    uint32_t peer_mr_id = 0;
    uint64_t offset = 0, length = 0;
    std::memcpy(&peer_mr_id, req + 1, 4);
    std::memcpy(&offset,     req + 5, 8);
    std::memcpy(&length,     req + 13, 8);

    // Resolve the MR locally.
    void* addr = nullptr;
    std::size_t bytes = 0;
    {
        std::lock_guard lk(mu_);
        auto it = local_mrs_.find(peer_mr_id);
        if (it == local_mrs_.end()) {
            if (op == kOpPut) {
                const uint8_t status = 2;
                WriteAll(conn_fd, &status, 1);
            } else {
                const uint8_t status = 2;
                const uint64_t len = 0;
                WriteAll(conn_fd, &status, 1);
                WriteAll(conn_fd, &len, 8);
            }
            return;
        }
        addr  = it->second.addr;
        bytes = it->second.bytes;
    }
    if (offset + length > bytes) {
        if (op == kOpPut) {
            const uint8_t status = 3;
            WriteAll(conn_fd, &status, 1);
        } else {
            const uint8_t status = 3;
            const uint64_t len = 0;
            WriteAll(conn_fd, &status, 1);
            WriteAll(conn_fd, &len, 8);
        }
        return;
    }

    if (op == kOpPut) {
        // Phase M-6 — peer is depositing bytes into our MR. Read
        // `length` bytes directly into the target offset, then ack.
        if (length > 0) {
            if (!ReadAll(conn_fd,
                          static_cast<uint8_t*>(addr) + offset, length)) {
                const uint8_t status = 4;
                WriteAll(conn_fd, &status, 1);
                return;
            }
        }
        const uint8_t status = 0;
        WriteAll(conn_fd, &status, 1);
        return;
    }

    // GET path.
    const uint8_t status = 0;
    if (!WriteAll(conn_fd, &status, 1))  return;
    if (!WriteAll(conn_fd, &length, 8))  return;
    if (length > 0) {
        WriteAll(conn_fd, static_cast<uint8_t*>(addr) + offset, length);
    }
}

// ---------------------------------------------------------------------------
// MR management
// ---------------------------------------------------------------------------

MrKey TcpBackend::RegisterRegion(void* addr, std::size_t bytes,
                                  std::string* err) {
    if (!addr || bytes == 0) {
        if (err) *err = "tcp: invalid region";
        return kInvalidMrKey;
    }
    std::lock_guard lk(mu_);
    const MrKey k = next_key_.fetch_add(1, std::memory_order_relaxed);
    local_mrs_.emplace(k, LocalMr{addr, bytes});
    return k;
}

void TcpBackend::UnregisterRegion(MrKey key) {
    std::lock_guard lk(mu_);
    local_mrs_.erase(key);
    remote_mrs_.erase(key);
}

bool TcpBackend::ResolveRegion(MrKey key, void** addr,
                                std::size_t* bytes) const {
    std::lock_guard lk(mu_);
    auto it = local_mrs_.find(key);
    if (it == local_mrs_.end()) {
        // Remote MRs don't have a local address — surface bytes only.
        auto r = remote_mrs_.find(key);
        if (r == remote_mrs_.end()) return false;
        if (addr)  *addr  = nullptr;
        if (bytes) *bytes = r->second.bytes;
        return true;
    }
    if (addr)  *addr  = it->second.addr;
    if (bytes) *bytes = it->second.bytes;
    return true;
}

// ---------------------------------------------------------------------------
// Remote MR exchange
// ---------------------------------------------------------------------------
//
// Wire layout for a TcpBackend RemoteMrDescriptor:
//   uint16_t host_len ; host_bytes[host_len] ; uint16_t port ;
//   uint32_t peer_mr_id ; uint64_t bytes
//
// Total = 2 + host_len + 2 + 4 + 8

bool TcpBackend::ExportMr(MrKey local_key, RemoteMrDescriptor* out_desc,
                          std::string* err) {
    if (!out_desc) { if (err) *err = "tcp: out_desc is null"; return false; }
    std::size_t bytes = 0;
    {
        std::lock_guard lk(mu_);
        auto it = local_mrs_.find(local_key);
        if (it == local_mrs_.end()) {
            if (err) *err = "tcp: unknown local MR";
            return false;
        }
        bytes = it->second.bytes;
    }

    const uint16_t host_len = static_cast<uint16_t>(bind_host_.size());
    const uint16_t port = port_.load(std::memory_order_acquire);
    const uint32_t peer_mr_id = local_key;
    const uint64_t bytes_u64 = bytes;

    out_desc->opaque.clear();
    out_desc->opaque.reserve(2 + host_len + 2 + 4 + 8);
    auto append = [&](const void* p, std::size_t n) {
        const auto* b = static_cast<const uint8_t*>(p);
        out_desc->opaque.insert(out_desc->opaque.end(), b, b + n);
    };
    append(&host_len,    sizeof(host_len));
    append(bind_host_.data(), host_len);
    append(&port,        sizeof(port));
    append(&peer_mr_id,  sizeof(peer_mr_id));
    append(&bytes_u64,   sizeof(bytes_u64));
    return true;
}

MrKey TcpBackend::ImportRemoteMr(const RemoteMrDescriptor& desc,
                                  std::string* err) {
    if (desc.opaque.size() < 2) {
        if (err) *err = "tcp: descriptor too short";
        return kInvalidMrKey;
    }
    std::size_t off = 0;
    uint16_t host_len = 0;
    std::memcpy(&host_len, desc.opaque.data() + off, 2); off += 2;
    if (desc.opaque.size() < 2u + host_len + 2u + 4u + 8u) {
        if (err) *err = "tcp: descriptor truncated";
        return kInvalidMrKey;
    }
    std::string host(reinterpret_cast<const char*>(desc.opaque.data() + off),
                     host_len);
    off += host_len;
    uint16_t port = 0;
    uint32_t peer_mr_id = 0;
    uint64_t bytes = 0;
    std::memcpy(&port,       desc.opaque.data() + off, 2); off += 2;
    std::memcpy(&peer_mr_id, desc.opaque.data() + off, 4); off += 4;
    std::memcpy(&bytes,      desc.opaque.data() + off, 8); off += 8;

    std::lock_guard lk(mu_);
    const MrKey k = next_key_.fetch_add(1, std::memory_order_relaxed);
    remote_mrs_.emplace(k, RemoteMr{std::move(host), port, peer_mr_id, bytes});
    return k;
}

// ---------------------------------------------------------------------------
// Pull
// ---------------------------------------------------------------------------

int TcpBackend::ConnectPeer(const RemoteMr& peer, std::string* err) const {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        if (err) *err = std::string("tcp: socket: ") + std::strerror(errno);
        return -1;
    }
    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    if (::inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr) != 1) {
        if (err) *err = "tcp: bad peer host '" + peer.host + "'";
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (err) *err = std::string("tcp: connect: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    return fd;
}

CompletionId TcpBackend::Pull(const PullRequest& req, std::string* err) {
    // Resolve dst (must be local).
    void* dst_addr = nullptr;
    std::size_t dst_bytes = 0;
    {
        std::lock_guard lk(mu_);
        auto it = local_mrs_.find(req.dst_mr);
        if (it == local_mrs_.end()) {
            if (err) *err = "tcp: dst_mr is not local";
            return kInvalidCompletionId;
        }
        dst_addr = it->second.addr;
        dst_bytes = it->second.bytes;
    }
    if (req.dst_off + req.bytes > dst_bytes) {
        if (err) *err = "tcp: dst out-of-bounds";
        return kInvalidCompletionId;
    }

    // Resolve src: either local (intra-process Pull = memcpy) or remote
    // (network fetch).
    void* src_local_addr = nullptr;
    std::size_t src_local_bytes = 0;
    RemoteMr remote_copy;
    bool is_remote = false;
    {
        std::lock_guard lk(mu_);
        auto lit = local_mrs_.find(req.src_mr);
        if (lit != local_mrs_.end()) {
            src_local_addr = lit->second.addr;
            src_local_bytes = lit->second.bytes;
        } else {
            auto rit = remote_mrs_.find(req.src_mr);
            if (rit == remote_mrs_.end()) {
                if (err) *err = "tcp: unknown src_mr";
                return kInvalidCompletionId;
            }
            remote_copy = rit->second;
            is_remote = true;
        }
    }

    if (!is_remote) {
        // Local path: memcpy.
        if (req.src_off + req.bytes > src_local_bytes) {
            if (err) *err = "tcp: src out-of-bounds";
            return kInvalidCompletionId;
        }
        std::memcpy(static_cast<uint8_t*>(dst_addr) + req.dst_off,
                    static_cast<uint8_t*>(src_local_addr) + req.src_off,
                    req.bytes);
        return next_completion_.fetch_add(1, std::memory_order_relaxed);
    }

    // Remote path: connect to peer, send GET, receive bytes into dst.
    if (req.src_off + req.bytes > remote_copy.bytes) {
        if (err) *err = "tcp: src out-of-bounds (remote)";
        return kInvalidCompletionId;
    }
    int fd = ConnectPeer(remote_copy, err);
    if (fd < 0) return kInvalidCompletionId;

    uint8_t out[kRequestSize];
    out[0] = kOpGet;
    std::memcpy(out + 1,  &remote_copy.peer_mr_id, 4);
    std::memcpy(out + 5,  &req.src_off,            8);
    std::memcpy(out + 13, &req.bytes,              8);
    if (!WriteAll(fd, out, sizeof(out))) {
        if (err) *err = "tcp: short write of request";
        ::close(fd);
        return kInvalidCompletionId;
    }

    uint8_t status = 255;
    uint64_t resp_len = 0;
    if (!ReadAll(fd, &status, 1) || !ReadAll(fd, &resp_len, 8)) {
        if (err) *err = "tcp: short read of response header";
        ::close(fd);
        return kInvalidCompletionId;
    }
    if (status != 0) {
        if (err) *err = "tcp: peer returned error status " + std::to_string(status);
        ::close(fd);
        return kInvalidCompletionId;
    }
    if (resp_len != req.bytes) {
        if (err) *err = "tcp: peer length mismatch";
        ::close(fd);
        return kInvalidCompletionId;
    }
    if (!ReadAll(fd, static_cast<uint8_t*>(dst_addr) + req.dst_off, resp_len)) {
        if (err) *err = "tcp: short read of payload";
        ::close(fd);
        return kInvalidCompletionId;
    }
    ::close(fd);
    return next_completion_.fetch_add(1, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Push (Phase M-6) — server-push counterpart of Pull.
// ---------------------------------------------------------------------------
//
// src_mr MUST be local; dst_mr MAY be local (intra-process — memcpy)
// or imported-remote (we connect to the peer's listener and write the
// bytes via the PUT op).
CompletionId TcpBackend::Push(const PushRequest& req, std::string* err) {
    // Resolve src (must be local).
    void* src_addr = nullptr;
    std::size_t src_bytes = 0;
    {
        std::lock_guard lk(mu_);
        auto it = local_mrs_.find(req.src_mr);
        if (it == local_mrs_.end()) {
            if (err) *err = "tcp: src_mr is not local (Push requires local src)";
            return kInvalidCompletionId;
        }
        src_addr  = it->second.addr;
        src_bytes = it->second.bytes;
    }
    if (req.src_off + req.bytes > src_bytes) {
        if (err) *err = "tcp: src out-of-bounds";
        return kInvalidCompletionId;
    }

    // Resolve dst: local (memcpy) or remote (PUT over wire).
    void* dst_local_addr = nullptr;
    std::size_t dst_local_bytes = 0;
    RemoteMr remote_copy;
    bool is_remote = false;
    {
        std::lock_guard lk(mu_);
        auto lit = local_mrs_.find(req.dst_mr);
        if (lit != local_mrs_.end()) {
            dst_local_addr  = lit->second.addr;
            dst_local_bytes = lit->second.bytes;
        } else {
            auto rit = remote_mrs_.find(req.dst_mr);
            if (rit == remote_mrs_.end()) {
                if (err) *err = "tcp: unknown dst_mr";
                return kInvalidCompletionId;
            }
            remote_copy = rit->second;
            is_remote = true;
        }
    }

    if (!is_remote) {
        if (req.dst_off + req.bytes > dst_local_bytes) {
            if (err) *err = "tcp: dst out-of-bounds";
            return kInvalidCompletionId;
        }
        std::memcpy(static_cast<uint8_t*>(dst_local_addr) + req.dst_off,
                    static_cast<uint8_t*>(src_addr) + req.src_off,
                    req.bytes);
        return next_completion_.fetch_add(1, std::memory_order_relaxed);
    }

    if (req.dst_off + req.bytes > remote_copy.bytes) {
        if (err) *err = "tcp: dst out-of-bounds (remote)";
        return kInvalidCompletionId;
    }
    int fd = ConnectPeer(remote_copy, err);
    if (fd < 0) return kInvalidCompletionId;

    uint8_t hdr[kRequestSize];
    hdr[0] = kOpPut;
    std::memcpy(hdr + 1,  &remote_copy.peer_mr_id, 4);
    std::memcpy(hdr + 5,  &req.dst_off,            8);
    std::memcpy(hdr + 13, &req.bytes,              8);
    if (!WriteAll(fd, hdr, sizeof(hdr))) {
        if (err) *err = "tcp: short write of PUT header";
        ::close(fd);
        return kInvalidCompletionId;
    }
    if (req.bytes > 0 &&
        !WriteAll(fd, static_cast<uint8_t*>(src_addr) + req.src_off,
                   req.bytes)) {
        if (err) *err = "tcp: short write of PUT payload";
        ::close(fd);
        return kInvalidCompletionId;
    }
    uint8_t status = 255;
    if (!ReadAll(fd, &status, 1)) {
        if (err) *err = "tcp: short read of PUT ack";
        ::close(fd);
        return kInvalidCompletionId;
    }
    ::close(fd);
    if (status != 0) {
        if (err) *err = "tcp: peer returned PUT error status " +
                          std::to_string(status);
        return kInvalidCompletionId;
    }
    return next_completion_.fetch_add(1, std::memory_order_relaxed);
}

bool TcpBackend::IsRemote(MrKey key) const {
    std::lock_guard lk(mu_);
    return remote_mrs_.find(key) != remote_mrs_.end();
}

bool TcpBackend::Wait(CompletionId cid, uint32_t /*timeout_ms*/,
                       std::string* err) {
    if (cid == kInvalidCompletionId) {
        if (err) *err = "tcp: invalid completion id";
        return false;
    }
    return true;  // Pull is synchronous on this backend.
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

std::unique_ptr<INixlBackend> CreateTcpBackend(const BackendOptions& opts,
                                                std::string* err) {
    auto b = std::make_unique<TcpBackend>(opts);
    if (!b->Ok()) {
        if (err) *err = "tcp: failed to bind " + opts.bind_host + ":" +
                        std::to_string(opts.bind_port);
        return nullptr;
    }
    return b;
}

}  // namespace kvcache::node::transport
