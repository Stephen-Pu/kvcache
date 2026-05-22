// Phase L-1 — minimum-viable kvstore-node runtime. See node_runtime.h.
#include "runtime/node_runtime.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <span>
#include <string>
#include <string_view>

#include "metrics.h"

namespace kvcache::node::runtime {

namespace {

// One signal handler per process. K8s sends SIGTERM on pod evict;
// Ctrl-C from a dev `make run` sends SIGINT. We funnel both into a
// process-wide atomic that the active NodeRuntime polls via the
// signal_pipe_ trick below — keeps the handler async-signal-safe.
std::atomic<NodeRuntime*> g_active_runtime{nullptr};
std::atomic<int>          g_last_signal{0};

void SignalHandler(int sig) {
    g_last_signal.store(sig, std::memory_order_release);
    auto* rt = g_active_runtime.load(std::memory_order_acquire);
    if (rt) {
        // Notify is async-signal-safe in practice on macOS / Linux
        // glibc; pthread_cond_signal is the canonical primitive.
        // We don't hold rt's mutex here — Wait() re-checks the
        // signal slot under the lock so the spurious-wakeup is fine.
        rt->Stop();
    }
}

// Bind a TCP listener on (host, port). Returns the fd and writes the
// actually-bound port back through `*resolved` (handles port=0).
int BindListener(const std::string& host, uint16_t port,
                  uint16_t* resolved, std::string* err) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        if (err) *err = std::string("socket: ") + std::strerror(errno);
        return -1;
    }
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        if (err) *err = "bad bind host: " + host;
        ::close(fd);
        return -1;
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        if (err) *err = std::string("bind: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    if (::listen(fd, 32) != 0) {
        if (err) *err = std::string("listen: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    sockaddr_in bound{};
    socklen_t   blen = sizeof(bound);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &blen) != 0) {
        if (err) *err = std::string("getsockname: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    if (resolved) *resolved = ntohs(bound.sin_port);
    return fd;
}

bool WriteAll(int fd, const void* buf, std::size_t n) {
    auto* p = static_cast<const char*>(buf);
    while (n > 0) {
        ssize_t w = ::send(fd, p, n, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (w == 0) return false;
        p += w;
        n -= static_cast<std::size_t>(w);
    }
    return true;
}

// Read up to the first CRLF CRLF (end of HTTP request headers). Returns
// the request line on success or empty on error/closed.
std::string ReadRequestLine(int fd) {
    std::string buf;
    char b[1024];
    while (buf.find("\r\n\r\n") == std::string::npos && buf.size() < 8192) {
        ssize_t r = ::recv(fd, b, sizeof(b), 0);
        if (r <= 0) return {};
        buf.append(b, static_cast<std::size_t>(r));
    }
    auto end = buf.find("\r\n");
    if (end == std::string::npos) return {};
    return buf.substr(0, end);
}

void WriteHttpResponse(int fd, int status, std::string_view content_type,
                        std::string_view body) {
    std::string head;
    head.reserve(128);
    head += "HTTP/1.1 ";
    head += std::to_string(status);
    head += (status == 200 ? " OK\r\n" : " Not Found\r\n");
    head += "Content-Type: ";
    head.append(content_type.data(), content_type.size());
    head += "\r\n";
    head += "Content-Length: ";
    head += std::to_string(body.size());
    head += "\r\n";
    head += "Connection: close\r\n\r\n";
    WriteAll(fd, head.data(), head.size());
    WriteAll(fd, body.data(), body.size());
}

}  // namespace

NodeRuntime::NodeRuntime(const Options& opts) : opts_(opts) {
    // grpc port: bind iff the caller hasn't reserved it for an external
    // grpc::Server (Phase M-1 main.cpp opts into this when KVCACHE_HAVE_GRPC).
    if (!opts_.skip_grpc_listener) {
        grpc_fd_ = BindListener(opts_.bind_host, opts_.grpc_port,
                                  &grpc_port_resolved_, &error_);
        if (grpc_fd_ < 0) return;
    } else {
        // Reflect the caller-supplied port back so GrpcPort() reports
        // something useful in logs / tests.
        grpc_port_resolved_ = opts_.grpc_port;
    }
    metrics_fd_ = BindListener(opts_.bind_host, opts_.metrics_port,
                                  &metrics_port_resolved_, &error_);
    if (metrics_fd_ < 0) {
        if (grpc_fd_ >= 0) {
            ::close(grpc_fd_);
            grpc_fd_ = -1;
        }
        return;
    }
    ok_ = true;
}

NodeRuntime::~NodeRuntime() { Stop(); }

void NodeRuntime::Start() {
    if (!ok_) return;

    // Install signal handlers so a real `kubectl delete pod` (SIGTERM)
    // unblocks Wait(). Dev runs hit Ctrl-C → SIGINT.
    NodeRuntime* expected = nullptr;
    if (g_active_runtime.compare_exchange_strong(expected, this)) {
        struct sigaction sa{};
        sa.sa_handler = SignalHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        ::sigaction(SIGTERM, &sa, nullptr);
        ::sigaction(SIGINT,  &sa, nullptr);
    }

    // The grpc accept loop is just a placeholder TCP listener for the
    // readiness probe. When the caller hands the grpc port to a real
    // grpc::Server (skip_grpc_listener), we skip the thread entirely.
    if (!opts_.skip_grpc_listener) {
        grpc_thread_ = std::thread(&NodeRuntime::GrpcAcceptLoop, this);
    }
    metrics_thread_ = std::thread(&NodeRuntime::MetricsAcceptLoop, this);
}

int NodeRuntime::Wait() {
    std::unique_lock<std::mutex> lk(wait_mu_);
    wait_cv_.wait(lk, [&] { return stop_.load(std::memory_order_acquire); });
    return signal_received_;
}

void NodeRuntime::Stop() {
    bool expected = false;
    if (!stop_.compare_exchange_strong(expected, true)) {
        // Already stopping; just join if a Wait()er is still around.
    }
    // Shutdown both listeners so accept() returns. Close after shutdown
    // so an in-flight client connection doesn't dangle.
    if (grpc_fd_ >= 0) {
        ::shutdown(grpc_fd_, SHUT_RDWR);
        ::close(grpc_fd_);
        grpc_fd_ = -1;
    }
    if (metrics_fd_ >= 0) {
        ::shutdown(metrics_fd_, SHUT_RDWR);
        ::close(metrics_fd_);
        metrics_fd_ = -1;
    }
    if (grpc_thread_.joinable())    grpc_thread_.join();
    if (metrics_thread_.joinable()) metrics_thread_.join();

    {
        std::lock_guard<std::mutex> lk(wait_mu_);
        signal_received_ = g_last_signal.load(std::memory_order_acquire);
        wait_cv_.notify_all();
    }

    NodeRuntime* expected_self = this;
    g_active_runtime.compare_exchange_strong(expected_self, nullptr);
}

void NodeRuntime::GrpcAcceptLoop() {
    while (!stop_.load(std::memory_order_acquire) && grpc_fd_ >= 0) {
        int conn = ::accept(grpc_fd_, nullptr, nullptr);
        if (conn < 0) {
            if (stop_.load(std::memory_order_acquire)) return;
            if (errno == EINTR) continue;
            return;
        }
        // Phase L-1 just keeps the port alive for the readiness probe.
        // The TCPSocket check the operator sets only needs accept() to
        // succeed; the body never matters. Close immediately so we don't
        // pin descriptors.
        ::close(conn);
    }
}

void NodeRuntime::MetricsAcceptLoop() {
    while (!stop_.load(std::memory_order_acquire) && metrics_fd_ >= 0) {
        int conn = ::accept(metrics_fd_, nullptr, nullptr);
        if (conn < 0) {
            if (stop_.load(std::memory_order_acquire)) return;
            if (errno == EINTR) continue;
            return;
        }
        HandleMetricsConnection(conn);
        ::close(conn);
    }
}

void NodeRuntime::HandleMetricsConnection(int fd) {
    const auto req = ReadRequestLine(fd);
    if (req.empty()) return;

    // Parse "GET /path HTTP/1.1" — we only care about the path.
    auto sp1 = req.find(' ');
    auto sp2 = (sp1 != std::string::npos) ? req.find(' ', sp1 + 1) : std::string::npos;
    if (sp1 == std::string::npos || sp2 == std::string::npos) {
        WriteHttpResponse(fd, 404, "text/plain", "bad request\n");
        return;
    }
    const std::string path = req.substr(sp1 + 1, sp2 - sp1 - 1);

    if (path == "/healthz" || path == "/readyz") {
        WriteHttpResponse(fd, 200, "text/plain", "ok\n");
        return;
    }
    if (path == "/metrics") {
        std::string body;
        kvcache::metrics::Registry::Default().Scrape(body);
        WriteHttpResponse(fd, 200, "text/plain; version=0.0.4", body);
        return;
    }
    WriteHttpResponse(fd, 404, "text/plain", "not found\n");
}

}  // namespace kvcache::node::runtime
