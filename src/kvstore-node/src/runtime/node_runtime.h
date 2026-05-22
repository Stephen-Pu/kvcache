// Phase L-1 — minimum-viable kvstore-node runtime.
//
// The Phase A–K work shipped library code; nothing actually ran inside a
// pod. The K-1 kind e2e validated operator object-shape against a real
// apiserver, but kvstore-node containers always landed in
// CrashLoopBackOff because src/main.cpp was a `puts + exit(0)` stub.
//
// `NodeRuntime` closes that gap with the smallest surface that turns a
// kvstore-node container into a Ready pod:
//
//   * A TCP accept-loop on the `grpc` port (default 7000). The operator's
//     readiness probe is a TCPSocket check on this port — so just
//     listening is enough to flip the pod Ready. We accept-and-close on
//     each connection; real gRPC handlers land in a follow-up.
//
//   * A tiny HTTP server on the `metrics` port (default 9090) exposing
//     two endpoints:
//       GET /metrics  → Prometheus exposition text from
//                       kvcache::metrics::Registry::Default().Scrape().
//       GET /healthz  → 200 OK + "ok\n", for liveness if K8s adds one.
//
//   * A blocking `Wait()` that returns on SIGTERM / SIGINT or on an
//     explicit `Stop()` call. The latter is what unit tests drive.
//
// Both listeners are configurable to port 0 (OS-picked) so unit tests
// can drive them on ephemeral ports without colliding with the
// production constants.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

namespace kvcache::node::runtime {

class NodeRuntime {
   public:
    struct Options {
        std::string config_path;   // unused for now; passed by operator
        std::string bind_host    = "0.0.0.0";
        uint16_t    grpc_port    = 7000;
        uint16_t    metrics_port = 9090;
        // When true, NodeRuntime does NOT bind / accept on the grpc
        // port — the caller (main.cpp, when grpc++ is compiled in)
        // claims it with a real grpc::Server instead. NodeRuntime
        // still owns the metrics port + signal handling regardless.
        bool        skip_grpc_listener = false;
    };

    // Construct + bind both listeners. On failure `Ok()` returns false
    // and the resolved error string is in `error()`.
    explicit NodeRuntime(const Options& opts);
    ~NodeRuntime();

    NodeRuntime(const NodeRuntime&)            = delete;
    NodeRuntime& operator=(const NodeRuntime&) = delete;

    bool Ok() const noexcept { return ok_; }
    const std::string& error() const noexcept { return error_; }

    // Actual bound ports (resolves OS-picked port=0 to the live number).
    uint16_t GrpcPort()    const noexcept { return grpc_port_resolved_; }
    uint16_t MetricsPort() const noexcept { return metrics_port_resolved_; }

    // Start the accept loops. Returns immediately; the threads run until
    // Stop() / dtor.
    void Start();

    // Block until Stop() or a SIGTERM/SIGINT signal arrives. Returns the
    // signal number if interrupted, 0 for an explicit Stop().
    int  Wait();

    // Drop both listeners and join the worker threads. Idempotent.
    void Stop();

   private:
    void GrpcAcceptLoop();
    void MetricsAcceptLoop();
    void HandleMetricsConnection(int fd);

    Options              opts_;
    bool                 ok_ = false;
    std::string          error_;

    int                  grpc_fd_    = -1;
    int                  metrics_fd_ = -1;
    uint16_t             grpc_port_resolved_    = 0;
    uint16_t             metrics_port_resolved_ = 0;

    std::atomic<bool>    stop_{false};
    std::thread          grpc_thread_;
    std::thread          metrics_thread_;

    std::mutex                wait_mu_;
    std::condition_variable   wait_cv_;
    int                       signal_received_ = 0;
};

}  // namespace kvcache::node::runtime
