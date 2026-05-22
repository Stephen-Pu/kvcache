// Phase M-1 — thin RAII wrapper around grpc::Server for the kvstore-node
// binary. Hides the grpc++ headers from main.cpp / NodeRuntime so they
// don't have to pull the dep transitively.
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

namespace kvcache::node::grpc_server {

class GrpcServer {
   public:
    struct Options {
        std::string bind_host = "0.0.0.0";
        uint16_t    port      = 7000;
    };

    // Construct + bind + start serving. On success Ok() returns true
    // and BoundPort() reports the actually-bound port (resolves port=0).
    // The service ptr is borrowed, not owned — the caller (main.cpp /
    // tests) keeps it alive for the server's lifetime.
    GrpcServer(const Options& opts, ::grpc::Service* service);
    ~GrpcServer();

    GrpcServer(const GrpcServer&)            = delete;
    GrpcServer& operator=(const GrpcServer&) = delete;

    bool                Ok()        const noexcept { return server_ != nullptr; }
    uint16_t            BoundPort() const noexcept { return bound_port_; }
    const std::string&  error()     const noexcept { return error_; }

    // Stops the server, draining in-flight RPCs first. Idempotent.
    void Stop();

   private:
    std::unique_ptr<::grpc::Server> server_;
    uint16_t                        bound_port_ = 0;
    std::string                     error_;
};

}  // namespace kvcache::node::grpc_server
