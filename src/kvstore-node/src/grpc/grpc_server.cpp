// Phase M-1 — grpc::Server wrapper. See grpc_server.h.
#include "grpc/grpc_server.h"

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include <string>

namespace kvcache::node::grpc_server {

GrpcServer::GrpcServer(const Options& opts, ::grpc::Service* service) {
    ::grpc::ServerBuilder builder;
    const std::string addr = opts.bind_host + ":" + std::to_string(opts.port);
    int selected_port = 0;
    // AddListeningPort writes the actually-bound port through the
    // optional out-param. For port=0 this is how we learn the OS-picked
    // value.
    builder.AddListeningPort(addr, ::grpc::InsecureServerCredentials(),
                                &selected_port);
    builder.RegisterService(service);
    server_ = builder.BuildAndStart();
    if (!server_) {
        error_ = "grpc::ServerBuilder::BuildAndStart returned null";
        return;
    }
    if (selected_port <= 0) {
        error_ = "grpc::ServerBuilder failed to bind " + addr;
        server_.reset();
        return;
    }
    bound_port_ = static_cast<uint16_t>(selected_port);
}

GrpcServer::~GrpcServer() { Stop(); }

void GrpcServer::Stop() {
    if (server_) {
        server_->Shutdown();
        server_.reset();
    }
}

}  // namespace kvcache::node::grpc_server
