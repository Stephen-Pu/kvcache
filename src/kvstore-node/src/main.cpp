// kvstore-node entrypoint.
//
// Phase M-1 scope: bind the NodeData gRPC service (LLD §3) onto the
// container's grpc port. The agent talks to the binary through this
// service; metadata flows over gRPC, KV bytes flow over NIXL.
//
// What this binary does today:
//
//   1. Parses --config / --grpc-port / --metrics-port flags. The
//      operator-emitted ConfigMap path is stored for future use; the
//      consumer isn't wired yet (Phase M-2 reads it).
//   2. Opens a kv_ctx_t for the "default" tenant — the agent's
//      auth context is bound at ctx-open time inside libkvcache.
//      For Phase M-1 we use a single default ctx for all RPCs.
//   3. Starts a NodeData gRPC server bound to the grpc port and a
//      NodeRuntime running the metrics / healthz HTTP server on
//      the metrics port. NodeRuntime is told to skip the grpc-port
//      placeholder so the grpc::Server can claim it.
//   4. Blocks on SIGTERM (K8s pod evict) / SIGINT (Ctrl-C).
//
// Builds without grpc still produce a working binary — the grpc
// section is gated on KVCACHE_HAVE_GRPC. Without grpc the binary
// falls back to the L-1 accept-and-close placeholder (pods still
// pass readiness; the agent just can't talk to it).
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "runtime/node_runtime.h"

#if defined(KVCACHE_HAVE_GRPC)
#include "kvcache/kv_abi.h"
#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#endif

namespace {

void Usage(const char* argv0) {
    std::fprintf(stderr,
        "usage: %s [--config PATH] [--grpc-port N] [--metrics-port N]\n",
        argv0);
}

bool TryFlag(int& i, int argc, char** argv,
              const char* flag, std::string* out) {
    if (std::strcmp(argv[i], flag) != 0) return false;
    if (i + 1 >= argc) return false;
    *out = argv[i + 1];
    ++i;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    kvcache::node::runtime::NodeRuntime::Options o;
    std::string grpc_port_s    = "7000";
    std::string metrics_port_s = "9090";
    for (int i = 1; i < argc; ++i) {
        if (TryFlag(i, argc, argv, "--config",       &o.config_path)) continue;
        if (TryFlag(i, argc, argv, "--grpc-port",    &grpc_port_s))   continue;
        if (TryFlag(i, argc, argv, "--metrics-port", &metrics_port_s)) continue;
        if (std::strcmp(argv[i], "-h") == 0 ||
            std::strcmp(argv[i], "--help") == 0) {
            Usage(argv[0]);
            return 0;
        }
        std::fprintf(stderr, "unknown arg: %s\n", argv[i]);
        Usage(argv[0]);
        return 2;
    }
    o.grpc_port    = static_cast<uint16_t>(std::atoi(grpc_port_s.c_str()));
    o.metrics_port = static_cast<uint16_t>(std::atoi(metrics_port_s.c_str()));

    std::fprintf(stderr,
        "kvstore-node: starting (config=%s grpc=%u metrics=%u)\n",
        o.config_path.c_str(), o.grpc_port, o.metrics_port);

#if defined(KVCACHE_HAVE_GRPC)
    // Open a default kv_ctx_t for the agent — Phase M-2 will cache one
    // per (tenant, model) keyed off the request fields. The default
    // tenant_id / model_id are placeholders today.
    kv_ctx_config_t cfg{};
    cfg.abi_version    = KVCACHE_ABI_VERSION;
    cfg.agent_endpoint = nullptr;
    cfg.tenant_id      = "kvstore-node-default";
    cfg.model_id       = "kvstore-node-default";
    cfg.flags          = 0;
    kv_ctx_t* ctx = nullptr;
    if (const int rc = kv_ctx_open(&cfg, &ctx); rc != 0 || !ctx) {
        std::fprintf(stderr,
            "kvstore-node: kv_ctx_open failed (status=%d)\n", rc);
        return 1;
    }

    kvcache::node::grpc_server::NodeDataServiceImpl svc(ctx);
    kvcache::node::grpc_server::GrpcServer::Options grpc_opts;
    grpc_opts.bind_host = o.bind_host;
    grpc_opts.port      = o.grpc_port;
    kvcache::node::grpc_server::GrpcServer grpc(grpc_opts, &svc);
    if (!grpc.Ok()) {
        std::fprintf(stderr, "kvstore-node: grpc: %s\n", grpc.error().c_str());
        kv_ctx_close(ctx);
        return 1;
    }
    o.skip_grpc_listener = true;
    o.grpc_port          = grpc.BoundPort();  // for log clarity
#endif

    kvcache::node::runtime::NodeRuntime rt(o);
    if (!rt.Ok()) {
        std::fprintf(stderr, "kvstore-node: %s\n", rt.error().c_str());
#if defined(KVCACHE_HAVE_GRPC)
        kv_ctx_close(ctx);
#endif
        return 1;
    }
    rt.Start();
    std::fprintf(stderr,
        "kvstore-node: listening (grpc=%u metrics=%u); SIGTERM to stop\n",
        rt.GrpcPort(), rt.MetricsPort());
    const int sig = rt.Wait();
    std::fprintf(stderr, "kvstore-node: stopped (signal=%d)\n", sig);

#if defined(KVCACHE_HAVE_GRPC)
    grpc.Stop();
    kv_ctx_close(ctx);
#endif
    return 0;
}
