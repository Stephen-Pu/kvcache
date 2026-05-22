// Phase M-1 — NodeData gRPC handlers, forwarding to the C ABI.
//
// Each handler is a thin proto<->C-struct translator plus a single
// kv_* call. Error mapping follows the convention:
//
//   KV_OK              → grpc::Status::OK
//   KV_E_NOT_FOUND     → grpc::Status::NOT_FOUND
//   KV_E_INVAL         → grpc::Status::INVALID_ARGUMENT
//   anything else      → grpc::Status::INTERNAL (with kv_status_str text)
//
// The intent matches what an agent would do if it hit the C ABI directly,
// so test failures look identical whether you go through the wire or not.
#include "grpc/node_data_service.h"

#include <cstring>
#include <string>

#include "kvcache/kv_errors.h"
#include "kvcache/kv_types.h"

namespace kvcache::node::grpc_server {

namespace {

::grpc::Status ToGrpcStatus(int rc, const char* op) {
    switch (rc) {
        case KV_OK:           return ::grpc::Status::OK;
        case KV_E_NOT_FOUND:  return {::grpc::StatusCode::NOT_FOUND,
                                       std::string(op) + ": not found"};
        case KV_E_INVAL:      return {::grpc::StatusCode::INVALID_ARGUMENT,
                                       std::string(op) + ": invalid argument"};
        case KV_E_NOMEM:      return {::grpc::StatusCode::RESOURCE_EXHAUSTED,
                                       std::string(op) + ": out of memory"};
        case KV_E_TIMEOUT:    return {::grpc::StatusCode::DEADLINE_EXCEEDED,
                                       std::string(op) + ": timeout"};
        case KV_E_PERM:       return {::grpc::StatusCode::PERMISSION_DENIED,
                                       std::string(op) + ": permission denied"};
        case KV_E_QUOTA:      return {::grpc::StatusCode::RESOURCE_EXHAUSTED,
                                       std::string(op) + ": quota exceeded"};
        case KV_E_SEALED:     return {::grpc::StatusCode::FAILED_PRECONDITION,
                                       std::string(op) + ": handle already sealed"};
        case KV_E_NOT_SEALED: return {::grpc::StatusCode::FAILED_PRECONDITION,
                                       std::string(op) + ": handle not sealed"};
        default:
            return {::grpc::StatusCode::INTERNAL,
                      std::string(op) + ": " + kv_status_str(rc) +
                      " (status=" + std::to_string(rc) + ")"};
    }
}

void FromProtoLocator(const kvcache::proto::Locator& in, kv_locator_t* out) {
    std::memset(out, 0, sizeof(*out));
    const std::string& tid = in.tenant_id();
    std::memcpy(out->tenant_id, tid.data(),
                  std::min<std::size_t>(tid.size(), sizeof(out->tenant_id)));
    out->model_id_hash = in.model_id_hash();
    const std::string& ph = in.prefix_hash();
    std::memcpy(out->prefix_hash, ph.data(),
                  std::min<std::size_t>(ph.size(), sizeof(out->prefix_hash)));
    if (in.has_range()) {
        const auto& r = in.range();
        out->range.layer_start = static_cast<uint16_t>(r.layer_start());
        out->range.layer_count = static_cast<uint16_t>(r.layer_count());
        out->range.head_start  = static_cast<uint16_t>(r.head_start());
        out->range.head_count  = static_cast<uint16_t>(r.head_count());
        out->range.token_start = r.token_start();
        out->range.token_count = r.token_count();
    }
    out->version = in.version() == 0 ? 1 : in.version();
    out->flags   = in.flags();
}

void ToProtoLocator(const kv_locator_t& in, kvcache::proto::Locator* out) {
    out->set_tenant_id(std::string(reinterpret_cast<const char*>(in.tenant_id),
                                       sizeof(in.tenant_id)));
    out->set_model_id_hash(in.model_id_hash);
    out->set_prefix_hash(std::string(reinterpret_cast<const char*>(in.prefix_hash),
                                         sizeof(in.prefix_hash)));
    auto* r = out->mutable_range();
    r->set_layer_start(in.range.layer_start);
    r->set_layer_count(in.range.layer_count);
    r->set_head_start (in.range.head_start);
    r->set_head_count (in.range.head_count);
    r->set_token_start(in.range.token_start);
    r->set_token_count(in.range.token_count);
    out->set_version(in.version);
    out->set_flags  (in.flags);
}

}  // namespace

::grpc::Status NodeDataServiceImpl::Lookup(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::LookupRequest* request,
    kvcache::proto::LookupResponse*      response) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};

    std::vector<uint32_t> tokens(request->tokens().begin(),
                                   request->tokens().end());
    kv_locator_t meta{};
    kv_handle_t  handle  = 0;
    uint32_t     matched = 0;
    int rc = kv_lookup(ctx_, tokens.data(), tokens.size(),
                         &meta, &handle, &matched);
    if (rc == KV_E_NOT_FOUND) {
        response->set_hit(false);
        return ::grpc::Status::OK;
    }
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_lookup");

    response->set_hit(true);
    ToProtoLocator(meta, response->mutable_locator());
    response->set_server_handle(handle);
    response->set_matched_tokens(matched);
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Reserve(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::ReserveRequest* request,
    kvcache::proto::ReserveResponse*      response) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    kv_locator_t loc{};
    FromProtoLocator(request->locator(), &loc);

    kv_handle_t      h    = 0;
    kv_buffer_desc_t slot{};
    int rc = kv_reserve(ctx_, &loc, request->bytes(), &h, &slot);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_reserve");

    response->set_server_handle(h);
    response->set_slot_iova(reinterpret_cast<uint64_t>(slot.addr));
    response->set_slot_bytes(slot.len);
    response->set_mr_key(slot.mr_key);
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Publish(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::PublishRequest* request,
    kvcache::proto::PublishResponse*      response) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    // Send an empty buffer descriptor since the caller has already
    // written into the previously-reserved slot.
    kv_buffer_desc_t empty{};
    int rc = kv_publish(ctx_, request->server_handle(), empty,
                          request->watermark());
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_publish");
    response->set_queue_position(0);  // backpressure metric — Phase M-2
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Fetch(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::FetchRequest* request,
    kvcache::proto::FetchResponse*      response) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    kv_buffer_desc_t dst{};
    dst.addr     = reinterpret_cast<void*>(request->dst_iova());
    dst.len      = request->dst_bytes();
    dst.mem_type = 0;  // KV_MEM_HOST
    dst.mr_key   = request->dst_mr_key();

    // The proto carries `repeated Range ranges`; the C ABI takes a flat
    // C array. We translate them 1:1 — empty list means "whole prefix".
    std::vector<kv_range_t> ranges;
    ranges.reserve(request->ranges_size());
    for (const auto& r : request->ranges()) {
        kv_range_t out{};
        out.layer_start = static_cast<uint16_t>(r.layer_start());
        out.layer_count = static_cast<uint16_t>(r.layer_count());
        out.head_start  = static_cast<uint16_t>(r.head_start());
        out.head_count  = static_cast<uint16_t>(r.head_count());
        out.token_start = r.token_start();
        out.token_count = r.token_count();
        ranges.push_back(out);
    }
    kv_completion_t cid = 0;
    int rc = kv_fetch(ctx_, request->server_handle(),
                        ranges.empty() ? nullptr : ranges.data(),
                        ranges.size(), dst, &cid);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_fetch");
    rc = kv_wait(ctx_, cid, 5000);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_wait");
    response->set_completion_id(cid);
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Seal(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::SealRequest* request,
    kvcache::proto::SealResponse*      response) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    // SealRequest in the proto carries only server_handle. The tokens
    // are bound at Reserve-time inside the server. The C ABI's kv_seal
    // wants the tokens at this point — we recover them from the
    // server-side handle bookkeeping.
    //
    // For Phase M-1 we accept the limitation that the wire SealRequest
    // doesn't carry the token list: tests Reserve+Publish, then the
    // server's HeadlessNode already knows the tokens from the locator's
    // prefix_hash and the original write path. Phase M-2 will either
    // extend the proto with `repeated uint32 tokens` here or move seal
    // bookkeeping fully server-side.
    //
    // Today's kv_seal signature requires the tokens. For the test we
    // pass an empty token list — kv_seal in the loopback HeadlessNode
    // currently treats an empty list as "use the locator's prefix path",
    // which is what we want. If that contract changes the test will
    // catch it.
    int rc = kv_seal(ctx_, request->server_handle(), nullptr, 0);
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_seal");
    response->mutable_locator();  // empty Locator; H-3 will fill in if needed
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Release(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::ReleaseRequest* request,
    kvcache::proto::ReleaseResponse*      /*response*/) {
    if (!ctx_) return {::grpc::StatusCode::FAILED_PRECONDITION, "no ctx"};
    int rc = kv_release(ctx_, request->server_handle());
    if (rc != KV_OK) return ToGrpcStatus(rc, "kv_release");
    return ::grpc::Status::OK;
}

::grpc::Status NodeDataServiceImpl::Subscribe(
    ::grpc::ServerContext* /*context*/,
    const kvcache::proto::SubscribeRequest* /*request*/,
    ::grpc::ServerWriter<kvcache::proto::Event>* /*writer*/) {
    return {::grpc::StatusCode::UNIMPLEMENTED,
              "Subscribe: server-side push deferred to Phase M-2"};
}

}  // namespace kvcache::node::grpc_server
