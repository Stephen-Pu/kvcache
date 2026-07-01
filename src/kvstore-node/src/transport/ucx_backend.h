// LLD §3.5 D-NET-1 — UCX backend for the NIXL transport abstraction (Phase A1).
//
// TcpBackend gave real cross-node transfer with no RDMA dependency. UcxBackend
// is the production RDMA data path: it speaks UCP (UCX's tagged/RMA API), which
// auto-selects the fastest available transport at runtime — InfiniBand / RoCE
// verbs, shared memory, or TCP — with zero code change. On a host with a
// Mellanox NIC it does true one-sided RDMA; on a plain VM (or this dev box) it
// falls back to UCX's TCP / posix-shm transport, so the exact same code path is
// exercisable without special hardware.
//
// Model (mirrors TcpBackend's MR-export/import contract):
//   * RegisterRegion  -> ucp_mem_map on the caller's buffer, yielding a memh.
//   * ExportMr        -> pack {this worker's address, ucp_rkey for the memh,
//                        the region's virtual address, its byte length} into
//                        the opaque RemoteMrDescriptor. A peer imports this to
//                        RMA into/out of the region.
//   * ImportRemoteMr  -> create a ucp_ep to the peer worker + unpack its rkey;
//                        the returned MrKey names the *remote* region.
//   * Pull (server-pull) -> ucp_get_nbx: one-sided GET from the remote region
//                        into a local buffer. This is the KV read path.
//   * Push            -> ucp_put_nbx + flush: one-sided PUT into a remote region.
//
// Completion: a dedicated progress thread drives ucp_worker_progress so the
// passive (target) side advances and request callbacks fire. Pull/Push submit a
// non-blocking op with a completion callback and block until it fires (or times
// out) — matching the synchronous-on-issue semantics of the loopback/TCP
// backends, so Wait() is a formality.
//
// Compiled only under KVCACHE_ENABLE_UCX (CMake finds UCX via pkg-config and
// defines KVCACHE_HAVE_UCX); without it CreateUcxBackend returns an error and
// the rest of the transport layer builds unchanged.
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "transport/nixl_wrapper.h"

namespace kvcache::node::transport {

// Factory entry exposed to CreateBackend (defined in ucx_backend.cpp). Returns
// nullptr + *err if UCX isn't compiled in or ucp initialisation fails.
std::unique_ptr<INixlBackend> CreateUcxBackend(const BackendOptions& opts,
                                               std::string* err);

}  // namespace kvcache::node::transport
