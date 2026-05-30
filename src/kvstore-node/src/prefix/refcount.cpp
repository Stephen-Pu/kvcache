// LLD §3.2 — Refcount: the entire hot-path implementation is inline in the
// header. This TU exists so the build system has something to compile and so
// the under-flow reporter can be defined out-of-line — keeping the logging
// facade dependency out of the per-leaf header that ~every other TU includes.
#include "prefix/refcount.h"

#include <cstdio>

#include "logging.h"

namespace kvcache::node::prefix {

void Refcount::ReportUnderflow(const Refcount* self) noexcept {
    // Phase O-2: route under-flow through the kvcache::log facade with a
    // dedicated "refcount" subsystem so operators can grep by name. We
    // include the leaf address so post-mortem reconstruction can match the
    // log line to the ART leaf via heap dumps. Error level is right: the
    // counter has wrapped, the next Release will under-flow again, and
    // some caller is missing an Acquire.
    char buf[96];
    std::snprintf(buf, sizeof(buf),
                  "Release() under-flow detected at leaf %p — missing "
                  "Acquire pairing", static_cast<const void*>(self));
    ::kvcache::log::Get("refcount").Error(buf);
}

}  // namespace kvcache::node::prefix
