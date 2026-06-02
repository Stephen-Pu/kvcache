// LLD §4.1 — etcd-backed ViewLoader implementation.
#include "router/etcd_view_loader.h"

#include <utility>

namespace kvcache::agent::router {

ViewLoader MakeEtcdViewLoader(kvcache::node::cluster::IEtcdClient& etcd,
                              std::string key) {
    // Capture &etcd (caller guarantees lifetime) + key by value.
    return [&etcd, key = std::move(key)]() -> std::optional<std::string> {
        std::string err;
        auto kv = etcd.Get(key, &err);
        if (!err.empty()) {
            // etcd unreachable / transport error → signal load failure so
            // the watcher keeps the last-good node set.
            return std::nullopt;
        }
        if (!kv) {
            // Key genuinely absent (no leader has published a view yet) →
            // empty body → ParseNodeIds yields an empty set.
            return std::string{};
        }
        return kv->value;
    };
}

}  // namespace kvcache::agent::router
