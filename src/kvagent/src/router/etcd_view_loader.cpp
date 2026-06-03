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

// ----- Phase A1.11 — Watch subscription ------------------------------------

EtcdViewSubscription::EtcdViewSubscription(
    kvcache::node::cluster::IEtcdClient& etcd,
    std::function<void()> on_change, std::string key)
    : etcd_(etcd), on_change_(std::move(on_change)), key_(std::move(key)) {}

EtcdViewSubscription::~EtcdViewSubscription() { Stop(); }

void EtcdViewSubscription::Start() {
    if (watching_) return;
    // WatchPrefix on the exact key (a key is a prefix of itself) delivers
    // both Put (view republished) and Delete (view key expired with the
    // leader's lease) events. We don't inspect the event — any change to
    // the view key means "re-read it", which on_change does.
    handle_ = etcd_.WatchPrefix(
        key_, [this](const kvcache::node::cluster::WatchEvent&) {
            if (on_change_) on_change_();
        });
    watching_ = true;
}

void EtcdViewSubscription::Stop() {
    if (watching_) {
        etcd_.Unwatch(handle_);
        watching_ = false;
    }
}

}  // namespace kvcache::agent::router
