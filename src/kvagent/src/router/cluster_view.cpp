// LLD §4.1 / §4.2 — ClusterViewWatcher implementation.
#include "router/cluster_view.h"

#include <cctype>

#include <nlohmann/json.hpp>

namespace kvcache::agent::router {

std::optional<std::vector<std::string>> ClusterViewWatcher::ParseNodeIds(
    const std::string& json_body) {
    if (json_body.empty()) {
        // Key absent (no leader / fresh cluster) → an empty set, not an
        // error. The resolver will miss and the engine recomputes.
        return std::vector<std::string>{};
    }
    nlohmann::json j = nlohmann::json::parse(json_body, /*cb=*/nullptr,
                                             /*allow_exceptions=*/false);
    if (j.is_discarded() || !j.is_object()) return std::nullopt;

    auto it = j.find("nodes");
    if (it == j.end() || !it->is_array()) return std::nullopt;

    std::vector<std::string> ids;
    ids.reserve(it->size());
    for (const auto& node : *it) {
        if (!node.is_object()) continue;
        auto nid = node.find("node_id");
        if (nid == node.end() || !nid->is_string()) continue;
        std::string id = nid->get<std::string>();
        if (id.empty()) continue;
        // Phase A2.1 — skip DRAINING nodes so the HRW resolver stops
        // sending NEW prefixes to a node the operator is draining. We
        // match case-insensitively on the substring "drain" so both
        // the CP overlay's "draining" and the proto enum's
        // "NODE_DRAINING" are caught. In-flight work isn't affected —
        // the engine's existing pulls/seals to that node continue;
        // only fresh resolution avoids it.
        if (auto st = node.find("state");
            st != node.end() && st->is_string()) {
            std::string state = st->get<std::string>();
            for (auto& c : state) c = static_cast<char>(std::tolower(c));
            if (state.find("drain") != std::string::npos) continue;
        }
        ids.push_back(std::move(id));
    }
    return ids;
}

int ClusterViewWatcher::RefreshOnce() {
    std::optional<std::string> body;
    try {
        body = opts_.loader ? opts_.loader() : std::nullopt;
    } catch (...) {
        body = std::nullopt;
    }
    if (!body) {  // load error — keep the last-good set
        refreshes_failed_.fetch_add(1, std::memory_order_relaxed);
        return -1;
    }
    auto ids = ParseNodeIds(*body);
    if (!ids) {  // malformed JSON — keep the last-good set
        refreshes_failed_.fetch_add(1, std::memory_order_relaxed);
        return -1;
    }
    resolver_.SetNodes(*ids);
    refreshes_ok_.fetch_add(1, std::memory_order_relaxed);
    last_node_count_.store(ids->size(), std::memory_order_relaxed);
    return static_cast<int>(ids->size());
}

void ClusterViewWatcher::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;  // already on
    thread_ = std::thread([this] { RefreshLoop(); });
}

void ClusterViewWatcher::Stop() {
    running_.store(false, std::memory_order_relaxed);
    {
        // Wake the loop so it observes running_=false and exits its wait
        // instead of blocking up to a full poll interval.
        std::lock_guard<std::mutex> lk(wake_mu_);
        refresh_requested_ = true;
    }
    wake_cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

ClusterViewWatcher::~ClusterViewWatcher() { Stop(); }

void ClusterViewWatcher::RequestRefresh() {
    {
        std::lock_guard<std::mutex> lk(wake_mu_);
        refresh_requested_ = true;
    }
    wake_cv_.notify_one();
}

void ClusterViewWatcher::RefreshLoop() {
    // Refresh immediately on start so the resolver has a node set before
    // the first interval elapses, then on each tick OR each external
    // RequestRefresh (whichever comes first). Waiting on the cv — rather
    // than sleeping in slices — means a watch-driven RequestRefresh wakes
    // us within microseconds, while the interval timeout still fires the
    // periodic poll as a safety net.
    RefreshOnce();
    while (running_.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lk(wake_mu_);
            wake_cv_.wait_for(lk, opts_.refresh_interval,
                              [this] { return refresh_requested_; });
            refresh_requested_ = false;
        }
        if (!running_.load(std::memory_order_relaxed)) break;
        RefreshOnce();
    }
}

ClusterViewWatcher::Stats ClusterViewWatcher::SnapshotStats() const {
    return Stats{
        .refreshes_ok     = refreshes_ok_.load(std::memory_order_relaxed),
        .refreshes_failed = refreshes_failed_.load(std::memory_order_relaxed),
        .last_node_count  = last_node_count_.load(std::memory_order_relaxed),
    };
}

}  // namespace kvcache::agent::router
