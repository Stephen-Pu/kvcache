// LLD §4.1 — Real etcd v3 client over the HTTP/JSON gateway (Phase F-1).
//
// Each public method translates to a single synchronous HTTP POST against
// `<endpoint>/v3/...`. The libcurl easy handle is serialised under one
// mutex; concurrent callers fan out at the I/O level rather than the
// connection level. That's enough for control-plane traffic.
//
// Failure model:
//   * Network / transport errors → return false / nullopt with `*err` set
//     to the libcurl error string.
//   * HTTP non-2xx → same, with the response body in `*err` (etcd's JSON
//     error message is much more useful than the status code alone).
//   * Successful etcd-but-no-such-key → returns false / nullopt with empty
//     `*err`. That's the same convention InMemoryEtcdClient uses.
#include "cluster/etcd_client.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace kvcache::node::cluster {

namespace {

using json = nlohmann::json;

// -------- libcurl global init ----------------------------------------------
//
// curl_global_init must be called once per process before any easy_handle is
// created. We do it on first HttpEtcdClient::Create.
std::once_flag g_curl_once;

void CurlGlobalInit() {
    std::call_once(g_curl_once, [] {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        // We deliberately do not register a cleanup hook — libcurl's
        // documented contract is that global_cleanup happens at process
        // exit, and we don't want to race with destructors of other
        // libraries that also use libcurl.
    });
}

// -------- base64 (etcd v3 JSON requires base64-encoded keys/values) --------
//
// Tiny standard base64 encoder / decoder. Etcd uses standard (not URL-safe)
// alphabet with `=` padding.
constexpr char kB64Tab[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string B64Encode(const std::string& in) {
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    int val = 0, valb = -6;
    for (unsigned char c : in) {
        val = (val << 8) | c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(kB64Tab[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(kB64Tab[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

std::string B64Decode(const std::string& in) {
    static int8_t T[128];
    static std::once_flag once;
    std::call_once(once, [] {
        std::memset(T, -1, sizeof(T));
        for (int i = 0; i < 64; ++i)
            T[static_cast<unsigned char>(kB64Tab[i])] = static_cast<int8_t>(i);
    });
    std::string out;
    out.reserve(in.size() * 3 / 4);
    int val = 0, valb = -8;
    for (unsigned char c : in) {
        if (c >= 128) continue;
        int8_t d = T[c];
        if (d < 0) continue;
        val = (val << 6) | d;
        valb += 6;
        if (valb >= 0) {
            out.push_back(static_cast<char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return out;
}

// -------- libcurl write callback into a std::string ------------------------
size_t WriteToString(void* ptr, size_t size, size_t nmemb, void* user) {
    auto* out = static_cast<std::string*>(user);
    out->append(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

}  // namespace

// ===========================================================================
// HttpEtcdClient::Impl
// ===========================================================================

struct HttpEtcdClient::Impl {
    HttpEtcdClient::Options opts;
    CURL*                   curl  = nullptr;
    mutable std::mutex      curl_mu;  // serialises easy-handle access

    // Watcher state. Each WatchPrefix call registers a record; a single
    // background thread polls all of them.
    struct PollWatcher {
        std::string                                   prefix;
        WatchCallback                                  cb;
        Revision                                       last_seen_rev = 0;
        std::unordered_map<std::string, KeyValue>     last_state;
    };

    std::mutex                                   w_mu;
    std::unordered_map<WatchHandle, PollWatcher> watchers;
    std::atomic<WatchHandle>                     next_watch{1};
    std::atomic<bool>                            stop{false};
    std::thread                                  poller;
};

// ---- HTTP POST helper -----------------------------------------------------

namespace {

bool PostJson(HttpEtcdClient::Impl& impl, const std::string& path,
              const json& body, json* out_response, std::string* err) {
    const std::string url = impl.opts.endpoint + path;
    const std::string body_str = body.dump();
    std::string resp;
    long status = 0;
    char errbuf[CURL_ERROR_SIZE] = {0};

    std::lock_guard<std::mutex> lk(impl.curl_mu);
    auto* c = impl.curl;
    curl_easy_reset(c);
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body_str.c_str());
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(body_str.size()));
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, WriteToString);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(c, CURLOPT_ERRORBUFFER, errbuf);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS,
                      static_cast<long>(impl.opts.dial_timeout.count()));
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS,
                      static_cast<long>(impl.opts.dial_timeout.count()));
    if (!impl.opts.ca_pem_path.empty()) {
        curl_easy_setopt(c, CURLOPT_CAINFO, impl.opts.ca_pem_path.c_str());
    }
    if (!impl.opts.client_cert_pem_path.empty()) {
        curl_easy_setopt(c, CURLOPT_SSLCERT, impl.opts.client_cert_pem_path.c_str());
    }
    if (!impl.opts.client_key_pem_path.empty()) {
        curl_easy_setopt(c, CURLOPT_SSLKEY, impl.opts.client_key_pem_path.c_str());
    }
    struct curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);

    const CURLcode rc = curl_easy_perform(c);
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);
    curl_slist_free_all(hdrs);

    if (rc != CURLE_OK) {
        if (err) *err = std::string("etcd HTTP ") + path + ": " +
                          (errbuf[0] ? errbuf : curl_easy_strerror(rc));
        return false;
    }
    if (status < 200 || status >= 300) {
        if (err) *err = std::string("etcd HTTP ") + path + " status " +
                          std::to_string(status) + ": " + resp;
        return false;
    }
    try {
        *out_response = json::parse(resp);
    } catch (const std::exception& e) {
        if (err) *err = std::string("etcd parse: ") + e.what() + " body=" + resp;
        return false;
    }
    return true;
}

// Helper: pull "mod_revision" / "create_revision" / "lease" / "key" / "value"
// out of a single kv object in the response. Etcd uses strings for 64-bit
// ints in JSON.
KeyValue ParseKv(const json& kv) {
    KeyValue r;
    if (kv.contains("key"))   r.key   = B64Decode(kv["key"].get<std::string>());
    if (kv.contains("value")) r.value = B64Decode(kv["value"].get<std::string>());
    auto opt_int = [&](const char* k) -> uint64_t {
        if (!kv.contains(k)) return 0;
        const auto& v = kv[k];
        if (v.is_string()) return std::stoull(v.get<std::string>());
        return v.get<uint64_t>();
    };
    r.mod_revision    = opt_int("mod_revision");
    r.create_revision = opt_int("create_revision");
    r.lease           = static_cast<LeaseId>(opt_int("lease"));
    return r;
}

// `range_end` for "all keys with this prefix" is the prefix with the last
// byte incremented (etcd v3 convention).
std::string PrefixRangeEnd(const std::string& prefix) {
    std::string e = prefix;
    if (e.empty()) return std::string(1, '\0');
    for (std::size_t i = e.size(); i > 0; ) {
        --i;
        if (static_cast<unsigned char>(e[i]) < 0xff) {
            ++e[i];
            e.resize(i + 1);
            return e;
        }
    }
    return std::string{};
}

}  // namespace

// ===========================================================================
// HttpEtcdClient
// ===========================================================================

std::unique_ptr<HttpEtcdClient>
HttpEtcdClient::Create(const Options& opts, std::string* err) {
    if (opts.endpoint.empty()) {
        if (err) *err = "HttpEtcdClient: endpoint required";
        return nullptr;
    }
    CurlGlobalInit();
    auto self = std::unique_ptr<HttpEtcdClient>(new HttpEtcdClient());
    self->impl_ = std::make_unique<Impl>();
    self->impl_->opts = opts;
    self->impl_->curl = curl_easy_init();
    if (!self->impl_->curl) {
        if (err) *err = "HttpEtcdClient: curl_easy_init failed";
        return nullptr;
    }
    // Smoke-test the endpoint with a benign Range (empty key, empty
    // range_end). On success, etcd returns a header-only response. This
    // gives Create() a clear failure mode if etcd is unreachable.
    json req{{"key", B64Encode(std::string("\0", 1))}};
    json resp;
    if (!PostJson(*self->impl_, "/v3/kv/range", req, &resp, err)) {
        return nullptr;
    }
    return self;
}

HttpEtcdClient::~HttpEtcdClient() {
    if (!impl_) return;
    impl_->stop.store(true, std::memory_order_release);
    if (impl_->poller.joinable()) impl_->poller.join();
    if (impl_->curl) curl_easy_cleanup(impl_->curl);
}

bool HttpEtcdClient::Put(const std::string& key, const std::string& value,
                          LeaseId lease, Revision* out_rev, std::string* err) {
    json req{
        {"key",   B64Encode(key)},
        {"value", B64Encode(value)},
    };
    if (lease != kNoLease) req["lease"] = std::to_string(lease);
    json resp;
    if (!PostJson(*impl_, "/v3/kv/put", req, &resp, err)) return false;
    if (out_rev && resp.contains("header")) {
        const auto& h = resp["header"];
        if (h.contains("revision")) {
            *out_rev = std::stoull(h["revision"].get<std::string>());
        }
    }
    return true;
}

std::optional<KeyValue> HttpEtcdClient::Get(const std::string& key,
                                              std::string* err) {
    json req{{"key", B64Encode(key)}};
    json resp;
    if (!PostJson(*impl_, "/v3/kv/range", req, &resp, err)) return std::nullopt;
    if (!resp.contains("kvs") || resp["kvs"].empty()) return std::nullopt;
    return ParseKv(resp["kvs"][0]);
}

std::vector<KeyValue> HttpEtcdClient::GetPrefix(const std::string& prefix,
                                                  std::string* err) {
    json req{
        {"key",       B64Encode(prefix)},
        {"range_end", B64Encode(PrefixRangeEnd(prefix))},
    };
    json resp;
    std::vector<KeyValue> out;
    if (!PostJson(*impl_, "/v3/kv/range", req, &resp, err)) return out;
    if (!resp.contains("kvs")) return out;
    out.reserve(resp["kvs"].size());
    for (const auto& kv : resp["kvs"]) out.push_back(ParseKv(kv));
    return out;
}

bool HttpEtcdClient::Delete(const std::string& key, std::string* err) {
    json req{{"key", B64Encode(key)}};
    json resp;
    if (!PostJson(*impl_, "/v3/kv/deleterange", req, &resp, err)) return false;
    // etcd returns `deleted` (string) — 0 if the key didn't exist.
    if (resp.contains("deleted")) {
        const std::string n = resp["deleted"].get<std::string>();
        return n != "0";
    }
    return true;
}

bool HttpEtcdClient::PutIfRevision(const std::string& key,
                                     const std::string& value,
                                     Revision expected_rev, LeaseId lease,
                                     Revision* out_rev, std::string* err) {
    // Use a transaction with a single MOD compare. Per etcd docs, comparing
    // a non-existent key with mod_revision=0 succeeds — which matches our
    // "create-if-absent" semantics.
    json put_op{
        {"request_put", json{
            {"key",   B64Encode(key)},
            {"value", B64Encode(value)},
        }}
    };
    if (lease != kNoLease) put_op["request_put"]["lease"] = std::to_string(lease);

    json req{
        {"compare", json::array({
            json{
                {"target",        "MOD"},
                {"key",           B64Encode(key)},
                {"mod_revision",  std::to_string(expected_rev)},
                {"result",        "EQUAL"},
            },
        })},
        {"success", json::array({put_op})},
    };
    json resp;
    if (!PostJson(*impl_, "/v3/kv/txn", req, &resp, err)) return false;
    const bool ok = resp.value("succeeded", false);
    if (!ok) {
        if (err) *err = "etcd: revision mismatch";
        return false;
    }
    if (out_rev && resp.contains("header") && resp["header"].contains("revision")) {
        *out_rev = std::stoull(resp["header"]["revision"].get<std::string>());
    }
    return true;
}

LeaseId HttpEtcdClient::LeaseGrant(uint32_t ttl_seconds, std::string* err) {
    json req{{"TTL", std::to_string(ttl_seconds)}};
    json resp;
    if (!PostJson(*impl_, "/v3/lease/grant", req, &resp, err)) return kNoLease;
    if (!resp.contains("ID")) {
        if (err) *err = "etcd: lease grant response missing ID";
        return kNoLease;
    }
    return static_cast<LeaseId>(std::stoll(resp["ID"].get<std::string>()));
}

bool HttpEtcdClient::LeaseKeepAlive(LeaseId id, std::string* err) {
    // Single-shot keepalive via the unary endpoint (not streaming).
    json req{{"ID", std::to_string(id)}};
    json resp;
    if (!PostJson(*impl_, "/v3/lease/keepalive", req, &resp, err)) return false;
    // etcd returns TTL=0 if the lease was already expired or doesn't exist.
    if (resp.contains("result") && resp["result"].contains("TTL")) {
        const std::string ttl = resp["result"]["TTL"].get<std::string>();
        if (ttl == "0") {
            if (err) *err = "etcd: lease expired or not found";
            return false;
        }
    }
    return true;
}

bool HttpEtcdClient::LeaseRevoke(LeaseId id, std::string* err) {
    json req{{"ID", std::to_string(id)}};
    json resp;
    return PostJson(*impl_, "/v3/lease/revoke", req, &resp, err);
}

uint32_t HttpEtcdClient::LeaseTTLRemaining(LeaseId id) const {
    // /v3/lease/timetolive returns "TTL" (remaining) and "grantedTTL".
    json req{{"ID", std::to_string(id)}};
    json resp;
    std::string err;
    if (!PostJson(*impl_, "/v3/lease/timetolive", req, &resp, &err)) return 0;
    if (!resp.contains("TTL")) return 0;
    const std::string s = resp["TTL"].get<std::string>();
    long long ttl = std::stoll(s);
    return ttl < 0 ? 0u : static_cast<uint32_t>(ttl);
}

// ---- polling watcher ------------------------------------------------------

WatchHandle HttpEtcdClient::WatchPrefix(const std::string& prefix,
                                          WatchCallback cb) {
    std::lock_guard<std::mutex> lk(impl_->w_mu);
    const WatchHandle h = impl_->next_watch.fetch_add(1, std::memory_order_relaxed);

    // Seed the watcher with the current state of the prefix so the first
    // poll only fires events for *changes* (rather than dumping every
    // existing key as a synthetic Put).
    Impl::PollWatcher pw;
    pw.prefix = prefix;
    pw.cb     = std::move(cb);
    std::string err;
    auto initial = GetPrefix(prefix, &err);
    for (const auto& kv : initial) {
        pw.last_state[kv.key] = kv;
        pw.last_seen_rev = std::max(pw.last_seen_rev, kv.mod_revision);
    }
    impl_->watchers[h] = std::move(pw);

    // Spawn the poller lazily on the first watcher.
    if (!impl_->poller.joinable()) {
        impl_->poller = std::thread([this] {
            while (!impl_->stop.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(impl_->opts.watch_poll_interval);
                if (impl_->stop.load(std::memory_order_acquire)) break;

                // Take a snapshot of the watcher set under w_mu, then poll
                // each one outside the lock. New watchers added during the
                // poll just miss this tick.
                std::vector<std::pair<WatchHandle, std::string>> to_poll;
                {
                    std::lock_guard<std::mutex> lk2(impl_->w_mu);
                    to_poll.reserve(impl_->watchers.size());
                    for (const auto& [id, w] : impl_->watchers) {
                        to_poll.emplace_back(id, w.prefix);
                    }
                }
                for (const auto& [id, prefix] : to_poll) {
                    std::string e;
                    auto cur = GetPrefix(prefix, &e);
                    // On transient error, just skip this round.
                    if (!e.empty() && cur.empty()) continue;

                    std::lock_guard<std::mutex> lk2(impl_->w_mu);
                    auto it = impl_->watchers.find(id);
                    if (it == impl_->watchers.end()) continue;
                    auto& w = it->second;

                    // Build the new state map and diff against last.
                    std::unordered_map<std::string, KeyValue> new_state;
                    new_state.reserve(cur.size());
                    for (const auto& kv : cur) new_state[kv.key] = kv;

                    // Puts: new or changed.
                    for (const auto& [k, kv] : new_state) {
                        auto pit = w.last_state.find(k);
                        if (pit == w.last_state.end() ||
                            pit->second.mod_revision != kv.mod_revision) {
                            WatchEvent ev{WatchEventType::kPut, kv,
                                           pit == w.last_state.end()
                                               ? KeyValue{} : pit->second};
                            w.cb(ev);
                        }
                    }
                    // Deletes: in last but not in new.
                    for (const auto& [k, kv] : w.last_state) {
                        if (new_state.find(k) == new_state.end()) {
                            WatchEvent ev{WatchEventType::kDelete, kv, kv};
                            w.cb(ev);
                        }
                    }
                    w.last_state = std::move(new_state);
                }
            }
        });
    }
    return h;
}

void HttpEtcdClient::Unwatch(WatchHandle h) {
    std::lock_guard<std::mutex> lk(impl_->w_mu);
    impl_->watchers.erase(h);
}

}  // namespace kvcache::node::cluster
