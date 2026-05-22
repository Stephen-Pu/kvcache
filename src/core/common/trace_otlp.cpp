// Distributed-tracing facade — OTLP/HTTP exporter (Phase J-2).
//
// Spans land on an in-memory queue from the calling thread; a single
// worker thread drains in batches and POSTs an OpenTelemetry
// ExportTraceServiceRequest JSON document to the configured collector.
//
// The wire format is the OTel "1.x" JSON encoding of the protobuf
// messages defined in opentelemetry-proto. We embed only the subset
// our facade actually emits: ResourceSpans / ScopeSpans / Span /
// Status, with attribute values restricted to string / int / double /
// bool — matching what trace.h's Attribute variant carries.
#include "trace.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace kvcache::trace {

namespace {

using json = nlohmann::json;

// libcurl global init: once per process, irrespective of how many
// OtlpHttpExporter instances we spawn.
std::once_flag g_curl_once;
void CurlGlobalInit() {
    std::call_once(g_curl_once, [] {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        // Same convention as http_etcd_client.cpp — we deliberately do
        // not register a cleanup hook; let process teardown handle it.
    });
}

size_t DiscardWrite(void* /*ptr*/, size_t size, size_t nmemb, void* /*user*/) {
    return size * nmemb;  // we don't need the response body
}

// Lower-case hex of `n` bytes from `in`. OTel/JSON requires hex
// (the protobuf wire would be raw bytes; the JSON encoding uses hex).
std::string HexLower(const uint8_t* in, std::size_t n) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.resize(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out[2 * i]     = kHex[(in[i] >> 4) & 0xF];
        out[2 * i + 1] = kHex[in[i] & 0xF];
    }
    return out;
}

// OTel uses ``string`` for 64-bit integer fields in JSON (so JS clients
// don't quietly lose precision). We follow suit.
std::string Uint64Str(uint64_t v) { return std::to_string(v); }

uint64_t TimeToUnixNanos(const TimePoint& tp) {
    // The facade uses std::chrono::steady_clock, which doesn't have an
    // epoch — we anchor to system_clock once at startup so that
    // subsequent spans get a wall-clock unix-nanos approximation good
    // enough for the collector to plot. The offset is process-local; if
    // the system clock jumps the rendered span timeline jumps with it.
    static const auto sys_anchor   = std::chrono::system_clock::now();
    static const auto steady_anchor = Clock::now();
    const auto delta_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        tp - steady_anchor).count();
    const auto sys_anchor_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        sys_anchor.time_since_epoch()).count();
    const int64_t result = sys_anchor_ns + delta_ns;
    return result < 0 ? 0u : static_cast<uint64_t>(result);
}

json AttributeValueToJson(const AttributeValue& v) {
    return std::visit([](const auto& x) -> json {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string>) {
            return json{{"stringValue", x}};
        } else if constexpr (std::is_same_v<T, int64_t>) {
            // OTel wants intValue as a string to dodge JS's 53-bit limit.
            return json{{"intValue", std::to_string(x)}};
        } else if constexpr (std::is_same_v<T, double>) {
            return json{{"doubleValue", x}};
        } else {
            return json{{"boolValue", x}};
        }
    }, v);
}

json AttributesToJson(const std::vector<Attribute>& attrs) {
    json out = json::array();
    for (const auto& a : attrs) {
        out.push_back({{"key", a.key}, {"value", AttributeValueToJson(a.value)}});
    }
    return out;
}

json SpanToJson(const SpanData& s) {
    json out;
    out["traceId"] = HexLower(s.context.trace_id.data(), s.context.trace_id.size());
    out["spanId"]  = HexLower(s.context.span_id.data(),  s.context.span_id.size());
    if (s.parent.IsValid()) {
        out["parentSpanId"] = HexLower(s.parent.span_id.data(),
                                         s.parent.span_id.size());
    }
    out["name"] = s.name;
    // SPAN_KIND_INTERNAL — all our spans are in-process for now.
    out["kind"] = 1;
    out["startTimeUnixNano"] = Uint64Str(TimeToUnixNanos(s.start));
    out["endTimeUnixNano"]   = Uint64Str(TimeToUnixNanos(s.end));
    out["attributes"]        = AttributesToJson(s.attributes);
    json status;
    switch (s.status) {
        case StatusCode::Unset: status["code"] = 0; break;
        case StatusCode::Ok:    status["code"] = 1; break;
        case StatusCode::Error: status["code"] = 2; break;
    }
    if (!s.status_message.empty()) status["message"] = s.status_message;
    out["status"] = status;
    return out;
}

json ResourceJson(const std::string& service_name,
                   const std::string& service_version) {
    json attrs = json::array();
    attrs.push_back({{"key", "service.name"},
                       {"value", json{{"stringValue", service_name}}}});
    if (!service_version.empty()) {
        attrs.push_back({{"key", "service.version"},
                           {"value", json{{"stringValue", service_version}}}});
    }
    return json{{"attributes", attrs}};
}

}  // namespace

// ---- pure encoder -------------------------------------------------------

std::string EncodeOtlpTracesJson(const std::vector<SpanData>& spans,
                                   const std::string& service_name,
                                   const std::string& service_version) {
    json scope_spans = json::array();
    for (const auto& s : spans) {
        scope_spans.push_back(SpanToJson(s));
    }
    json doc;
    doc["resourceSpans"] = json::array();
    doc["resourceSpans"].push_back({
        {"resource",   ResourceJson(service_name, service_version)},
        {"scopeSpans", json::array({
            json{
                {"scope", json{{"name", "kvcache"}}},
                {"spans", scope_spans},
            },
        })},
    });
    return doc.dump();
}

// ---- OtlpHttpExporter::Impl ---------------------------------------------

struct OtlpHttpExporter::Impl {
    Options                  opts;
    CURL*                    curl = nullptr;
    std::mutex               curl_mu;

    std::mutex               queue_mu;
    std::condition_variable  queue_cv;
    std::vector<SpanData>    queue;
    uint64_t                 enqueued_seq  = 0;  // ever-incrementing
    uint64_t                 flushed_seq   = 0;
    std::condition_variable  flushed_cv;

    std::atomic<bool>        stop{false};
    std::thread              worker;
};

namespace {

bool PostBatch(OtlpHttpExporter::Impl& impl,
                const std::vector<SpanData>& batch,
                std::string* err) {
    if (batch.empty()) return true;
    const std::string body = EncodeOtlpTracesJson(
        batch, impl.opts.service_name, impl.opts.service_version);

    std::lock_guard<std::mutex> lk(impl.curl_mu);
    auto* c = impl.curl;
    char errbuf[CURL_ERROR_SIZE] = {0};
    curl_easy_reset(c);
    curl_easy_setopt(c, CURLOPT_URL, impl.opts.endpoint.c_str());
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, DiscardWrite);
    curl_easy_setopt(c, CURLOPT_ERRORBUFFER, errbuf);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS,
                      static_cast<long>(impl.opts.dial_timeout.count()));
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS,
                      static_cast<long>(impl.opts.dial_timeout.count()));
    if (!impl.opts.ca_pem_path.empty())
        curl_easy_setopt(c, CURLOPT_CAINFO, impl.opts.ca_pem_path.c_str());
    if (!impl.opts.client_cert_pem_path.empty())
        curl_easy_setopt(c, CURLOPT_SSLCERT, impl.opts.client_cert_pem_path.c_str());
    if (!impl.opts.client_key_pem_path.empty())
        curl_easy_setopt(c, CURLOPT_SSLKEY, impl.opts.client_key_pem_path.c_str());

    curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);

    const CURLcode rc = curl_easy_perform(c);
    long status = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);
    curl_slist_free_all(hdrs);

    if (rc != CURLE_OK) {
        if (err) *err = std::string("otlp POST: ") +
                          (errbuf[0] ? errbuf : curl_easy_strerror(rc));
        return false;
    }
    if (status < 200 || status >= 300) {
        if (err) *err = "otlp POST status " + std::to_string(status);
        return false;
    }
    return true;
}

}  // namespace

std::unique_ptr<OtlpHttpExporter>
OtlpHttpExporter::Create(const Options& opts, std::string* err) {
    if (opts.endpoint.empty()) {
        if (err) *err = "OtlpHttpExporter: endpoint required";
        return nullptr;
    }
    CurlGlobalInit();
    auto self = std::unique_ptr<OtlpHttpExporter>(new OtlpHttpExporter());
    self->impl_ = std::make_unique<Impl>();
    self->impl_->opts = opts;
    self->impl_->curl = curl_easy_init();
    if (!self->impl_->curl) {
        if (err) *err = "OtlpHttpExporter: curl_easy_init failed";
        return nullptr;
    }
    // Worker thread. Drains the queue in batches and POSTs each one.
    self->impl_->worker = std::thread([impl = self->impl_.get()] {
        std::vector<SpanData> batch;
        batch.reserve(impl->opts.max_batch_size);
        while (true) {
            uint64_t seq_taken = 0;
            {
                std::unique_lock<std::mutex> lk(impl->queue_mu);
                impl->queue_cv.wait_for(lk, impl->opts.flush_interval, [&] {
                    return impl->stop.load(std::memory_order_acquire) ||
                           !impl->queue.empty();
                });
                if (impl->queue.empty() &&
                    impl->stop.load(std::memory_order_acquire)) {
                    return;
                }
                const std::size_t take = std::min(
                    impl->queue.size(), impl->opts.max_batch_size);
                batch.assign(
                    std::make_move_iterator(impl->queue.begin()),
                    std::make_move_iterator(impl->queue.begin() + take));
                impl->queue.erase(impl->queue.begin(),
                                    impl->queue.begin() + take);
                seq_taken = impl->enqueued_seq -
                             static_cast<uint64_t>(impl->queue.size());
            }
            if (!batch.empty()) {
                std::string e;
                (void)PostBatch(*impl, batch, &e);
                // Errors are intentionally swallowed — the exporter is a
                // best-effort path. A production deployment hooks its own
                // monitoring on the collector side; we don't want a wobbly
                // collector to cascade into the cache hot path.
                batch.clear();
            }
            {
                std::lock_guard<std::mutex> lk(impl->queue_mu);
                impl->flushed_seq = std::max(impl->flushed_seq, seq_taken);
                impl->flushed_cv.notify_all();
            }
        }
    });
    return self;
}

OtlpHttpExporter::~OtlpHttpExporter() {
    if (!impl_) return;
    impl_->stop.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(impl_->queue_mu);
        impl_->queue_cv.notify_all();
    }
    if (impl_->worker.joinable()) impl_->worker.join();
    if (impl_->curl) curl_easy_cleanup(impl_->curl);
}

void OtlpHttpExporter::Export(const SpanData& span) {
    std::lock_guard<std::mutex> lk(impl_->queue_mu);
    impl_->queue.push_back(span);
    ++impl_->enqueued_seq;
    if (impl_->queue.size() >= impl_->opts.max_batch_size) {
        impl_->queue_cv.notify_all();
    }
}

void OtlpHttpExporter::Flush() {
    uint64_t target;
    {
        std::lock_guard<std::mutex> lk(impl_->queue_mu);
        target = impl_->enqueued_seq;
        impl_->queue_cv.notify_all();
    }
    std::unique_lock<std::mutex> lk(impl_->queue_mu);
    impl_->flushed_cv.wait(lk, [&] {
        return impl_->flushed_seq >= target ||
               impl_->stop.load(std::memory_order_acquire);
    });
}

}  // namespace kvcache::trace
