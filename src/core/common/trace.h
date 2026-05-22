// Distributed-tracing facade.
//
// Same design tenets as logging.h: this header is the only thing the rest
// of the codebase #includes for tracing; concrete exporters (in-memory
// for tests, JSON-stderr for dev, OTLP-HTTP for production) live behind
// the abstraction and can be swapped without recompiling call sites.
//
// LLD §6.2 calls out Prometheus metrics + OTel tracing as the two pillars
// of observability. The Prometheus side already lives in metrics.h; this
// header is the OTel side.
//
// Design:
//   - W3C-compatible identifiers (128-bit trace_id, 64-bit span_id).
//   - RAII spans: ``auto s = tracer.StartSpan("kv.lookup");`` — End() runs
//     in the destructor; nested spans automatically pick up the active
//     parent through a thread-local stack.
//   - Attributes: ``s.SetAttribute("tenant", "...")`` — string + numeric
//     types only, matching the OTel core spec's common-case subset.
//   - Status: ``s.SetError("msg")`` flips the span's terminal status to
//     Error and records a brief message. Otherwise spans default to OK.
//   - Exporters are pluggable: ``trace::SetExporter(std::make_shared<...>())``.
//     Default exporter is a no-op so any cold build path stays a single
//     atomic compare, never a syscall.
//
// The hot-path budget for ``StartSpan`` is < 200 ns on the no-op path
// (load three atomics, RAII bookkeeping). The JSON exporter adds a
// fprintf at End-time — fine for the control-plane and slow data-plane
// ops but explicitly NOT for the inner Lookup walk.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace kvcache::trace {

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

// W3C trace context identifiers. trace_id is 128 bits, span_id is 64 bits;
// both zero means "no context" (i.e. the root has no parent).
struct SpanContext {
    std::array<uint8_t, 16> trace_id{};
    std::array<uint8_t, 8>  span_id{};

    bool IsValid() const noexcept {
        for (auto b : trace_id) if (b) return true;
        for (auto b : span_id)  if (b) return true;
        return false;
    }
};

// Attribute values. Mirrors the OTel core-spec common case.
using AttributeValue = std::variant<std::string, int64_t, double, bool>;

struct Attribute {
    std::string    key;
    AttributeValue value;
};

enum class StatusCode : uint8_t { Unset = 0, Ok = 1, Error = 2 };

// Recorded span — what an exporter receives at End() time.
struct SpanData {
    SpanContext            context;
    SpanContext            parent;
    std::string            name;
    TimePoint              start;
    TimePoint              end;
    std::vector<Attribute> attributes;
    StatusCode             status        = StatusCode::Unset;
    std::string            status_message;
};

class Exporter {
   public:
    virtual ~Exporter() = default;
    virtual void Export(const SpanData& span) = 0;
};

// ---- Span (RAII) ----------------------------------------------------------

// A span is a movable-but-non-copyable handle. Destructor calls End() if
// the caller didn't already do so. Setting attributes / status after End()
// is a no-op.
class Span {
   public:
    Span() noexcept = default;
    Span(Span&&) noexcept;
    Span& operator=(Span&&) noexcept;
    Span(const Span&)            = delete;
    Span& operator=(const Span&) = delete;
    ~Span();

    void SetAttribute(std::string_view key, std::string_view value);
    void SetAttribute(std::string_view key, int64_t value);
    void SetAttribute(std::string_view key, double  value);
    void SetAttribute(std::string_view key, bool    value);
    void SetError(std::string_view message);
    void End() noexcept;

    bool        Active()  const noexcept { return data_ != nullptr; }
    SpanContext Context() const noexcept;

   private:
    friend class Tracer;
    struct Pending;
    std::unique_ptr<Pending> data_;
};

// ---- Tracer ---------------------------------------------------------------

// Per-process singleton. ``StartSpan`` pushes the new span onto a
// thread-local active stack; nested StartSpan calls discover the
// parent from there automatically.
class Tracer {
   public:
    static Tracer& Get();

    Span StartSpan(std::string_view name);

    // Exporter lifecycle. nullptr re-installs the no-op exporter.
    void SetExporter(std::shared_ptr<Exporter> exporter);
    std::shared_ptr<Exporter> exporter() const;

    // Active parent context as seen by the current thread, or zero-context
    // when no span is on the stack.
    static SpanContext ActiveContext();

   private:
    Tracer() = default;
    mutable std::mutex        mu_;
    std::shared_ptr<Exporter> exporter_;
};

// ---- Built-in exporters ---------------------------------------------------

// Stores every Export() call in memory. Tests inspect ``.spans()``.
class InMemoryExporter final : public Exporter {
   public:
    void Export(const SpanData& span) override;

    std::vector<SpanData> spans() const;
    std::size_t Size() const;
    void Clear();

   private:
    mutable std::mutex    mu_;
    std::vector<SpanData> spans_;
};

// Writes one JSON line per span to stderr. Useful for dev runs; gate
// installation on the KVCACHE_TRACE_JSON env var (see InstallFromEnv).
class JsonStderrExporter final : public Exporter {
   public:
    void Export(const SpanData& span) override;
};

// Install JsonStderrExporter iff `KVCACHE_TRACE_JSON=1` (or `true`) is
// set in the process env at call time. No-op otherwise. Safe to call
// multiple times; only the first install takes effect.
void InstallFromEnv();

// ---------------------------------------------------------------------------
// OTLP/HTTP exporter (Phase J-2)
//
// Streams spans as OpenTelemetry's ExportTraceServiceRequest JSON to a
// configured ``/v1/traces`` endpoint — i.e. a real OTel collector
// (Tempo, Jaeger, Honeycomb refinery, alloy, ...). The exporter
// batches spans on a background worker thread so the calling thread
// only pays the cost of dropping a SpanData onto a queue.
//
// Lives in the sibling ``kvcache_otlp`` static library (not in
// ``kvcache_common``) so the libcurl + nlohmann/json link dependency
// stays opt-in: callers that don't need OTLP keep the lean build.
// ---------------------------------------------------------------------------

class OtlpHttpExporter final : public Exporter {
   public:
    struct Options {
        // OTel collector endpoint. Example: ``http://localhost:4318/v1/traces``.
        std::string endpoint;
        // Reported as ``service.name`` resource attribute. Default: "kvcache".
        std::string service_name = "kvcache";
        // Reported alongside service.name. Empty string disables.
        std::string service_version;
        // How long the worker waits between flushes when the queue is
        // non-empty but below ``max_batch_size``. Smaller = lower
        // latency; larger = fewer HTTP roundtrips.
        std::chrono::milliseconds flush_interval{500};
        // Hard cap on a single HTTP POST. The worker drains up to this
        // many spans per flush and POSTs them as one request.
        std::size_t max_batch_size = 512;
        // libcurl connect + total timeout.
        std::chrono::milliseconds dial_timeout{5000};
        // Optional TLS material (libcurl). Empty = insecure / system CA.
        std::string ca_pem_path;
        std::string client_cert_pem_path;
        std::string client_key_pem_path;
    };

    static std::unique_ptr<OtlpHttpExporter> Create(const Options& opts,
                                                     std::string* err);
    ~OtlpHttpExporter() override;

    // Enqueues `span` for asynchronous export. Non-blocking; the
    // worker thread does the HTTP POST.
    void Export(const SpanData& span) override;

    // Block until the queue is empty (or until the worker has POSTed
    // every span queued before the call). Used by tests and by
    // graceful-shutdown paths.
    void Flush();

    // pImpl publicly exposed (forward-declared only) so the .cpp's
    // anonymous-namespace helpers can manipulate the impl directly
    // without each being befriended individually. Mirrors
    // HttpEtcdClient::Impl in cluster/etcd_client.h.
    struct Impl;

   private:
    OtlpHttpExporter() = default;
    std::unique_ptr<Impl> impl_;
};

// Pure encoder, public so tests can drive it without an HTTP round-trip.
// Produces the JSON body of an OpenTelemetry ExportTraceServiceRequest
// covering every span in ``spans`` under the given resource attributes.
std::string EncodeOtlpTracesJson(const std::vector<SpanData>& spans,
                                   const std::string& service_name,
                                   const std::string& service_version);

}  // namespace kvcache::trace
