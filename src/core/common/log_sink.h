// LLD §6.2 — Structured JSON logging facade.
//
// Process-wide pluggable logger with a JSON-line default impl. Callers
// use the ``KV_LOG_*`` macros (header-only sugar over
// ``Default().Log(level, __FILE__, __LINE__, msg)``); tests swap in a
// ``NullLogger`` via ``SetDefault()`` to silence output.
//
// What this facade is FOR:
//   * Replace scattered ``std::cerr <<`` / ``fprintf(stderr, ...)``
//     calls with a single sink so operators can route logs through
//     their preferred aggregator (stdout JSON, syslog, OTLP, etc.).
//   * Give tests a way to assert "X logged a warning" or "this
//     subsystem stays silent under happy path" without grepping
//     captured stderr.
//   * Let production deployments raise the level (``kWarn`` and
//     above) so the hot path's debug chatter doesn't blow up
//     log bills.
//
// What this facade is NOT (yet):
//   * Callsite migration. Existing ``fprintf(stderr, ...)`` sites
//     keep working; converting them is per-file follow-on (search
//     for ``TODO(stephen): route through the logging facade``).
//   * Structured-field logging. Today's `msg` is opaque; if we
//     need ``logger.Log("foo", "key1=v1", "key2=v2")`` that's
//     a v2 extension. Strings only for the MVP.
//   * High-performance ring buffer. The default writer goes
//     straight to stderr under a mutex; fine for ~100 msg/s, not
//     for hot-path tracing.
//
// LLD reference: §6.2.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace kvcache::log::sink {

enum class LogLevel : int {
    kTrace = 0,
    kDebug = 1,
    kInfo  = 2,
    kWarn  = 3,
    kError = 4,
};

// Human-readable name for a level — used in the JSON output's
// ``level`` field and in error messages.
constexpr const char* LogLevelName(LogLevel l) noexcept {
    switch (l) {
        case LogLevel::kTrace: return "trace";
        case LogLevel::kDebug: return "debug";
        case LogLevel::kInfo:  return "info";
        case LogLevel::kWarn:  return "warn";
        case LogLevel::kError: return "error";
    }
    return "unknown";
}

// Abstract sink. Concrete loggers implement ``Log`` and (optionally)
// override the level threshold.
class Logger {
   public:
    virtual ~Logger() = default;

    // Emit one log record. ``file`` and ``line`` are typically
    // ``__FILE__`` / ``__LINE__`` from the calling macro; ``msg`` is
    // the rendered string (the facade does no formatting on its
    // own). Implementations MUST be safe to call from multiple
    // threads concurrently — the default ``ConsoleLogger`` serialises
    // via an internal mutex.
    virtual void Log(LogLevel level,
                     const char* file,
                     int line,
                     const std::string& msg) = 0;

    // Cheap fast-path check the macros use to avoid building the
    // ``msg`` string when the level is below threshold. Default
    // accepts everything; concrete sinks override.
    virtual bool ShouldLog(LogLevel /*level*/) const noexcept {
        return true;
    }
};

// JSON-line console logger. One object per record, written to
// stderr, separated by newlines:
//
//   {"ts":"2026-05-29T22:14:08.123Z","level":"warn","file":"foo.cpp","line":42,"msg":"queue depth=3"}
//
// Output is serialised through ``mu_`` so concurrent ``Log`` calls
// produce one well-formed record each (no interleaved bytes). The
// timestamp is ISO-8601 millisecond precision.
class ConsoleLogger : public Logger {
   public:
    explicit ConsoleLogger(LogLevel min_level = LogLevel::kInfo)
        : min_level_(min_level) {}

    void Log(LogLevel level, const char* file, int line,
              const std::string& msg) override;

    bool ShouldLog(LogLevel level) const noexcept override {
        return static_cast<int>(level) >= static_cast<int>(min_level_);
    }

    // Run-time level adjustment (e.g. via a SIGUSR1 handler or
    // operator console).
    void SetMinLevel(LogLevel l) noexcept { min_level_ = l; }

   private:
    LogLevel              min_level_;
    mutable std::mutex    mu_;
};

// No-op logger for tests + benchmarks. Drops every record.
class NullLogger : public Logger {
   public:
    void Log(LogLevel, const char*, int, const std::string&) override {}
    bool ShouldLog(LogLevel) const noexcept override { return false; }
};

// Process-wide default. The first call constructs a
// ``ConsoleLogger`` at the ``kInfo`` threshold; tests override via
// ``SetDefault`` (typically with a ``NullLogger``) and restore.
// Returns a strong reference (``shared_ptr``) so the macro's
// caller pins the sink's lifetime for the duration of one Log
// call — without this, a concurrent ``SetDefault`` could free the
// underlying sink while we're mid-call. Cheap on the hot path:
// shared_ptr copy is one atomic increment + one atomic decrement
// per log call; ~10ns.
std::shared_ptr<Logger> Default();
void                    SetDefault(std::shared_ptr<Logger> logger);

}  // namespace kvcache::log::sink

// Macro sugar — the standard call shape. ``msg`` can be any
// expression convertible to ``std::string`` (e.g. a literal, a
// ``std::string``, the result of ``fmt::format``). The level check
// runs before the string is built, so on a no-op logger the
// argument expression isn't evaluated either.
#define KV_LOG(level, msg)                                                 \
    do {                                                                   \
        auto _kv_log = ::kvcache::log::sink::Default();                    \
        if (_kv_log->ShouldLog(level)) {                                   \
            _kv_log->Log((level), __FILE__, __LINE__, (msg));              \
        }                                                                  \
    } while (0)

#define KV_LOG_TRACE(msg) KV_LOG(::kvcache::log::sink::LogLevel::kTrace, (msg))
#define KV_LOG_DEBUG(msg) KV_LOG(::kvcache::log::sink::LogLevel::kDebug, (msg))
#define KV_LOG_INFO(msg)  KV_LOG(::kvcache::log::sink::LogLevel::kInfo,  (msg))
#define KV_LOG_WARN(msg)  KV_LOG(::kvcache::log::sink::LogLevel::kWarn,  (msg))
#define KV_LOG_ERROR(msg) KV_LOG(::kvcache::log::sink::LogLevel::kError, (msg))
