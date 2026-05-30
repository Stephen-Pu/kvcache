// Logging facade implementation.
//
// Phase O-2: routes the actual emit through ``kvcache::log::sink``
// (the JSON-line ConsoleLogger from Phase O-1). This file is the
// bridge between the historical public API (per-subsystem named
// loggers; documented design intent for a future spdlog backend)
// and the concrete sink. Spdlog wiring can replace the obs sink
// later via ``kvcache::log::sink::SetDefault(...)``; the public ``kvcache::log``
// surface here stays unchanged.
#include "logging.h"

#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "log_sink.h"

namespace kvcache::log {

namespace {

Level g_level = Level::Info;
std::mutex g_mu;
std::unordered_map<std::string, Logger> g_loggers;

const char* LevelName(Level l) {
    switch (l) {
        case Level::Trace:    return "trace";
        case Level::Debug:    return "debug";
        case Level::Info:     return "info";
        case Level::Warn:     return "warn";
        case Level::Error:    return "error";
        case Level::Critical: return "critical";
        case Level::Off:      return "off";
    }
    return "?";
}

// Map the historical ``kvcache::log::Level`` to the obs facade's
// ``LogLevel``. The two scales are nearly identical except
// kvcache::log has a ``Critical`` bucket that obs collapses into
// ``Error`` (obs aims for a smaller, more standard 5-level scale).
// ``Off`` is never reached via the sink — ShouldLog() filters it
// at the kvcache::log layer.
::kvcache::log::sink::LogLevel ToObsLevel(Level l) noexcept {
    using O = ::kvcache::log::sink::LogLevel;
    switch (l) {
        case Level::Trace:    return O::kTrace;
        case Level::Debug:    return O::kDebug;
        case Level::Info:     return O::kInfo;
        case Level::Warn:     return O::kWarn;
        case Level::Error:    return O::kError;
        case Level::Critical: return O::kError;  // collapsed
        case Level::Off:      return O::kError;  // unreachable via ShouldLog
    }
    return O::kError;
}

}  // namespace

void Init(const InitOptions& opts) {
    std::lock_guard lk(g_mu);
    g_level = opts.level;
    // Phase O-2 — install the obs ConsoleLogger at the matching
    // threshold so subsequent Log() calls actually filter at that
    // level. Operators get JSON-line output from this moment on.
    // The ``json`` / ``async`` / ``file_path`` fields in
    // InitOptions are honored when the spdlog backend lands; today's
    // obs::ConsoleLogger is sync-JSON-to-stderr only.
    ::kvcache::log::sink::SetDefault(
        std::make_shared<::kvcache::log::sink::ConsoleLogger>(
            ToObsLevel(opts.level)));
}

void Shutdown() {
    std::lock_guard lk(g_mu);
    g_loggers.clear();
    // Don't touch kvcache::log::sink::Default — other (non-kvcache::log)
    // callers may still want it. SetDefault is idempotent if they re-init.
}

Logger& Get(std::string_view subsystem) {
    std::lock_guard lk(g_mu);
    auto [it, inserted] = g_loggers.try_emplace(std::string{subsystem});
    if (inserted) {
        it->second.subsystem_ = it->first;
    }
    return it->second;
}

bool Logger::ShouldLog(Level level) const noexcept {
    return static_cast<int>(level) >= static_cast<int>(g_level);
}

void Logger::Log(Level level, std::string_view msg) noexcept {
    if (!ShouldLog(level)) return;
    // Compose: prepend "[subsystem] " so the obs sink's flat ``msg``
    // field retains grep-by-subsystem ability. Once obs grows
    // structured field support we can drop the prefix.
    std::string composed;
    composed.reserve(subsystem_.size() + msg.size() + 4);
    if (!subsystem_.empty()) {
        composed.push_back('[');
        composed.append(subsystem_);
        composed.append("] ");
    }
    composed.append(msg);
    // Route through kvcache::log::sink::Default(). The shared_ptr copy pins the
    // sink's lifetime through this call even if a concurrent
    // obs::SetDefault swap fires.
    auto sink = ::kvcache::log::sink::Default();
    // file/line aren't carried by the historical kvcache::log API.
    // Pass "?"/0 — the obs sink writes them as such, which is
    // honest about where the log came from until a future phase
    // wires __FILE__/__LINE__ through this layer too.
    sink->Log(ToObsLevel(level), "?", 0, composed);
}

void Logger::Trace(std::string_view m) noexcept { Log(Level::Trace,    m); }
void Logger::Debug(std::string_view m) noexcept { Log(Level::Debug,    m); }
void Logger::Info (std::string_view m) noexcept { Log(Level::Info,     m); }
void Logger::Warn (std::string_view m) noexcept { Log(Level::Warn,     m); }
void Logger::Error(std::string_view m) noexcept { Log(Level::Error,    m); }
void Logger::Crit (std::string_view m) noexcept { Log(Level::Critical, m); }

}  // namespace kvcache::log
