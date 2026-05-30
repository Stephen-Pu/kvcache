// Structured logging facade.
//
// This is the public API the rest of the codebase #includes. Spdlog stays out
// of public headers so we can swap the backend. Phase O-2 routes the actual
// emit through ``kvcache::log::sink::Default()`` (see
// ``src/core/common/log_sink.h``) — that sink is the JSON-line
// ConsoleLogger today; spdlog can replace it later without touching this
// header or any callsite.
//
// Design:
//   - Per-subsystem named logger; default sink is stderr JSON.
//   - Subsystem name is prepended to every record's `msg` field so operators
//     can grep by subsystem (the obs sink doesn't yet carry structured
//     fields).
//   - Audit log uses a separate stream (see security/audit.h) — DO NOT call
//     these macros for audit events.
//   - Levels: trace < debug < info < warn < error < critical.
//   - On the hot path (lookup / fetch / publish), prefer ring-buffer events
//     (LLD §D-PERF-2) over Log() calls; sync logging can blow the 1µs budget.
#pragma once

#include <string>
#include <string_view>

namespace kvcache::log {

enum class Level { Trace = 0, Debug, Info, Warn, Error, Critical, Off };

struct InitOptions {
    Level       level         = Level::Info;
    bool        json          = true;     // newline-delimited JSON sink
    bool        async         = true;     // bounded async queue
    std::size_t async_queue   = 8192;
    std::string_view file_path = {};      // empty = stderr only
};

void Init(const InitOptions& opts);
void Shutdown();

// Get-or-create a named logger. Loggers are cheap and thread-safe.
class Logger;
Logger& Get(std::string_view subsystem);

class Logger {
   public:
    // Stamp-then-format pattern (avoids work if level is filtered out).
    void Log(Level level, std::string_view fmt) noexcept;

    // Convenience helpers (one-liners; for full formatting use spdlog directly
    // through the impl translation unit).
    void Trace(std::string_view msg) noexcept;
    void Debug(std::string_view msg) noexcept;
    void Info (std::string_view msg) noexcept;
    void Warn (std::string_view msg) noexcept;
    void Error(std::string_view msg) noexcept;
    void Crit (std::string_view msg) noexcept;

    bool ShouldLog(Level level) const noexcept;

   private:
    friend Logger& Get(std::string_view);
    // Subsystem name (e.g. "art", "scheduler", "grpc"). Set by ``Get``
    // after try_emplace into the global map; prepended to every record's
    // msg so operators can `grep '\[scheduler\]'` even though the obs
    // sink doesn't yet carry structured fields (Phase O-2 limitation —
    // when O-3 adds structured field support we can drop the prefix).
    std::string subsystem_;
};

}  // namespace kvcache::log
