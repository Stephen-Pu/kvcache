// Structured logging facade.
//
// Backed by spdlog at link time. This header is the only thing the rest of the
// codebase #includes; spdlog stays out of public headers so we can swap it.
//
// Design:
//   - Per-subsystem named logger; default sink is stderr JSON.
//   - Audit log uses a separate stream (see security/audit.h) — DO NOT call
//     these macros for audit events.
//   - Levels: trace < debug < info < warn < error < critical.
//   - On the hot path (lookup / fetch / publish), prefer ring-buffer events
//     (LLD §D-PERF-2) over Log() calls; sync logging can blow the 1µs budget.
#pragma once

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
    // pImpl slot for the eventual spdlog::logger*. Marked maybe_unused so the
    // placeholder build doesn't warn before the spdlog dep is vendored in.
    [[maybe_unused]] void* impl_ = nullptr;
};

}  // namespace kvcache::log
