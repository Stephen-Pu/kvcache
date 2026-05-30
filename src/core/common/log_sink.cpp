// LLD §6.2 — Structured JSON logging facade.
#include "log_sink.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>

namespace kvcache::log::sink {

namespace {

// Process-wide default, held behind a shared_ptr so SetDefault can
// atomically swap the sink without leaving dangling references in
// other threads' Log calls (each Default() returns the current
// strong reference). A `std::atomic<std::shared_ptr<T>>` would be
// cleaner but isn't lock-free on every libstdc++ version; we
// serialise installs through a small mutex and load via a normal
// copy.
std::mutex                            g_default_mu;
std::shared_ptr<Logger>               g_default;

std::shared_ptr<Logger> EnsureDefault() {
    std::lock_guard lk(g_default_mu);
    if (!g_default) {
        g_default = std::make_shared<ConsoleLogger>(LogLevel::kInfo);
    }
    return g_default;
}

// Minimal JSON-string escaper for the `msg`, `file` fields. Handles
// the four characters that JSON forbids inside a string literal —
// quote, backslash, and the two ASCII control bytes most likely to
// appear in error messages (newline, tab). Everything else passes
// through. NOT a full RFC 7159 implementation; if a caller passes
// bytes outside ASCII the output may be invalid JSON. Acceptable
// for the MVP — the alternative is taking a full JSON dep just
// for log lines.
std::string EscapeJsonString(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\t': out += "\\t";  break;
            case '\r': out += "\\r";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x",
                                    static_cast<unsigned>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

// ISO-8601 millisecond UTC timestamp: "2026-05-29T22:14:08.123Z".
// Uses thread-local storage for the broken-down time so the gmtime
// call doesn't race other threads (gmtime is not thread-safe on
// every platform; gmtime_r is POSIX-only).
std::string Iso8601MillisUtc() {
    using namespace std::chrono;
    const auto now    = system_clock::now();
    const auto secs   = time_point_cast<seconds>(now);
    const auto millis = duration_cast<milliseconds>(now - secs).count();
    const std::time_t tt = system_clock::to_time_t(secs);
    std::tm tm_buf{};
#if defined(_WIN32)
    gmtime_s(&tm_buf, &tt);
#else
    gmtime_r(&tt, &tm_buf);
#endif
    char out[40];
    std::snprintf(out, sizeof(out),
                    "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ",
                    tm_buf.tm_year + 1900,
                    tm_buf.tm_mon  + 1,
                    tm_buf.tm_mday,
                    tm_buf.tm_hour,
                    tm_buf.tm_min,
                    tm_buf.tm_sec,
                    static_cast<long long>(millis));
    return std::string(out);
}

}  // namespace

void ConsoleLogger::Log(LogLevel level, const char* file, int line,
                         const std::string& msg) {
    if (!ShouldLog(level)) return;
    const std::string ts        = Iso8601MillisUtc();
    const std::string file_safe = EscapeJsonString(file ? file : "?");
    const std::string msg_safe  = EscapeJsonString(msg);
    // Pre-build the line so the stderr write is one syscall (mostly).
    char hdr[256];
    std::snprintf(hdr, sizeof(hdr),
                    R"({"ts":"%s","level":"%s","file":"%s","line":%d,"msg":")",
                    ts.c_str(), LogLevelName(level), file_safe.c_str(),
                    line);
    std::lock_guard lk(mu_);
    std::fputs(hdr, stderr);
    std::fputs(msg_safe.c_str(), stderr);
    std::fputs("\"}\n", stderr);
    std::fflush(stderr);
}

std::shared_ptr<Logger> Default() {
    return EnsureDefault();
}

void SetDefault(std::shared_ptr<Logger> logger) {
    std::lock_guard lk(g_default_mu);
    g_default = std::move(logger);
}

}  // namespace kvcache::log::sink
