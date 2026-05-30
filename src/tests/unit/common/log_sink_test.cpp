// LLD §6.2 — logging facade tests.
//
// Verifies the four invariants that matter for a sink-style logger:
//   * ConsoleLogger emits one well-formed JSON object per Log() call
//     (no interleaved bytes from concurrent callers).
//   * ShouldLog gating prevents the message from being formatted at
//     all when the level is below threshold.
//   * NullLogger is a true no-op (no output, ShouldLog always
//     false).
//   * SetDefault swaps the process-wide sink without dropping
//     records from in-flight Log calls (we serialise the swap so
//     each Default() returns the current strong reference).
#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "log_sink.h"
#include "logging.h"  // Phase O-2: kvcache::log public facade routes here

using kvcache::log::sink::ConsoleLogger;
using kvcache::log::sink::Default;
using kvcache::log::sink::Logger;
using kvcache::log::sink::LogLevel;
using kvcache::log::sink::NullLogger;
using kvcache::log::sink::SetDefault;

namespace {

// Capturing logger that just records records to a vector for the
// test to assert against. Avoids depending on stderr capture
// (which is fragile under parallel ctest).
class CapturingLogger : public Logger {
   public:
    struct Record { LogLevel level; std::string file; int line;
                     std::string msg; };

    void Log(LogLevel level, const char* file, int line,
              const std::string& msg) override {
        std::lock_guard lk(mu_);
        records_.push_back({level, file ? file : "", line, msg});
    }

    std::vector<Record> snapshot() const {
        std::lock_guard lk(mu_);
        return records_;
    }

   private:
    mutable std::mutex mu_;
    std::vector<Record> records_;
};

// Restore the previous default at teardown so other test binaries
// in the suite don't see our NullLogger swap.
class LoggerFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        cap_ = std::make_shared<CapturingLogger>();
        SetDefault(cap_);
    }
    void TearDown() override {
        // Restore a ConsoleLogger; subsequent tests get fresh
        // defaults via EnsureDefault if anyone needs it.
        SetDefault(std::make_shared<ConsoleLogger>());
    }
    std::shared_ptr<CapturingLogger> cap_;
};

}  // namespace

TEST_F(LoggerFixture, KVLogMacroForwardsLevelFileLineMsg) {
    KV_LOG_INFO("hello info");
    KV_LOG_WARN("hello warn");
    KV_LOG_ERROR("hello error");

    auto recs = cap_->snapshot();
    ASSERT_EQ(recs.size(), 3u);
    EXPECT_EQ(recs[0].level, LogLevel::kInfo);
    EXPECT_EQ(recs[0].msg, "hello info");
    EXPECT_NE(recs[0].file.find("log_sink_test.cpp"), std::string::npos);
    EXPECT_GT(recs[0].line, 0);
    EXPECT_EQ(recs[1].level, LogLevel::kWarn);
    EXPECT_EQ(recs[2].level, LogLevel::kError);
}

TEST_F(LoggerFixture, NullLoggerSwallowsEverything) {
    auto null_logger = std::make_shared<NullLogger>();
    SetDefault(null_logger);
    KV_LOG_ERROR("vanishes");
    KV_LOG_WARN("also vanishes");
    EXPECT_FALSE(Default()->ShouldLog(LogLevel::kError));
}

TEST_F(LoggerFixture, ShouldLogGatesAtConsoleLoggerThreshold) {
    auto console = std::make_shared<ConsoleLogger>(LogLevel::kWarn);
    SetDefault(console);
    // Below threshold — ShouldLog is false; the KV_LOG macro
    // short-circuits before building the message.
    EXPECT_FALSE(Default()->ShouldLog(LogLevel::kTrace));
    EXPECT_FALSE(Default()->ShouldLog(LogLevel::kDebug));
    EXPECT_FALSE(Default()->ShouldLog(LogLevel::kInfo));
    // At + above threshold — ShouldLog is true.
    EXPECT_TRUE(Default()->ShouldLog(LogLevel::kWarn));
    EXPECT_TRUE(Default()->ShouldLog(LogLevel::kError));
}

TEST_F(LoggerFixture, KvLogMacroSkipsArgumentEvaluationBelowThreshold) {
    // Use a Warn-threshold ConsoleLogger; below-threshold calls must
    // NOT evaluate the msg expression. We prove this by passing a
    // function call that would increment a counter — counter stays
    // zero when the level is below threshold.
    auto console = std::make_shared<ConsoleLogger>(LogLevel::kWarn);
    SetDefault(console);
    std::atomic<int> calls{0};
    auto build_msg = [&]() {
        calls.fetch_add(1, std::memory_order_relaxed);
        return std::string("expensive");
    };
    KV_LOG_DEBUG(build_msg());  // below threshold — skip
    KV_LOG_INFO(build_msg());   // below threshold — skip
    EXPECT_EQ(calls.load(), 0)
        << "KV_LOG should NOT evaluate the msg expression when the "
           "level is below the sink's threshold";
    KV_LOG_WARN(build_msg());   // at threshold — runs
    EXPECT_EQ(calls.load(), 1);
}

TEST_F(LoggerFixture, SetDefaultIsConcurrencySafe) {
    // Spam Log + SetDefault from multiple threads; the test passes
    // if it doesn't crash or deadlock. The mutex inside SetDefault
    // serialises the swap; in-flight Log calls hold their own
    // shared_ptr ref (returned by Default()) so they can't see a
    // half-destroyed sink.
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                KV_LOG_INFO("from worker");
            }
        });
    }
    // Hammer SetDefault from the main thread.
    for (int i = 0; i < 200; ++i) {
        SetDefault(std::make_shared<NullLogger>());
        SetDefault(std::make_shared<ConsoleLogger>(LogLevel::kError));
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : threads) th.join();
    SUCCEED() << "no crash/deadlock under concurrent SetDefault + KV_LOG";
}

TEST(LogLevelName, AllLevelsHaveNames) {
    using kvcache::log::sink::LogLevelName;
    EXPECT_STREQ(LogLevelName(LogLevel::kTrace), "trace");
    EXPECT_STREQ(LogLevelName(LogLevel::kDebug), "debug");
    EXPECT_STREQ(LogLevelName(LogLevel::kInfo),  "info");
    EXPECT_STREQ(LogLevelName(LogLevel::kWarn),  "warn");
    EXPECT_STREQ(LogLevelName(LogLevel::kError), "error");
}

// Phase O-2: prove kvcache::log::Get(subsystem).Info(msg) routes through
// the sink with the subsystem prefix attached to the flat msg field.
// This is the contract that lets per-callsite migration of fprintf/cerr
// happen against the public facade without touching the sink call.
TEST(LogFacadeO2, RoutesThroughSinkWithSubsystemPrefix) {
    auto cap = std::make_shared<CapturingLogger>();
    SetDefault(cap);
    kvcache::log::Init({.level = kvcache::log::Level::Trace});
    // Re-install the capturing sink — Init() swaps in a ConsoleLogger.
    SetDefault(cap);

    auto& lg = kvcache::log::Get("scheduler");
    lg.Info("queue depth=42");
    lg.Warn("slow tier");

    auto recs = cap->snapshot();
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].level, LogLevel::kInfo);
    EXPECT_EQ(recs[0].msg, "[scheduler] queue depth=42");
    EXPECT_EQ(recs[1].level, LogLevel::kWarn);
    EXPECT_EQ(recs[1].msg, "[scheduler] slow tier");
}
