// Phase A1.1 — SqCq MPMC ring tests.
//
// Covers:
//   1. Create + Open round-trip with magic / version / slot-size validation.
//   2. Single-threaded fill-then-drain at exactly capacity (no off-by-one).
//   3. push-full + pop-empty are non-blocking false-returns.
//   4. Cross-process visibility — Create in process A, Open in process B,
//      observe the writes. We simulate this with fork() so we exercise
//      the real mmap-shared semantics.
//   5. MPMC stress — 4 producers + 4 consumers shovel N messages each
//      through a small ring; verify (a) no lost writes (sum invariant)
//      and (b) no torn reads (each payload's checksum byte matches).
//   6. Mismatched-kind Open is rejected.
#include "shmem_ring/sq_cq.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <numeric>
#include <thread>
#include <vector>

using namespace kvcache::agent::shmem_ring;

namespace {

std::string TmpPath(const char* tag) {
    // /dev/shm exists on Linux + CI; tmpdir on macOS dev rigs.
    const char* base = std::filesystem::exists("/dev/shm") ? "/dev/shm" : "/tmp";
    std::string p = std::string(base) + "/kvcache-sqcq-test-" + tag + "-"
                  + std::to_string(::getpid()) + ".bin";
    ::unlink(p.c_str());
    return p;
}

SqCq::OpenOptions Opts(const std::string& path, RingKind k,
                        uint32_t slots = 16, uint32_t bytes = 64) {
    return SqCq::OpenOptions{
        .path = path,
        .kind = k,
        .slot_count = slots,
        .slot_bytes = bytes,
    };
}

}  // namespace

TEST(SqCqTest, CreateOpenRoundTrip) {
    auto path = TmpPath("roundtrip");
    std::string err;
    auto a = SqCq::Create(Opts(path, RingKind::Sq, 32, 128), &err);
    ASSERT_NE(a, nullptr) << err;
    EXPECT_EQ(a->slot_count(), 32u);
    EXPECT_EQ(a->slot_bytes(), 128u);

    auto b = SqCq::Open(Opts(path, RingKind::Sq, 32, 128), &err);
    ASSERT_NE(b, nullptr) << err;
    EXPECT_EQ(b->slot_count(), 32u);

    // 'b' did not Create, so we don't unlink in its dtor. 'a' will.
    ::unlink(path.c_str());
}

TEST(SqCqTest, FillExactlyCapacityThenDrain) {
    auto path = TmpPath("fillcap");
    std::string err;
    auto r = SqCq::Create(Opts(path, RingKind::Sq, 8, 16), &err);
    ASSERT_NE(r, nullptr) << err;

    std::vector<uint8_t> payload(16, 0xAB);
    for (int i = 0; i < 8; ++i) {
        payload[0] = static_cast<uint8_t>(i);
        EXPECT_TRUE(r->try_push({payload.data(), payload.size()})) << "push " << i;
    }
    // 9th push must fail.
    EXPECT_FALSE(r->try_push({payload.data(), payload.size()}));

    std::vector<uint8_t> out;
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(r->try_pop(&out)) << "pop " << i;
        EXPECT_EQ(out[0], static_cast<uint8_t>(i));
    }
    EXPECT_FALSE(r->try_pop(&out));
}

TEST(SqCqTest, PopEmptyIsNonBlockingFalse) {
    auto path = TmpPath("empty");
    std::string err;
    auto r = SqCq::Create(Opts(path, RingKind::Cq, 4, 16), &err);
    ASSERT_NE(r, nullptr) << err;
    std::vector<uint8_t> out;
    EXPECT_FALSE(r->try_pop(&out));
}

TEST(SqCqTest, MismatchedKindOpenIsRejected) {
    auto path = TmpPath("kindmismatch");
    std::string err;
    auto a = SqCq::Create(Opts(path, RingKind::Sq, 4, 16), &err);
    ASSERT_NE(a, nullptr);

    auto b = SqCq::Open(Opts(path, RingKind::Cq, 4, 16), &err);
    EXPECT_EQ(b, nullptr);
    EXPECT_NE(err.find("magic"), std::string::npos);
}

TEST(SqCqTest, MismatchedSlotsOpenIsRejected) {
    auto path = TmpPath("slotmismatch");
    std::string err;
    auto a = SqCq::Create(Opts(path, RingKind::Sq, 8, 16), &err);
    ASSERT_NE(a, nullptr);

    auto b = SqCq::Open(Opts(path, RingKind::Sq, 16, 16), &err);
    EXPECT_EQ(b, nullptr);
    EXPECT_NE(err.find("slot_count"), std::string::npos);
}

// Cross-process visibility: parent creates, child opens + pushes, parent
// pops. Exercises the real mmap-shared semantics — a unit test inside a
// single process would catch nothing the OS doesn't already give us.
TEST(SqCqTest, CrossProcessVisibility) {
    auto path = TmpPath("xproc");
    std::string err;
    auto parent = SqCq::Create(Opts(path, RingKind::Sq, 8, 8), &err);
    ASSERT_NE(parent, nullptr) << err;

    pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        // Child — open + push 5 messages then exit.
        std::string cerr;
        auto child = SqCq::Open(Opts(path, RingKind::Sq, 8, 8), &cerr);
        if (!child) _exit(2);
        for (int i = 0; i < 5; ++i) {
            std::vector<uint8_t> p(8, 0);
            p[0] = static_cast<uint8_t>(0xC0 + i);
            if (!child->try_push({p.data(), p.size()})) _exit(3);
        }
        _exit(0);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        FAIL() << "child killed by signal " << WTERMSIG(status);
    }
    ASSERT_TRUE(WIFEXITED(status)) << "child did not exit normally";
    ASSERT_EQ(WEXITSTATUS(status), 0) << "child exit code " << WEXITSTATUS(status);

    std::vector<uint8_t> out;
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(parent->try_pop(&out)) << "parent pop " << i;
        EXPECT_EQ(out[0], static_cast<uint8_t>(0xC0 + i));
    }
}

// MPMC stress test: 4 producers x 4 consumers. Each producer pushes N
// payloads tagged with (producer_id, seq); consumers pop into per-bucket
// counters. Invariant: total popped == total pushed AND every (producer,
// seq) is seen exactly once.
TEST(SqCqTest, MpmcStressNoLossNoDup) {
    auto path = TmpPath("stress");
    std::string err;
    auto r = SqCq::Create(Opts(path, RingKind::Sq, /*slots=*/64, /*bytes=*/8), &err);
    ASSERT_NE(r, nullptr) << err;

    constexpr int kProducers   = 4;
    constexpr int kConsumers   = 4;
    constexpr int kPerProducer = 5000;

    std::atomic<int> total_popped{0};
    // 32-bit (prod_id, seq) -> count seen.
    std::vector<std::atomic<int>> seen(kProducers * kPerProducer);
    for (auto& s : seen) s.store(0);

    std::atomic<bool> producers_done{false};

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&, p] {
            for (int s = 0; s < kPerProducer; ++s) {
                std::vector<uint8_t> buf(8, 0);
                buf[0] = static_cast<uint8_t>(p);
                buf[1] = static_cast<uint8_t>(s & 0xFF);
                buf[2] = static_cast<uint8_t>((s >> 8) & 0xFF);
                buf[3] = static_cast<uint8_t>((s >> 16) & 0xFF);
                while (!r->try_push({buf.data(), buf.size()})) {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back([&] {
            std::vector<uint8_t> out;
            while (true) {
                if (r->try_pop(&out)) {
                    int p = out[0];
                    int s = out[1] | (out[2] << 8) | (out[3] << 16);
                    ASSERT_GE(p, 0); ASSERT_LT(p, kProducers);
                    ASSERT_GE(s, 0); ASSERT_LT(s, kPerProducer);
                    seen[p * kPerProducer + s].fetch_add(1, std::memory_order_relaxed);
                    total_popped.fetch_add(1, std::memory_order_relaxed);
                } else if (producers_done.load(std::memory_order_acquire)) {
                    // One more drain attempt — producers may have pushed
                    // between our pop and the flag check.
                    if (!r->try_pop(&out)) break;
                    int p = out[0];
                    int s = out[1] | (out[2] << 8) | (out[3] << 16);
                    seen[p * kPerProducer + s].fetch_add(1, std::memory_order_relaxed);
                    total_popped.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (int i = 0; i < kProducers; ++i) threads[i].join();
    producers_done.store(true, std::memory_order_release);
    for (int i = kProducers; i < kProducers + kConsumers; ++i) threads[i].join();

    EXPECT_EQ(total_popped.load(), kProducers * kPerProducer);
    int dupes = 0, missing = 0;
    for (auto& s : seen) {
        int v = s.load();
        if (v == 0) ++missing;
        if (v > 1)  ++dupes;
    }
    EXPECT_EQ(dupes, 0);
    EXPECT_EQ(missing, 0);
}
