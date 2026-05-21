#include "transport/priority_scheduler.h"

#include <gtest/gtest.h>

using kvcache::node::transport::Priority;
using kvcache::node::transport::PriorityScheduler;
using kvcache::node::transport::WorkItem;

TEST(PrioritySchedulerTest, PrioritiesAreServedInOrder) {
    PriorityScheduler::Options o;
    o.total_window_bytes = 1000;
    o.p0_pct = 20; o.p1_pct = 75; o.p2_pct = 5;
    PriorityScheduler s(o);
    s.Submit(Priority::P2, 1, nullptr);
    s.Submit(Priority::P1, 1, nullptr);
    s.Submit(Priority::P0, 1, nullptr);

    auto a = s.TryNext();
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->priority, Priority::P0);
    auto b = s.TryNext();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->priority, Priority::P1);
    auto c = s.TryNext();
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->priority, Priority::P2);
    EXPECT_FALSE(s.TryNext().has_value());
}

TEST(PrioritySchedulerTest, ReservationIsRespected) {
    PriorityScheduler::Options o;
    o.total_window_bytes = 1000;
    o.p0_pct = 20; o.p1_pct = 75; o.p2_pct = 5;
    PriorityScheduler s(o);
    // Reservations: P0=200, P1=750, P2=50.
    // Two 100-byte P0 items fit exactly into the P0 reservation.
    s.Submit(Priority::P0, 100, nullptr);
    s.Submit(Priority::P0, 100, nullptr);
    s.Submit(Priority::P0, 100, nullptr);  // would push P0 over

    auto a = s.TryNext();
    auto b = s.TryNext();
    ASSERT_TRUE(a && b);
    // The third item exceeds P0's 200B reservation and there's no idle credit
    // to loan (no other classes have data). The loan window is "upward only",
    // so we expect the third item to be deferred.
    auto c = s.TryNext();
    EXPECT_FALSE(c.has_value()) << "P0 should not borrow from below";
}

TEST(PrioritySchedulerTest, IdleCreditLoansDownward) {
    PriorityScheduler::Options o;
    o.total_window_bytes = 1000;
    o.p0_pct = 20; o.p1_pct = 75; o.p2_pct = 5;
    PriorityScheduler s(o);

    // P2 reservation is 50B; the work item is 600B. P0 + P1 are idle, so the
    // 50B reservation + 950B loan should cover it.
    s.Submit(Priority::P2, 600, nullptr);
    auto w = s.TryNext();
    ASSERT_TRUE(w.has_value());
    EXPECT_EQ(w->priority, Priority::P2);
}

TEST(PrioritySchedulerTest, OnCompleteFreesCredit) {
    PriorityScheduler::Options o;
    o.total_window_bytes = 200;
    o.p0_pct = 50; o.p1_pct = 40; o.p2_pct = 10;
    PriorityScheduler s(o);
    // P0 reservation = 100 bytes. Three 40-byte items: first two fit
    // (40+40=80 ≤ 100), the third would overflow (120 > 100).
    auto id1 = s.Submit(Priority::P0, 40, nullptr);
    s.Submit(Priority::P0, 40, nullptr);
    s.Submit(Priority::P0, 40, nullptr);

    auto a = s.TryNext();
    auto b = s.TryNext();
    ASSERT_TRUE(a && b);
    // 180/100 — third blocked (no idle credit since only P0 has work).
    EXPECT_FALSE(s.TryNext().has_value());

    EXPECT_TRUE(s.OnComplete(id1));
    auto c = s.TryNext();
    ASSERT_TRUE(c.has_value());
}

TEST(PrioritySchedulerTest, StarvationOverrideKicksIn) {
    PriorityScheduler::Options o;
    o.total_window_bytes      = 200;
    o.p0_pct = 100; o.p1_pct = 0; o.p2_pct = 0;
    o.max_starvation_skips    = 3;
    PriorityScheduler s(o);
    // P2 has 0B reservation and there are no higher classes with idle work to
    // loan from … but P0 is empty, so loan is the entire 200B. Force the
    // opposite scenario: queue a P0 item that consumes all P0 budget, then a
    // P2 item that's too big for its 0B reservation AND the loan is zero
    // because P0 has data in flight.
    s.Submit(Priority::P0, 200, nullptr);
    auto a = s.TryNext();
    ASSERT_TRUE(a);
    s.Submit(Priority::P2, 50, nullptr);
    // First three TryNext: P2 has nothing admissible (P0's 200B reservation
    // is fully in_flight, so no loan). Each call increments skip counter.
    EXPECT_FALSE(s.TryNext().has_value());
    EXPECT_FALSE(s.TryNext().has_value());
    EXPECT_FALSE(s.TryNext().has_value());
    // On the fourth attempt the starvation watchdog forces admission.
    auto forced = s.TryNext();
    ASSERT_TRUE(forced.has_value());
    EXPECT_EQ(forced->priority, Priority::P2);
}

TEST(PrioritySchedulerTest, OnCompleteUnknownIdReturnsFalse) {
    PriorityScheduler s({});
    EXPECT_FALSE(s.OnComplete(99999));
}
