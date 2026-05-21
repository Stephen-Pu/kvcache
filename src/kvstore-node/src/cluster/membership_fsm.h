// LLD §4.1 — Node membership FSM (5 states).
//
//   JOINING       — pod started, etcd-registered, warm-up window (0.1× weight)
//   ACTIVE        — full weight, accepting all traffic
//   DRAINING      — preStop hook fired, finishing in-flight (no new reserves)
//   UNREACHABLE   — Etcd lease expired; peers route around
//   TERMINATED    — terminal; pod gone
//
// Allowed transitions (LLD §6.3):
//
//     JOINING ──► ACTIVE ──► DRAINING ──► TERMINATED
//        │           │
//        ▼           ▼
//   UNREACHABLE ◄────┘
//        │
//        └──► JOINING (re-join after partition heal)
//
// The FSM is driven by two signal sources:
//   1. Local controller (start / drain / terminate)
//   2. Etcd lease status (loss of lease → UNREACHABLE)
//
// The class itself is thread-safe but stays out of etcd I/O — it is fed by
// callers that hold an IEtcdClient and observe lease state.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

namespace kvcache::node::cluster {

enum class NodeState : uint8_t {
    Joining     = 1,
    Active      = 2,
    Draining    = 3,
    Unreachable = 4,
    Terminated  = 5,
};

const char* NodeStateName(NodeState s);

enum class TransitionResult {
    kOk,
    kIllegalTransition,
    kAlreadyInState,
};

class MembershipFsm {
   public:
    struct Options {
        std::chrono::seconds warmup{300};  // 5 min, LLD §6.3
    };

    using StateChangeCallback = std::function<void(NodeState old_s, NodeState new_s)>;

    MembershipFsm();
    explicit MembershipFsm(const Options& opts);

    NodeState State() const noexcept {
        return state_.load(std::memory_order_acquire);
    }

    // Whether the node is in the warm-up window. ACTIVE state always returns
    // false; JOINING returns true until the deadline; other states false.
    bool InWarmup() const;

    // Traffic weight for the routing layer (LLD §6.3):
    //   JOINING during warmup : 0.1
    //   JOINING after warmup  : 0.1 (controller should transition to ACTIVE)
    //   ACTIVE                : 1.0
    //   DRAINING              : 0.0
    //   UNREACHABLE           : 0.0
    //   TERMINATED            : 0.0
    double TrafficWeight() const;

    // ---- transition triggers ----
    TransitionResult Start();             // (entry) → JOINING
    TransitionResult Activate();          // JOINING → ACTIVE
    TransitionResult Drain();             // ACTIVE → DRAINING
    TransitionResult LeaseLost();         // any → UNREACHABLE
    TransitionResult LeaseRecovered();    // UNREACHABLE → JOINING
    TransitionResult Terminate();         // DRAINING|UNREACHABLE → TERMINATED

    void SetStateChangeCallback(StateChangeCallback cb);

   private:
    bool                   IsLegalLocked(NodeState from, NodeState to) const;
    TransitionResult       TransitionTo(NodeState target);

    mutable std::mutex     mu_;
    std::atomic<NodeState> state_{NodeState::Joining};
    std::chrono::steady_clock::time_point joined_at_;
    std::chrono::seconds   warmup_;
    StateChangeCallback    cb_;
};

}  // namespace kvcache::node::cluster
