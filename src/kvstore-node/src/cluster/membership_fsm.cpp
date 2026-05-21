// LLD §4.1 — MembershipFsm.
#include "cluster/membership_fsm.h"

namespace kvcache::node::cluster {

const char* NodeStateName(NodeState s) {
    switch (s) {
        case NodeState::Joining:     return "joining";
        case NodeState::Active:      return "active";
        case NodeState::Draining:    return "draining";
        case NodeState::Unreachable: return "unreachable";
        case NodeState::Terminated:  return "terminated";
    }
    return "?";
}

MembershipFsm::MembershipFsm() : MembershipFsm(Options{}) {}

MembershipFsm::MembershipFsm(const Options& opts)
    : joined_at_(std::chrono::steady_clock::now()), warmup_(opts.warmup) {}

bool MembershipFsm::InWarmup() const {
    if (State() != NodeState::Joining) return false;
    return std::chrono::steady_clock::now() < joined_at_ + warmup_;
}

double MembershipFsm::TrafficWeight() const {
    switch (State()) {
        case NodeState::Joining:  return 0.1;
        case NodeState::Active:   return 1.0;
        case NodeState::Draining:
        case NodeState::Unreachable:
        case NodeState::Terminated: return 0.0;
    }
    return 0.0;
}

bool MembershipFsm::IsLegalLocked(NodeState from, NodeState to) const {
    using S = NodeState;
    if (from == to) return false;
    switch (from) {
        case S::Joining:
            return to == S::Active || to == S::Unreachable;
        case S::Active:
            return to == S::Draining || to == S::Unreachable;
        case S::Draining:
            return to == S::Unreachable || to == S::Terminated;
        case S::Unreachable:
            return to == S::Joining || to == S::Terminated;
        case S::Terminated:
            return false;  // terminal
    }
    return false;
}

TransitionResult MembershipFsm::TransitionTo(NodeState target) {
    NodeState prev;
    StateChangeCallback cb;
    {
        std::lock_guard lk(mu_);
        prev = state_.load(std::memory_order_acquire);
        if (prev == target) return TransitionResult::kAlreadyInState;
        if (!IsLegalLocked(prev, target)) return TransitionResult::kIllegalTransition;
        state_.store(target, std::memory_order_release);
        if (target == NodeState::Joining) joined_at_ = std::chrono::steady_clock::now();
        cb = cb_;
    }
    if (cb) cb(prev, target);
    return TransitionResult::kOk;
}

TransitionResult MembershipFsm::Start() {
    // Start() resets joined_at_ even if already in Joining (re-init case).
    std::lock_guard lk(mu_);
    if (state_.load(std::memory_order_acquire) != NodeState::Joining) {
        return TransitionResult::kIllegalTransition;
    }
    joined_at_ = std::chrono::steady_clock::now();
    return TransitionResult::kAlreadyInState;
}

TransitionResult MembershipFsm::Activate()       { return TransitionTo(NodeState::Active); }
TransitionResult MembershipFsm::Drain()          { return TransitionTo(NodeState::Draining); }
TransitionResult MembershipFsm::LeaseLost()      { return TransitionTo(NodeState::Unreachable); }
TransitionResult MembershipFsm::LeaseRecovered() { return TransitionTo(NodeState::Joining); }
TransitionResult MembershipFsm::Terminate()      { return TransitionTo(NodeState::Terminated); }

void MembershipFsm::SetStateChangeCallback(StateChangeCallback cb) {
    std::lock_guard lk(mu_);
    cb_ = std::move(cb);
}

}  // namespace kvcache::node::cluster
