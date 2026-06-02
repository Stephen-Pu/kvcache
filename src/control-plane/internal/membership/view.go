// Phase K-2 — control-plane-published cluster view.
//
// The leader CP watches /kvcache/nodes/ and publishes a coherent
// snapshot of the live cluster at /kvcache/cluster/view every time
// membership changes. Consumers (kvstore-node's NodeDirectory, the
// future kvagent's router, kvctl) Watch ONE key instead of fanning
// out a watch over the whole /kvcache/nodes/ prefix.
//
// Two invariants the consumers rely on:
//
//   * `Epoch` is strictly monotonically increasing across publishes
//     by the SAME leader session. A re-election can reset it (each
//     new leader starts at 1), so consumers compare epochs only when
//     `LeaderID` matches.
//
//   * `Nodes` is the FULL membership the leader observed at publish
//     time — never a delta. The leader debounces rapid membership
//     events (default 100 ms) to avoid PUT-storms on etcd while
//     still surfacing convergence within a single human eye-blink.
//
// Storage is JSON-as-bytes under `/kvcache/cluster/view`, lease-bound
// to the leader's session — if the leader dies, the key expires and
// consumers see the absence as "no current view, fall back to direct
// prefix watch". On takeover the new leader rewrites the key under
// its own session.
package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// ViewKey is the etcd key the leader writes the snapshot to.
	ViewKey = "/kvcache/cluster/view"

	// DefaultDebounce coalesces a burst of membership events
	// (multiple node restarts, rolling deploy) into one publish.
	DefaultDebounce = 100 * time.Millisecond
)

// ClusterView is the JSON payload written to ViewKey.
type ClusterView struct {
	// Strictly-monotonic across publishes within a leader session.
	Epoch uint64 `json:"epoch"`
	// Leader identity (the `ident` string runElection passes to
	// concurrency.NewElection's Campaign). Stable across one
	// leader's term; rotates on re-election.
	LeaderID string `json:"leader_id"`
	// Unix nanoseconds when this view was produced.
	PublishedAtNs int64 `json:"published_at_ns"`
	// Membership at publish time, sorted by NodeID so byte-identical
	// publishes happen only when membership truly hasn't changed.
	Nodes []NodeDescriptor `json:"nodes"`
}

// ViewPublisher owns the watcher → debounce → PUT pipeline. Construct
// one per leader-duties invocation; cancel the context to stop.
type ViewPublisher struct {
	Registry *Registry
	// Etcd lease the publish key is bound to. Typically the leader
	// election session's lease so the key auto-expires on leader
	// loss. Pass 0 to write without a lease (key persists across
	// re-elections; only useful for tests).
	Lease clientv3.LeaseID
	// Self-identifier the leader publishes in `ClusterView.LeaderID`.
	LeaderID string
	// Debounce window. 0 picks DefaultDebounce.
	Debounce time.Duration

	// Internal: strictly-monotonic publish counter.
	epoch atomic.Uint64
}

// Run watches the membership prefix and publishes a fresh
// ClusterView snapshot on every change (debounced). Returns when ctx
// is cancelled or the underlying watch closes. The first publish
// fires synchronously off the initial ListNodes snapshot so the view
// is non-empty within milliseconds of leader takeover.
func (p *ViewPublisher) Run(ctx context.Context) error {
	if p.Debounce <= 0 {
		p.Debounce = DefaultDebounce
	}
	events, err := p.Registry.Watch(ctx)
	if err != nil {
		return fmt.Errorf("view: watch: %w", err)
	}

	// Track the most recent membership locally so the debounce timer
	// can publish a SNAPSHOT (not a diff) when it fires.
	var latest []NodeDescriptor
	// Bootstrap with the initial ListNodes so the first publish
	// reflects nodes that were present BEFORE the leader started.
	if snap, err := p.Registry.ListNodes(ctx); err == nil {
		latest = snap
	}
	dirty := true  // force first publish so consumers see the view
	timer := time.NewTimer(p.Debounce)
	timer.Stop()

	publish := func() error {
		sort.Slice(latest, func(i, j int) bool {
			return latest[i].NodeID < latest[j].NodeID
		})
		// Phase A2.1 — overlay operator drain markers onto the snapshot
		// so a drained node is published as DRAINING. Best-effort: a
		// drain-marker read error must not block the membership view, so
		// we skip the overlay on error (the next publish retries).
		nodes := append([]NodeDescriptor(nil), latest...)
		if drained, err := loadDrainedIDs(ctx, p.Registry); err == nil {
			nodes = applyDrainOverlay(nodes, drained)
		}
		view := ClusterView{
			Epoch:         p.epoch.Add(1),
			LeaderID:      p.LeaderID,
			PublishedAtNs: time.Now().UnixNano(),
			Nodes:         nodes,
		}
		body, err := json.Marshal(&view)
		if err != nil {
			return fmt.Errorf("view: marshal: %w", err)
		}
		// Empty lease is a no-op flag; pass actual lease through Put.
		if err := p.Registry.etcd.Put(ctx, ViewKey, string(body), p.Lease); err != nil {
			return fmt.Errorf("view: put: %w", err)
		}
		return nil
	}

	armTimer := func() {
		if !dirty {
			return
		}
		timer.Reset(p.Debounce)
	}
	armTimer()  // bootstrap publish

	for {
		select {
		case <-ctx.Done():
			return nil

		case ev, ok := <-events:
			if !ok {
				// Watch ended (registry's goroutine exited). Final
				// publish so consumers see the leader's last view.
				if dirty {
					_ = publish()
				}
				return nil
			}
			// Apply event to local snapshot. The registry already
			// delivers fully-decoded events; we just maintain set
			// semantics here.
			switch ev.Type {
			case EventAdd, EventUpdate:
				replaced := false
				for i := range latest {
					if latest[i].NodeID == ev.Node.NodeID {
						latest[i] = ev.Node
						replaced = true
						break
					}
				}
				if !replaced {
					latest = append(latest, ev.Node)
				}
			case EventDelete:
				for i := range latest {
					if latest[i].NodeID == ev.Node.NodeID {
						latest = append(latest[:i], latest[i+1:]...)
						break
					}
				}
			}
			dirty = true
			armTimer()

		case <-timer.C:
			if !dirty {
				continue
			}
			if err := publish(); err != nil {
				// Don't abort the publisher on a single PUT failure
				// — the next event will re-arm the timer. Log via
				// the caller's logger by returning would be too
				// loud; for now swallow + leave dirty=true so we
				// retry on the next tick.
				armTimer()
				continue
			}
			dirty = false
		}
	}
}

// ReadView is a one-shot helper consumers use to bootstrap before
// opening their own Watch on ViewKey. Returns (view, true) on hit;
// (zero, false) when the key is absent (no leader has published yet
// or the lease expired between elections).
func ReadView(ctx context.Context, etcd interface {
	Get(context.Context, string) ([]byte, error)
}) (ClusterView, bool, error) {
	body, err := etcd.Get(ctx, ViewKey)
	if err != nil {
		return ClusterView{}, false, err
	}
	if len(body) == 0 {
		return ClusterView{}, false, nil
	}
	var v ClusterView
	if err := json.Unmarshal(body, &v); err != nil {
		return ClusterView{}, false, fmt.Errorf("view: unmarshal: %w", err)
	}
	return v, true, nil
}
