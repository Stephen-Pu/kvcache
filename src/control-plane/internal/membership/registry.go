// Package membership manages the cluster member registry, backed by etcd.
//
// Keyspace (LLD §4.1):
//
//   /nodes/<node_id>           — JSON-encoded node descriptor, lease-bound.
//                                 Lease loss == node became unreachable.
//   /cp/leader                  — leader-election sentinel; lease-bound.
//
// The registry exposes:
//   * RegisterNode  — node calls this to publish itself with a fresh lease.
//   * KeepAlive     — refresh the lease (one call per heartbeat).
//   * ListNodes     — current snapshot of all live nodes.
//   * Watch         — stream of add / update / delete events.
//
// All operations are idempotent on retry.
package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	myetcd "github.com/Stephen-Pu/kvcache/control-plane/internal/etcd"
)

const (
	// Phase Q-1 / K-2 aligned — kvstore-node's NodeRegistrar writes its
	// identity here, and the CP leader watches the same prefix to
	// build cluster snapshots. Two different prefixes would mean the
	// CP never sees real nodes (the bug K-2 fixes).
	NodesPrefix    = "/kvcache/nodes/"
	DefaultLeaseTTL = 10
)

// NodeDescriptor is what the registry stores per node.
//
// Schema is forward-compatible with kvstore-node's NodeRegistrar JSON
// (Phase Q-1: {"node_id", "host", "grpc_port"}). Capacity / topology
// fields are populated by future agents that know their hardware; the
// MVP node binary leaves them at zero so the JSON parser is permissive.
type NodeDescriptor struct {
	NodeID              string   `json:"node_id"`
	Host                string   `json:"host"`
	// Phase K-2 — the gRPC port kvstore-node's NodeData service listens
	// on. Consumers (router, kvagent) read this to dial the node.
	GrpcPort            uint16   `json:"grpc_port,omitempty"`
	Version             string   `json:"version,omitempty"`
	// Phase A2.1 — node lifecycle state. The node leaves this empty
	// ("active"); the ViewPublisher's drain overlay sets it to
	// "draining" in the published ClusterView so consumers (kvagent
	// HRW resolver) can exclude it from routing.
	State               string   `json:"state,omitempty"`
	PinnedBytes         uint64   `json:"pinned_bytes,omitempty"`
	DramBytes           uint64   `json:"dram_bytes,omitempty"`
	NvmeBytes           uint64   `json:"nvme_bytes,omitempty"`
	NumaNodes           uint32   `json:"numa_nodes,omitempty"`
	Vcpus               uint32   `json:"vcpus,omitempty"`
	SupportedTransports []string `json:"supported_transports,omitempty"`
}

// EventType mirrors the event the watcher emits.
type EventType int

const (
	EventAdd    EventType = 1
	EventUpdate EventType = 2
	EventDelete EventType = 3
)

type Event struct {
	Type EventType
	Node NodeDescriptor
}

// Registry is the membership façade.
type Registry struct {
	etcd *myetcd.Client
}

func NewRegistry(c *myetcd.Client) *Registry {
	return &Registry{etcd: c}
}

// EtcdClient exposes the underlying client for collaborators that
// need their own Put/Watch path (e.g. the K-7 SketchAggregator).
// Returns nil for an uninitialised Registry — callers should
// nil-check before use.
func (r *Registry) EtcdClient() *myetcd.Client { return r.etcd }

// RegisterNode publishes the node under /nodes/<node_id> bound to a fresh lease
// and returns the lease id. The caller must call KeepAlive on that lease for
// the duration of the node's lifetime.
func (r *Registry) RegisterNode(ctx context.Context, n NodeDescriptor, ttlSec int64) (clientv3.LeaseID, error) {
	if ttlSec <= 0 {
		ttlSec = DefaultLeaseTTL
	}
	lease, err := r.etcd.LeaseGrant(ctx, ttlSec)
	if err != nil {
		return 0, err
	}
	body, err := json.Marshal(n)
	if err != nil {
		return 0, fmt.Errorf("registry: marshal: %w", err)
	}
	key := NodesPrefix + n.NodeID
	if err := r.etcd.Put(ctx, key, string(body), lease); err != nil {
		return 0, err
	}
	return lease, nil
}

// KeepAlive starts an etcd KeepAlive stream for the given lease. The caller
// supplies a context; cancelling it stops the keepalive and the etcd lease
// will expire ~ttl seconds later (causing the registry entry to vanish).
func (r *Registry) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return r.etcd.LeaseKeepAlive(ctx, id)
}

// ListNodes returns a snapshot of all live nodes.
func (r *Registry) ListNodes(ctx context.Context) ([]NodeDescriptor, error) {
	kvs, err := r.etcd.GetPrefix(ctx, NodesPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]NodeDescriptor, 0, len(kvs))
	for _, kv := range kvs {
		var n NodeDescriptor
		if err := json.Unmarshal([]byte(kv.Value), &n); err != nil {
			// Malformed entries shouldn't sink the whole list; emit a sentinel
			// with just the id so an operator can investigate via kvctl.
			n = NodeDescriptor{NodeID: strings.TrimPrefix(kv.Key, NodesPrefix)}
		}
		out = append(out, n)
	}
	return out, nil
}

// Watch streams membership changes over the returned channel until ctx is
// cancelled. Initial state is delivered as a burst of EventAdd events.
func (r *Registry) Watch(ctx context.Context) (<-chan Event, error) {
	ch := make(chan Event, 64)
	// Snapshot then watch — synchronous with respect to the returned channel.
	initial, err := r.ListNodes(ctx)
	if err != nil {
		close(ch)
		return nil, err
	}
	go func() {
		defer close(ch)
		for _, n := range initial {
			select {
			case <-ctx.Done():
				return
			case ch <- Event{Type: EventAdd, Node: n}:
			}
		}
		w := r.etcd.Raw().Watch(ctx, NodesPrefix,
			clientv3.WithPrefix(),
			clientv3.WithPrevKV())
		for resp := range w {
			for _, ev := range resp.Events {
				e, ok := decodeEvent(ev)
				if !ok {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case ch <- e:
				}
			}
		}
	}()
	return ch, nil
}

func decodeEvent(ev *clientv3.Event) (Event, bool) {
	var n NodeDescriptor
	switch {
	case ev.Type == clientv3.EventTypePut && ev.IsCreate():
		if err := json.Unmarshal(ev.Kv.Value, &n); err != nil {
			return Event{}, false
		}
		return Event{Type: EventAdd, Node: n}, true
	case ev.Type == clientv3.EventTypePut:
		if err := json.Unmarshal(ev.Kv.Value, &n); err != nil {
			return Event{}, false
		}
		return Event{Type: EventUpdate, Node: n}, true
	case ev.Type == clientv3.EventTypeDelete:
		// Use the prev_kv to recover the node id; if absent, fall back to
		// the key tail.
		if ev.PrevKv != nil {
			if err := json.Unmarshal(ev.PrevKv.Value, &n); err == nil {
				return Event{Type: EventDelete, Node: n}, true
			}
		}
		n.NodeID = strings.TrimPrefix(string(ev.Kv.Key), NodesPrefix)
		return Event{Type: EventDelete, Node: n}, true
	}
	return Event{}, false
}

// HeartbeatLoop is a convenience helper: registers + keep-alives in one call,
// running until ctx is cancelled. Logs are out of scope here — wire via the
// caller's logger.
func (r *Registry) HeartbeatLoop(ctx context.Context, n NodeDescriptor, ttlSec int64) error {
	lease, err := r.RegisterNode(ctx, n, ttlSec)
	if err != nil {
		return err
	}
	ka, err := r.KeepAlive(ctx, lease)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ka:
			if !ok {
				// Channel closed — usually means the lease was revoked or the
				// network went away. Sleep briefly and re-register.
				time.Sleep(500 * time.Millisecond)
				lease, err = r.RegisterNode(ctx, n, ttlSec)
				if err != nil {
					return err
				}
				ka, err = r.KeepAlive(ctx, lease)
				if err != nil {
					return err
				}
			}
		}
	}
}
