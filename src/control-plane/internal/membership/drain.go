// Phase A2.1 — operator-initiated node drain.
//
// A drain is a control-plane desired-state overlay, NOT a mutation of
// the node's own etcd entry: the node owns /kvcache/nodes/<id> (lease-
// bound, re-PUT on re-register), so writing DRAINING there would be
// clobbered on the node's next re-register. Instead `kvctl drain`
// writes a small marker under /kvcache/drain/<cluster>/<node_id>, and
// the leader's ViewPublisher overlays it onto the published
// ClusterView (State = StateDraining). Consumers (kvagent HRW
// resolver) read the view and stop routing NEW prefixes to a draining
// node; in-flight work finishes.
//
// The marker is intentionally lease-free (survives a CP leader
// election) and tiny — drains are rare, operator-driven events.
package membership

import (
	"context"
	"encoding/json"
	"strings"
)

const (
	// DrainPrefix is the etcd prefix kvctl writes drain markers under:
	//   /kvcache/drain/<cluster>/<node_id>  ->  DrainMarker JSON
	DrainPrefix = "/kvcache/drain/"

	// StateDraining is the NodeDescriptor.State value the overlay sets.
	// Consumers match case-insensitively on the substring "drain".
	StateDraining = "draining"
)

// DrainMarker is the JSON body kvctl writes. Fields are advisory — the
// overlay only needs the key's existence to mark a node draining — but
// reason/requested_at_ns give operators an audit trail.
type DrainMarker struct {
	NodeID        string `json:"node_id"`
	Reason        string `json:"reason,omitempty"`
	RequestedAtNs int64  `json:"requested_at_ns,omitempty"`
}

// DrainKey builds the marker key for (cluster, node_id).
func DrainKey(cluster, nodeID string) string {
	return DrainPrefix + cluster + "/" + nodeID
}

// applyDrainOverlay returns a copy of `nodes` with State set to
// StateDraining for every node whose id appears in `drainedIDs`.
// Pure function (no etcd) so it's unit-testable in isolation.
func applyDrainOverlay(nodes []NodeDescriptor, drainedIDs map[string]bool) []NodeDescriptor {
	if len(drainedIDs) == 0 {
		return nodes
	}
	out := make([]NodeDescriptor, len(nodes))
	copy(out, nodes)
	for i := range out {
		if drainedIDs[out[i].NodeID] {
			out[i].State = StateDraining
		}
	}
	return out
}

// loadDrainedIDs reads the drain-marker prefix and returns the set of
// node_ids currently marked for drain. A malformed marker is treated
// as "the key's last path segment is the node id" so a hand-written
// etcdctl marker still works.
func loadDrainedIDs(ctx context.Context, r *Registry) (map[string]bool, error) {
	kvs, err := r.etcd.GetPrefix(ctx, DrainPrefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(kvs))
	for _, kv := range kvs {
		var m DrainMarker
		if json.Unmarshal([]byte(kv.Value), &m) == nil && m.NodeID != "" {
			out[m.NodeID] = true
			continue
		}
		// Fallback: derive node id from the key's last segment.
		if idx := strings.LastIndex(kv.Key, "/"); idx >= 0 && idx+1 < len(kv.Key) {
			out[kv.Key[idx+1:]] = true
		}
	}
	return out, nil
}
