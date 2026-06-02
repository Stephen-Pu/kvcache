// Phase A2.1 — drain overlay unit tests.
package membership

import "testing"

func TestApplyDrainOverlay_MarksMatchingNodes(t *testing.T) {
	nodes := []NodeDescriptor{
		{NodeID: "node-a", State: "active"},
		{NodeID: "node-b", State: "active"},
		{NodeID: "node-c"},
	}
	drained := map[string]bool{"node-b": true}
	out := applyDrainOverlay(nodes, drained)

	if out[0].State != "active" {
		t.Errorf("node-a state = %q, want active (untouched)", out[0].State)
	}
	if out[1].State != StateDraining {
		t.Errorf("node-b state = %q, want %q", out[1].State, StateDraining)
	}
	if out[2].State != "" {
		t.Errorf("node-c state = %q, want empty (untouched)", out[2].State)
	}

	// Must not mutate the input slice.
	if nodes[1].State != "active" {
		t.Errorf("input slice mutated: node-b state = %q", nodes[1].State)
	}
}

func TestApplyDrainOverlay_EmptySetIsNoOp(t *testing.T) {
	nodes := []NodeDescriptor{{NodeID: "node-a", State: "active"}}
	out := applyDrainOverlay(nodes, nil)
	if len(out) != 1 || out[0].State != "active" {
		t.Errorf("empty drain set must be a no-op; got %+v", out)
	}
}

func TestDrainKey(t *testing.T) {
	if got := DrainKey("prod", "node-7"); got != "/kvcache/drain/prod/node-7" {
		t.Errorf("DrainKey = %q", got)
	}
}
