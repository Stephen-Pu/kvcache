// Phase A2 — kvctl smoke tests.
//
// These cover the pure-function helpers (no etcd required), the
// stub-command exit messaging, and the /metrics parser. They don't
// stand up an embedded etcd — the CP-side server tests already do
// that and the kvctl members command is the same Get() call shape.
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSummariseMetrics_BasicShape(t *testing.T) {
	lines := []string{
		"kv_pinned_used_bytes 1024",
		`kv_tier_hits_total{tier="dram"} 42`,
		"kv_invalid_line_no_value",          // skipped
		`kv_floaty 3.14`,
	}
	m := summariseMetrics(lines)
	if got := m["kv_pinned_used_bytes"]; got != 1024 {
		t.Errorf("kv_pinned_used_bytes = %g, want 1024", got)
	}
	if got := m[`kv_tier_hits_total{tier="dram"}`]; got != 42 {
		t.Errorf("kv_tier_hits_total = %g, want 42", got)
	}
	if got := m["kv_floaty"]; got != 3.14 {
		t.Errorf("kv_floaty = %g, want 3.14", got)
	}
	if _, ok := m["kv_invalid_line_no_value"]; ok {
		t.Error("malformed line should have been skipped")
	}
}

func TestFilterMetricsByPrefix(t *testing.T) {
	lines := []string{
		"kv_pinned_used_bytes 1",
		"kv_tier_x 2",
		"kv_dram_x 3",
		"kv_unrelated 4",
	}
	got := filterMetricsByPrefix(lines, "kv_tier_", "kv_dram_", "kv_pinned_")
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3: %v", len(got), got)
	}
	if _, ok := got["kv_unrelated"]; ok {
		t.Error("kv_unrelated should have been filtered out")
	}
}

func TestDrainStubExitsWithMessage(t *testing.T) {
	g := &globalFlags{}
	c := newDrainCmd(g)
	c.SetArgs([]string{"node-x"})
	var errOut, stdOut bytes.Buffer
	c.SetErr(&errOut)
	c.SetOut(&stdOut)
	err := c.Execute()
	if err == nil {
		t.Fatal("drain stub must return an error")
	}
	if !strings.Contains(errOut.String(), "not yet implemented") {
		t.Errorf("missing helpful stderr message; got: %q", errOut.String())
	}
	// The node id must appear in the workaround hint so users can
	// copy-paste it without retyping.
	if !strings.Contains(errOut.String(), "node-x") {
		t.Errorf("workaround hint should mention the node id; got: %q", errOut.String())
	}
}

func TestTraceStubExitsWithMessage(t *testing.T) {
	g := &globalFlags{}
	c := newTraceCmd(g)
	c.SetArgs([]string{"req-7"})
	var errOut bytes.Buffer
	c.SetErr(&errOut)
	err := c.Execute()
	if err == nil {
		t.Fatal("trace stub must return an error")
	}
	if !strings.Contains(errOut.String(), "not yet implemented") {
		t.Errorf("missing helpful stderr message; got: %q", errOut.String())
	}
}

func TestVersionCommand(t *testing.T) {
	c := newVersionCmd()
	var out bytes.Buffer
	c.SetOut(&out)
	if err := c.Execute(); err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.Contains(out.String(), "kvctl ") {
		t.Errorf("version output missing 'kvctl '; got: %q", out.String())
	}
}

func TestIndentJSON_PrettyPrintsAndDegradesGracefully(t *testing.T) {
	pretty := indentJSON([]byte(`{"a":1,"b":[2,3]}`))
	if !strings.Contains(pretty, "\"a\":") {
		t.Errorf("indentJSON didn't reformat: %s", pretty)
	}
	// Garbage in → garbage back out, no crash.
	got := indentJSON([]byte(`not json at all`))
	if got != "not json at all" {
		t.Errorf("indentJSON should pass garbage through verbatim; got %q", got)
	}
}

func TestDefaultStr(t *testing.T) {
	if defaultStr("", "fb") != "fb" {
		t.Error("empty -> fallback")
	}
	if defaultStr("v", "fb") != "v" {
		t.Error("non-empty -> value")
	}
}

func TestEnvOr_FallsBack(t *testing.T) {
	// Unlikely-to-exist env var.
	if got := envOr("KVCTL_UNLIKELY_VAR_xyz123", "fb"); got != "fb" {
		t.Errorf("envOr missing var should fall back; got %q", got)
	}
	t.Setenv("KVCTL_SET_VAR", "real")
	if got := envOr("KVCTL_SET_VAR", "fb"); got != "real" {
		t.Errorf("envOr set var; got %q want real", got)
	}
	// Empty-string env var should also fall back.
	t.Setenv("KVCTL_EMPTY_VAR", "")
	if got := envOr("KVCTL_EMPTY_VAR", "fb"); got != "fb" {
		t.Errorf("envOr empty var should fall back; got %q", got)
	}
}
