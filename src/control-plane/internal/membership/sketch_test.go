// Phase K-7 — integration tests for SketchAggregator against a real
// embedded etcd.
package membership_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	myetcd "github.com/Stephen-Pu/kvcache/control-plane/internal/etcd"
	"github.com/Stephen-Pu/kvcache/control-plane/internal/membership"
)

// makeSketch builds a wire-format BloomPublisher snapshot:
//
//	[m_bits:u32 LE][k_hashes:u32 LE][bits...]
//
// with bits[i] = setBytes[i]. The C++ side produces identically-laid-
// out blobs; the aggregator MUST be byte-format compatible.
func makeSketch(mBits, kHashes uint32, setBytes []byte) []byte {
	expect := int((mBits + 7) / 8)
	out := make([]byte, 8+expect)
	binary.LittleEndian.PutUint32(out[0:4], mBits)
	binary.LittleEndian.PutUint32(out[4:8], kHashes)
	for i := 0; i < expect && i < len(setBytes); i++ {
		out[8+i] = setBytes[i]
	}
	return out
}

// readSketch pulls + decodes the aggregator's output.
func readSketch(t *testing.T, ctx context.Context, cli *myetcd.Client) ([]byte, bool) {
	t.Helper()
	body, err := cli.Get(ctx, membership.ClusterSketchKey)
	if err != nil {
		t.Fatalf("read cluster sketch: %v", err)
	}
	if len(body) == 0 {
		return nil, false
	}
	return body, true
}

func waitForSketch(t *testing.T, ctx context.Context, cli *myetcd.Client,
	pred func([]byte) bool, budget time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		body, ok := readSketch(t, ctx, cli)
		if ok && pred(body) {
			return body
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster sketch never satisfied predicate within %s", budget)
	return nil
}

func TestSketchAggregator_OrsPerNodeBits(t *testing.T) {
	ep, stop := startEmbeddedEtcd(t)
	defer stop()

	cli, err := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Seed two per-node sketches with disjoint bit patterns. The
	// aggregator should OR them.
	const m, k uint32 = 64, 3
	// node-a sets bits 0..3 of byte 0 (0x0F).
	if err := cli.Put(ctx, membership.SketchesPrefix+"node-a",
		string(makeSketch(m, k, []byte{0x0F, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00})), 0); err != nil {
		t.Fatalf("put a: %v", err)
	}
	// node-b sets bits 4..7 of byte 0 (0xF0) plus byte 7 (0xAA).
	if err := cli.Put(ctx, membership.SketchesPrefix+"node-b",
		string(makeSketch(m, k, []byte{0xF0, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0xAA})), 0); err != nil {
		t.Fatalf("put b: %v", err)
	}

	agg := &membership.SketchAggregator{
		Etcd:     cli,
		LeaderID: "test-leader",
		Debounce: 30 * time.Millisecond,
	}
	aggCtx, aggCancel := context.WithCancel(ctx)
	defer aggCancel()
	go func() { _ = agg.Run(aggCtx) }()

	body := waitForSketch(t, ctx, cli, func(b []byte) bool {
		// header (8) + 8 bytes of bit-array
		return len(b) == 8+8 && b[8] == 0xFF
	}, 3*time.Second)

	// Verify the m/k header round-tripped.
	gotM := binary.LittleEndian.Uint32(body[0:4])
	gotK := binary.LittleEndian.Uint32(body[4:8])
	if gotM != m || gotK != k {
		t.Errorf("header lost: m=%d k=%d (want %d/%d)", gotM, gotK, m, k)
	}
	if body[8] != 0xFF {
		t.Errorf("byte 0: got 0x%02x want 0xFF (OR of 0x0F | 0xF0)", body[8])
	}
	if body[8+7] != 0xAA {
		t.Errorf("byte 7: got 0x%02x want 0xAA", body[8+7])
	}
}

func TestSketchAggregator_DropsParamMismatch(t *testing.T) {
	ep, stop := startEmbeddedEtcd(t)
	defer stop()

	cli, err := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First publisher pins params at m=64,k=3 with byte 0 = 0x01.
	if err := cli.Put(ctx, membership.SketchesPrefix+"node-a",
		string(makeSketch(64, 3, []byte{0x01, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00})), 0); err != nil {
		t.Fatalf("put a: %v", err)
	}
	// Second publisher comes in with DIFFERENT params (m=128). Must
	// be dropped — its bits should not flow into the aggregate.
	if err := cli.Put(ctx, membership.SketchesPrefix+"node-b",
		string(makeSketch(128, 3, []byte{0xFF, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00})), 0); err != nil {
		t.Fatalf("put b: %v", err)
	}

	agg := &membership.SketchAggregator{
		Etcd:     cli,
		LeaderID: "test-leader",
		Debounce: 30 * time.Millisecond,
	}
	aggCtx, aggCancel := context.WithCancel(ctx)
	defer aggCancel()
	go func() { _ = agg.Run(aggCtx) }()

	body := waitForSketch(t, ctx, cli, func(b []byte) bool {
		return len(b) == 8+8 && b[8] == 0x01
	}, 3*time.Second)

	// Header MUST match the first publisher's pinned params.
	if m := binary.LittleEndian.Uint32(body[0:4]); m != 64 {
		t.Errorf("agg params drifted: m=%d (want 64)", m)
	}
	// node-b's 0xFF must NOT have leaked in.
	if body[8] != 0x01 {
		t.Errorf("mismatched-params node-b leaked into aggregate (byte 0 = 0x%02x)", body[8])
	}
}

func TestSketchAggregator_DropsOnDelete(t *testing.T) {
	ep, stop := startEmbeddedEtcd(t)
	defer stop()

	cli, err := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const m, k uint32 = 64, 3
	if err := cli.Put(ctx, membership.SketchesPrefix+"node-a",
		string(makeSketch(m, k, []byte{0xAB, 0, 0, 0, 0, 0, 0, 0})), 0); err != nil {
		t.Fatalf("put: %v", err)
	}

	agg := &membership.SketchAggregator{
		Etcd:     cli,
		LeaderID: "test-leader",
		Debounce: 30 * time.Millisecond,
	}
	aggCtx, aggCancel := context.WithCancel(ctx)
	defer aggCancel()
	go func() { _ = agg.Run(aggCtx) }()

	// First publish: byte 0 = 0xAB.
	waitForSketch(t, ctx, cli, func(b []byte) bool {
		return len(b) >= 9 && b[8] == 0xAB
	}, 2*time.Second)

	// Delete the node's sketch — the aggregate must zero out byte 0.
	if _, err := cli.Delete(ctx, membership.SketchesPrefix+"node-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForSketch(t, ctx, cli, func(b []byte) bool {
		return len(b) >= 9 && b[8] == 0x00
	}, 2*time.Second)
}
