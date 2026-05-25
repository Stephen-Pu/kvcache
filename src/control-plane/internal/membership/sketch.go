// Phase K-7 — cluster-wide bloom-sketch aggregator.
//
// Each kvstore-node ships its local LocalBloom at /kvcache/sketches/<id>
// (Phase K-5). The CP leader watches that prefix, ORs all sketches into
// one aggregated bitmap, and PUTs the result at /kvcache/cluster/sketch
// bound to its election lease. Consumers (kvagent's pre-routing pass,
// kvctl, future routing-warmer code) read ONE aggregated key — "does
// the cluster as a whole have this prefix?" — without fanning out a
// watch over every node's sketch.
//
// The aggregator is intentionally simple: it assumes every publisher
// chose the same (m_bits, k_hashes) parameters. The first sketch the
// aggregator sees pins the params; sketches with mismatched params
// are dropped with a logged warning. (Production sizes them centrally
// via Options; mismatch usually means a misconfigured rolling deploy.)
//
// Wire format (matches BloomPublisher::EncodeSnapshot in C++):
//
//   [m_bits:u32 LE][k_hashes:u32 LE][bit_array...]
//
// Bit array length is ceil(m_bits / 8).
package membership

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	myetcd "github.com/Stephen-Pu/kvcache/control-plane/internal/etcd"
)

const (
	// Prefix kvstore-node writes its per-node sketches under.
	SketchesPrefix = "/kvcache/sketches/"

	// Where the leader publishes the aggregated cluster sketch.
	ClusterSketchKey = "/kvcache/cluster/sketch"
)

// SketchAggregator owns the watch + OR + publish pipeline. Construct
// one per leader-duties invocation; cancelling the context stops it.
type SketchAggregator struct {
	Etcd     *myetcd.Client
	Lease    clientv3.LeaseID  // election session lease; 0 = no lease
	LeaderID string

	// Debounce on inbound events: events arriving within this window
	// collapse into one publish, so a rolling restart that re-PUTs
	// every node's sketch in quick succession doesn't N×spam etcd.
	// Zero picks 500 ms.
	Debounce time.Duration

	// Internal: per-node raw bytes, keyed by node_id (the key tail).
	mu       sync.Mutex
	sketches map[string][]byte
	mBits    uint32
	kHashes  uint32
}

// Run watches the sketches prefix and re-publishes the OR-aggregated
// blob on every change (debounced). Returns on ctx cancel or fatal
// etcd error.
func (a *SketchAggregator) Run(ctx context.Context) error {
	if a.Etcd == nil {
		return errors.New("sketch: nil etcd client")
	}
	if a.Debounce <= 0 {
		a.Debounce = 500 * time.Millisecond
	}
	a.sketches = make(map[string][]byte)

	// Seed: read every sketch present at this revision so we don't
	// have to wait for the first PUT after leader takeover.
	kvs, err := a.Etcd.GetPrefix(ctx, SketchesPrefix)
	if err != nil {
		return fmt.Errorf("sketch: seed get: %w", err)
	}
	a.mu.Lock()
	for _, kv := range kvs {
		nid := strings.TrimPrefix(kv.Key, SketchesPrefix)
		if nid == "" {
			continue
		}
		a.ingestLocked(nid, []byte(kv.Value))
	}
	a.mu.Unlock()
	if err := a.publish(ctx); err != nil {
		// First publish failure is non-fatal; the timer will retry.
		log.Printf("sketch: initial publish: %v", err)
	}

	w := a.Etcd.Raw().Watch(ctx, SketchesPrefix,
		clientv3.WithPrefix(), clientv3.WithPrevKV())

	dirty := false
	timer := time.NewTimer(a.Debounce)
	timer.Stop()
	armTimer := func() {
		if !dirty {
			return
		}
		timer.Reset(a.Debounce)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case resp, ok := <-w:
			if !ok {
				return nil
			}
			a.mu.Lock()
			for _, ev := range resp.Events {
				nid := strings.TrimPrefix(string(ev.Kv.Key), SketchesPrefix)
				if nid == "" {
					continue
				}
				switch ev.Type {
				case clientv3.EventTypeDelete:
					delete(a.sketches, nid)
				case clientv3.EventTypePut:
					a.ingestLocked(nid, ev.Kv.Value)
				}
			}
			a.mu.Unlock()
			dirty = true
			armTimer()

		case <-timer.C:
			if !dirty {
				continue
			}
			if err := a.publish(ctx); err != nil {
				log.Printf("sketch: publish: %v", err)
				armTimer()  // retry next tick
				continue
			}
			dirty = false
		}
	}
}

// ingestLocked validates the wire-format header and parks the
// bit-array bytes in a.sketches[nid]. Caller holds a.mu.
func (a *SketchAggregator) ingestLocked(nid string, blob []byte) {
	if len(blob) < 8 {
		log.Printf("sketch: %s payload too short (%d bytes)", nid, len(blob))
		return
	}
	m := binary.LittleEndian.Uint32(blob[0:4])
	k := binary.LittleEndian.Uint32(blob[4:8])
	if m == 0 || k == 0 {
		log.Printf("sketch: %s invalid params m=%d k=%d", nid, m, k)
		return
	}
	expectBytes := int((m + 7) / 8)
	if len(blob) < 8+expectBytes {
		log.Printf("sketch: %s bit array truncated (%d < %d)",
			nid, len(blob)-8, expectBytes)
		return
	}
	if a.mBits == 0 {
		// First sketch pins the params.
		a.mBits = m
		a.kHashes = k
	} else if a.mBits != m || a.kHashes != k {
		log.Printf("sketch: %s param mismatch (have m=%d k=%d, got m=%d k=%d) — dropped",
			nid, a.mBits, a.kHashes, m, k)
		return
	}
	// Copy out — etcd's slice gets reused on the next event.
	bits := make([]byte, expectBytes)
	copy(bits, blob[8:8+expectBytes])
	a.sketches[nid] = bits
}

// publish ORs all per-node sketches into the cluster bitmap and
// writes it to ClusterSketchKey. A snapshot with no contributing
// sketches publishes an all-zero bit-array (the consumers can tell
// "no nodes have published yet" from the header alone).
func (a *SketchAggregator) publish(ctx context.Context) error {
	a.mu.Lock()
	var m, k uint32 = a.mBits, a.kHashes
	if m == 0 {
		// No sketches seen yet. Publish a header-only sentinel so
		// consumers can distinguish "leader is alive but cluster
		// is empty" from "no leader / key absent".
		m = 1
		k = 1
	}
	bits := make([]byte, (m+7)/8)
	for _, src := range a.sketches {
		// Length tolerance: ingestLocked rejected mismatched params
		// so any entry here has len(src) == len(bits).
		for i := range bits {
			bits[i] |= src[i]
		}
	}
	a.mu.Unlock()

	out := make([]byte, 8+len(bits))
	binary.LittleEndian.PutUint32(out[0:4], m)
	binary.LittleEndian.PutUint32(out[4:8], k)
	copy(out[8:], bits)
	return a.Etcd.Put(ctx, ClusterSketchKey, string(out), a.Lease)
}
