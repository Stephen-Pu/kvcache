// kvctl — operator CLI for the KV Cache cluster. LLD §8.3.
//
// Phase A2 — six subcommands wired:
//
//   members      — list cluster nodes (etcd /kvcache/nodes/)
//   inspect      — pull /metrics from a node and summarise key gauges
//   tier-stats   — pull /metrics and filter to tier_* keys
//   quota        — read tenant quota from etcd (set is A2.1)
//   drain        — mark a node DRAINING (stub today — needs CP-side
//                  write API; see RFC at the bottom of this file)
//   trace        — tail request-level trace events (stub — needs
//                  the kvstore-node Subscribe gRPC stream wired in)
//   version      — print build version
//
// All commands work directly against etcd + the node's HTTP probe
// surface (Phase L-1 wired /healthz + /metrics on the same port the
// gRPC listener uses + 1 by convention; default --metrics-port=9090
// is the operator's expected metrics-only port).
//
// We don't dial the CP's gRPC for reads: every read can be served
// straight off etcd, which is the source of truth. Writes will need
// the CP gRPC (Phase A2.1).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultEtcdEndpoints = "127.0.0.1:2379"
	defaultMetricsPort   = "9090"
	defaultClusterID     = "kvcache"
	versionString        = "0.1.0"
)

// ---- shared flags ---------------------------------------------------------

type globalFlags struct {
	etcdEndpoints string
	timeout       time.Duration
	jsonOut       bool
}

func (g *globalFlags) register(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&g.etcdEndpoints, "etcd-endpoints",
		envOr("KVCACHE_ETCD_ENDPOINTS", defaultEtcdEndpoints),
		"comma-separated list of etcd endpoints")
	cmd.PersistentFlags().DurationVar(&g.timeout, "timeout", 5*time.Second,
		"per-call deadline")
	cmd.PersistentFlags().BoolVar(&g.jsonOut, "json", false,
		"emit JSON instead of the table format")
}

func (g *globalFlags) etcd() (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(g.etcdEndpoints, ","),
		DialTimeout: g.timeout,
	})
}

func envOr(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return fallback
}

// ---- members --------------------------------------------------------------

// NodeDescriptor mirrors the JSON kvstore-node's NodeRegistrar (Phase Q-1)
// publishes under /kvcache/nodes/<node_id>. Decode is permissive — any
// field we don't recognise is silently dropped so a future-format node
// doesn't crash the CLI.
type nodeDescriptor struct {
	NodeID              string   `json:"node_id"`
	Host                string   `json:"host"`
	GrpcPort            uint16   `json:"grpc_port,omitempty"`
	Version             string   `json:"version,omitempty"`
	State               string   `json:"state,omitempty"`
	PinnedBytes         uint64   `json:"pinned_bytes,omitempty"`
	DramBytes           uint64   `json:"dram_bytes,omitempty"`
	NvmeBytes           uint64   `json:"nvme_bytes,omitempty"`
	NumaNodes           uint32   `json:"numa_nodes,omitempty"`
	Vcpus               uint32   `json:"vcpus,omitempty"`
	SupportedTransports []string `json:"supported_transports,omitempty"`
}

func newMembersCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "members",
		Short: "List cluster nodes from etcd",
		RunE: func(cmd *cobra.Command, args []string) error {
			cli, err := g.etcd()
			if err != nil {
				return fmt.Errorf("dial etcd: %w", err)
			}
			defer cli.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), g.timeout)
			defer cancel()
			resp, err := cli.Get(ctx, "/kvcache/nodes/", clientv3.WithPrefix())
			if err != nil {
				return fmt.Errorf("get /kvcache/nodes/: %w", err)
			}
			nodes := make([]nodeDescriptor, 0, len(resp.Kvs))
			for _, kv := range resp.Kvs {
				var n nodeDescriptor
				if err := json.Unmarshal(kv.Value, &n); err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "kvctl: skip unparseable entry %s: %v\n",
						string(kv.Key), err)
					continue
				}
				nodes = append(nodes, n)
			}
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
			if g.jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(nodes)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "%-20s  %-22s  %-8s  %-12s  %s\n",
				"NODE_ID", "HOST", "PORT", "STATE", "VERSION")
			for _, n := range nodes {
				fmt.Fprintf(out, "%-20s  %-22s  %-8d  %-12s  %s\n",
					n.NodeID, n.Host, n.GrpcPort, defaultStr(n.State, "-"),
					defaultStr(n.Version, "-"))
			}
			if len(nodes) == 0 {
				fmt.Fprintln(out, "(no nodes registered)")
			}
			return nil
		},
	}
}

// ---- inspect --------------------------------------------------------------

func newInspectCmd(g *globalFlags) *cobra.Command {
	var metricsPort string
	cmd := &cobra.Command{
		Use:   "inspect <host>",
		Short: "Pull /metrics from a node and summarise the key gauges",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host := args[0]
			url := fmt.Sprintf("http://%s:%s/metrics", host, metricsPort)
			lines, err := fetchMetrics(cmd.Context(), url, g.timeout)
			if err != nil {
				return err
			}
			summary := summariseMetrics(lines)
			if g.jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(summary)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Node:    %s\n", host)
			fmt.Fprintf(out, "Metrics: %s\n\n", url)
			for _, k := range sortedKeys(summary) {
				fmt.Fprintf(out, "  %-40s  %g\n", k, summary[k])
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&metricsPort, "metrics-port", defaultMetricsPort,
		"HTTP port the kvstore-node /metrics endpoint listens on")
	return cmd
}

// ---- tier-stats -----------------------------------------------------------

func newTierStatsCmd(g *globalFlags) *cobra.Command {
	var metricsPort string
	cmd := &cobra.Command{
		Use:   "tier-stats <host>",
		Short: "Pull /metrics and filter to per-tier capacity / hit gauges",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host := args[0]
			url := fmt.Sprintf("http://%s:%s/metrics", host, metricsPort)
			lines, err := fetchMetrics(cmd.Context(), url, g.timeout)
			if err != nil {
				return err
			}
			tier := filterMetricsByPrefix(lines, "kv_tier_", "kv_pinned_", "kv_dram_",
				"kv_nvme_", "kv_cold_")
			if g.jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(tier)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Tier stats for %s\n\n", host)
			for _, k := range sortedKeys(tier) {
				fmt.Fprintf(out, "  %-40s  %g\n", k, tier[k])
			}
			if len(tier) == 0 {
				fmt.Fprintln(out, "(no tier metrics found — wrong port, or node has no tiers yet)")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&metricsPort, "metrics-port", defaultMetricsPort,
		"HTTP port the kvstore-node /metrics endpoint listens on")
	return cmd
}

// ---- quota ----------------------------------------------------------------

func newQuotaCmd(g *globalFlags) *cobra.Command {
	var clusterID string
	cmd := &cobra.Command{
		Use:   "quota <tenant_id>",
		Short: "Show tenant quota (read-only; --set is A2.1)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tenant := args[0]
			cli, err := g.etcd()
			if err != nil {
				return fmt.Errorf("dial etcd: %w", err)
			}
			defer cli.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), g.timeout)
			defer cancel()
			key := fmt.Sprintf("/kvcache/tenants/%s/%s", clusterID, tenant)
			resp, err := cli.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("get %s: %w", key, err)
			}
			if len(resp.Kvs) == 0 {
				return fmt.Errorf("tenant %s not found at %s", tenant, key)
			}
			// We don't strongly-type the body — the CP-side
			// TenantPublisher (Phase H-4) writes a JSON blob.
			// Pretty-print as-is.
			if g.jsonOut {
				_, err = cmd.OutOrStdout().Write(resp.Kvs[0].Value)
				if err == nil {
					fmt.Fprintln(cmd.OutOrStdout())
				}
				return err
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Tenant: %s\n", tenant)
			fmt.Fprintf(out, "Key:    %s\n", key)
			fmt.Fprintf(out, "Body:\n  %s\n", indentJSON(resp.Kvs[0].Value))
			return nil
		},
	}
	cmd.Flags().StringVar(&clusterID, "cluster", defaultClusterID,
		"cluster id (matches the CP --cluster flag)")
	return cmd
}

// ---- drain ----------------------------------------------------------------

// drainKey mirrors control-plane/internal/membership.DrainKey:
//   /kvcache/drain/<cluster>/<node_id>
func drainKey(cluster, nodeID string) string {
	return "/kvcache/drain/" + cluster + "/" + nodeID
}

func newDrainCmd(g *globalFlags) *cobra.Command {
	var clusterID string
	var reason string
	cmd := &cobra.Command{
		Use:   "drain <node_id>",
		Short: "Mark a node DRAINING (CP overlays it into the cluster view)",
		Long: "Writes a drain marker to etcd. The control-plane leader " +
			"overlays it onto /kvcache/cluster/view as State=draining; " +
			"kvagent routers then stop sending NEW prefixes to the node. " +
			"In-flight work finishes. Undrain by deleting the marker " +
			"(kvctl undrain — A2.2) or removing it via etcdctl.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := args[0]
			cli, err := g.etcd()
			if err != nil {
				return fmt.Errorf("dial etcd: %w", err)
			}
			defer cli.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), g.timeout)
			defer cancel()

			// Validate the node exists before draining — a typo'd node id
			// would otherwise write a marker that never matches anything.
			nodeKey := "/kvcache/nodes/" + nodeID
			resp, err := cli.Get(ctx, nodeKey)
			if err != nil {
				return fmt.Errorf("get %s: %w", nodeKey, err)
			}
			if len(resp.Kvs) == 0 {
				return fmt.Errorf("node %q not registered (no %s)", nodeID, nodeKey)
			}

			marker, _ := json.Marshal(map[string]any{
				"node_id":         nodeID,
				"reason":          reason,
				"requested_at_ns": time.Now().UnixNano(),
			})
			key := drainKey(clusterID, nodeID)
			if _, err := cli.Put(ctx, key, string(marker)); err != nil {
				return fmt.Errorf("put %s: %w", key, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"drain marker written: %s\n"+
					"  the CP will publish %s as DRAINING within one view tick;\n"+
					"  kvagent routers stop sending new prefixes there.\n",
				key, nodeID)
			return nil
		},
	}
	cmd.Flags().StringVar(&clusterID, "cluster", defaultClusterID,
		"cluster id (matches the CP --cluster flag)")
	cmd.Flags().StringVar(&reason, "reason", "operator drain via kvctl",
		"free-text reason recorded in the drain marker")
	return cmd
}

// ---- trace (stub) ---------------------------------------------------------

func newTraceCmd(g *globalFlags) *cobra.Command {
	_ = g
	return &cobra.Command{
		Use:   "trace <request_id>",
		Short: "Tail request-level trace events (NOT YET IMPLEMENTED)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = args
			fmt.Fprintln(cmd.ErrOrStderr(),
				"kvctl trace: not yet implemented.")
			fmt.Fprintln(cmd.ErrOrStderr(),
				"  Needs the kvstore-node Subscribe gRPC stream (Phase M-2)")
			fmt.Fprintln(cmd.ErrOrStderr(),
				"  filtered by request_id + OTLP span correlation (J-1).")
			fmt.Fprintln(cmd.ErrOrStderr(),
				"  Land in Phase A2.2.")
			return fmt.Errorf("trace tail not yet implemented (see message above)")
		},
	}
}

// ---- version --------------------------------------------------------------

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "kvctl "+versionString)
			return nil
		},
	}
}

// ---- helpers --------------------------------------------------------------

// fetchMetrics returns the non-comment, non-empty lines of a Prometheus
// /metrics body. Errors out on non-2xx or read failure.
func fetchMetrics(ctx context.Context, url string, timeout time.Duration) ([]string, error) {
	c := &http.Client{Timeout: timeout}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	out := make([]string, 0, 64)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, nil
}

// summariseMetrics parses Prometheus text-format `key{labels} value` lines
// and reduces them to a flat name → value map (label-bearing series get the
// label suffix appended). Caller decides display layout. Errors are silent
// per-line so a single malformed series doesn't take down the summary.
func summariseMetrics(lines []string) map[string]float64 {
	out := make(map[string]float64, len(lines))
	for _, l := range lines {
		// Split into `name+labels` and `value`.
		spaceIdx := strings.LastIndex(l, " ")
		if spaceIdx <= 0 {
			continue
		}
		name := l[:spaceIdx]
		var val float64
		if _, err := fmt.Sscanf(l[spaceIdx+1:], "%g", &val); err != nil {
			continue
		}
		out[name] = val
	}
	return out
}

// filterMetricsByPrefix keeps only entries whose name starts with any of
// the supplied prefixes.
func filterMetricsByPrefix(lines []string, prefixes ...string) map[string]float64 {
	all := summariseMetrics(lines)
	out := make(map[string]float64)
	for k, v := range all {
		for _, p := range prefixes {
			if strings.HasPrefix(k, p) {
				out[k] = v
				break
			}
		}
	}
	return out
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func defaultStr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func indentJSON(raw []byte) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	out, err := json.MarshalIndent(v, "  ", "  ")
	if err != nil {
		return string(raw)
	}
	return string(out)
}

// ---- main -----------------------------------------------------------------

func main() {
	g := &globalFlags{}
	root := &cobra.Command{
		Use:           "kvctl",
		Short:         "Operator CLI for the KV Cache cluster",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	g.register(root)
	root.AddCommand(
		newMembersCmd(g),
		newInspectCmd(g),
		newTierStatsCmd(g),
		newQuotaCmd(g),
		newDrainCmd(g),
		newTraceCmd(g),
		newVersionCmd(),
	)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
