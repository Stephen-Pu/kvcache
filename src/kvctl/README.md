# kvctl

Operator CLI. LLD §8.3.

Subcommands (Phase A2):

| Command | Status | What it does |
|---------|--------|--------------|
| `kvctl members`              | ✅ live   | List cluster nodes from etcd `/kvcache/nodes/` |
| `kvctl inspect <host>`       | ✅ live   | HTTP GET `host:9090/metrics`, summary print |
| `kvctl tier-stats <host>`    | ✅ live   | Same, filtered to `kv_tier_/kv_pinned_/kv_dram_/kv_nvme_/kv_cold_` |
| `kvctl quota <tenant>`       | ✅ live (read-only) | Read tenant config from etcd. `--set` is A2.1 (needs CP gRPC write API). |
| `kvctl version`              | ✅ live   | Print build version |
| `kvctl drain <node>`         | ⚠️ stub  | Needs CP node-state-mutation RPC. Lands in A2.1. |
| `kvctl trace <request-id>`   | ⚠️ stub  | Needs Subscribe stream + OTLP correlation. Lands in A2.2. |

Global flags:
- `--etcd-endpoints` (env `KVCACHE_ETCD_ENDPOINTS`, default `127.0.0.1:2379`)
- `--timeout` (default `5s`)
- `--json` (emit JSON for scripting; tables otherwise)

Examples:

```bash
kvctl members                                    # cluster nodes table
kvctl members --json                             # for piping into jq
kvctl inspect node-a.kvcache.svc                 # node /metrics summary
kvctl tier-stats node-a --metrics-port=9090
kvctl quota my-tenant --cluster=prod
```

## Build & test

```bash
cd src/kvctl
go test -short -count=1 ./...
go build ./...
```
