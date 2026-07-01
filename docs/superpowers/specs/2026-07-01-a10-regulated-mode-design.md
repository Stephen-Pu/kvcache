# A10 — Regulated Mode (FedRAMP + sovereign-cloud)

Status: **Design** (Phase-3; roadmap Gantt "FedRAMP / sovereign-cloud path", 180d).
Date: 2026-07-01.
Scope: A10 only. Builds on and constrains [A9](2026-07-01-a9-cross-cluster-dr-federation-design.md).

## 1. Problem & goal

KV blocks are **derived from user prompts/context**, so they are in-scope
sensitive data for both (a) US-federal control regimes (FedRAMP / NIST
800-53) and (b) data-**residency** / sovereign-cloud requirements. Today the
product has the primitives — at-rest AES-256-GCM (B3.2), TLS + mTLS in transit
(N-1/N-2), SPIFFE identity (B8/A11), an audit sink (`audit.cpp`) — but nothing
that (1) guarantees sensitive data/keys/telemetry never leave a declared
boundary, nor (2) presents a control posture an auditor can map.

**Goal:** a single hardened deployment profile — **Regulated Mode** — that
satisfies both regimes, centered on a **fail-closed boundary/egress guard**,
plus a NIST-800-53 control map for the ATO conversation.

## 2. Requirements (decided)

| # | Decision | Rationale |
|---|----------|-----------|
| R1 | **One "Regulated Mode" profile** covers both FedRAMP and sovereign. | The overlapping controls (residency, crypto, audit, identity) are best enforced by one mode, not two. |
| R2 | **Configurable perimeter** — operator declares an allowlist (regions / accounts / host patterns / CIDRs); Mode enforces "nothing sensitive leaves it." Empty allowlist ⇒ air-gap. | One mechanism spans sovereign-region, GovCloud account, and air-gap. |
| R3 | **Boundary/egress guard is the centerpiece**, enforced **fail-closed** at startup and runtime, by the product itself. | Residency is the spine; delegating solely to network config isn't a product control and isn't testable. |
| R4 | **Off by default** — Regulated Mode is opt-in; disabled ⇒ current behavior byte-for-byte. | No tax on non-regulated deployments. |
| R5 | Ship a **NIST 800-53 control-family map** (satisfied / config / gap). | The FedRAMP-facing artifact; also drives the follow-on backlog. |

### Non-goals
- Not an ATO package / SSP authoring (that's a compliance-team artifact this
  design *feeds*).
- Deep implementations of FIPS mode, KMS-envelope custody, and tamper-evident
  audit are **designed to the interface here** but built as follow-ons.
- Cross-boundary data federation (see §5) is explicitly forbidden, not designed.

## 3. Regulated Mode

A profile toggled by a `regulated_mode` config flag (validated at process
start). When **on**, the node/CP refuse to start unless all hold:

1. `BoundaryGuard` is active with a non-null `BoundaryPolicy` (§4).
2. Crypto runs in **FIPS** mode (§6).
3. Keys come from a **KMS envelope**, not a plaintext `Options.key` (§6).
4. Audit is enabled (§6).
5. Every configured egress sink passes the guard's startup gate (§4).

Any unmet requirement ⇒ non-zero exit with a specific error. Off ⇒ unchanged.

## 4. BoundaryGuard (centerpiece)

```
struct Endpoint { std::string host; uint16_t port; Purpose purpose; };
enum class Purpose { kColdTier, kKms, kTelemetry, kReplication, kEtcd, kOther };
struct BoundaryPolicy { std::vector<Rule> allow; bool default_deny = true; };
struct Decision { bool allow; std::string reason; };

class BoundaryGuard {
 public:
  explicit BoundaryGuard(BoundaryPolicy);
  Decision Check(const Endpoint&) const;   // pure, thread-safe, unit-testable
};
```

- **Rules** match host glob / suffix, CIDR, or a region/account tag resolved
  from the endpoint. `default_deny` ⇒ anything not explicitly allowed is denied
  (air-gap = empty allow list).
- **Seams** (each adds a one-line `Check` before dialing; deny ⇒ error +
  `AU` audit event + `kv_boundary_denied_total` metric):
  - cold-tier `CurlHttpTransport` (S3 / REST UFS, incl. SigV4 target from B5),
  - KMS client (§6),
  - OTLP / telemetry exporter (J-2),
  - A9 `ReplicationConsumer` dial + `ReplicaFetch` target,
  - etcd / gRPC peer dialer.
- **Startup gate:** enumerate all statically-configured sinks, `Check` each,
  refuse to boot on any deny (names the offending sink).
- **Runtime:** every dial re-checks — defense in depth; catches HTTP redirects,
  dynamically-resolved A9 targets, and re-configured sinks.
- **Fail-closed always.** No path fails open; a guard construction failure or
  an unresolvable endpoint is a deny.

**Complementary control (not a substitute):** the design *requires*, as a
deployment control under SC-7, a network-layer egress boundary (K8s
NetworkPolicy / egress firewall / no-egress subnet). BoundaryGuard is the
in-product control; the network policy is belt-and-suspenders.

## 5. A9 ↔ A10 reconciliation

Regulated Mode **forbids cross-boundary A9 replication**: the guard denies a
replication target outside the declared perimeter. Consequences:

- A DR standby must be **in-boundary** — same region/jurisdiction, a different
  failure domain (AZ / rack). This satisfies DR without violating residency.
- Because replication never crosses a boundary, A9's **shared SPIFFE trust
  domain** (A9 R4) is correct and stays. Distinct-trust-domain, cross-boundary
  federation is a **separate, non-regulated** future — Regulated Mode does not
  enable it. *(This refines the A9 §9 note, which speculated A10 would move to
  distinct domains; the actual A10 answer is "cross-boundary is forbidden, so
  shared-domain stays." The A9 spec §9 is updated to match.)*

## 6. Other controls (interface-level design; impl deferred)

- **FIPS crypto mode** — load the OpenSSL FIPS provider; refuse non-approved
  ciphers; assert the provider is active at startup (fail-closed). Affects
  B3.2 (AES-GCM), N-1/N-2 (TLS/mTLS), A11 (X.509).
- **`IKeyProvider` (KMS envelope)** — replace `EncryptingColdTier::Options.key`
  (plaintext) with `IKeyProvider::CurrentDek()` that fetches + unwraps a DEK
  from an in-boundary KMS (whose endpoint the guard checks) and supports
  rotation. Plaintext-key config is rejected in Regulated Mode.
- **Tamper-evident audit** — hash-chain audit records (each carries the hash of
  the prior), append-only, with a periodic external anchor. Regulated Mode
  requires audit enabled + complete coverage of access + boundary-deny events.

## 7. NIST 800-53 control map (excerpt)

| Family / control | Mechanism | Status |
|---|---|---|
| AC-2/3/6, IA-2/5 (identity, least-priv) | SPIFFE tenant + workload SVID (B8/A11), tenant-cert binding N-3/4/5 | Satisfied |
| SC-7 (boundary protection) | **BoundaryGuard** (§4) + NetworkPolicy (deployment) | New (this design) |
| SC-8 (transmission confidentiality) | TLS (N-1) + mTLS (N-2), FIPS ciphers in FIPS mode | Satisfied |
| SC-12/13 (key mgmt, FIPS crypto) | B3.2 AES-256-GCM; **+ KMS envelope + OpenSSL FIPS** | Gap (§6) |
| SC-28 (protection at rest) | B3.2 `EncryptingColdTier` | Partial — T1/T2 in-memory unencrypted, pinned RAM in-boundary (documented, accepted) |
| AU-2/3/9/12 (audit + integrity) | `audit.cpp`; **+ hash-chain / complete coverage** | Partial → Gap (§6) |
| SC-7(residency) / sovereign | BoundaryGuard allowlist scoped to jurisdiction | New (this design) |

*(Full baseline mapping — CM, CP, SI, etc. — is a compliance-team artifact this
table feeds; the rows above are the ones the data-layer product owns.)*

## 8. Error handling / fail-closed semantics

- Startup: any unmet Regulated-Mode requirement or any out-of-boundary sink ⇒
  refuse to boot, error names the cause.
- Runtime: a guard deny ⇒ connection refused + `AU` event + metric; the calling
  operation returns a clear residency error (never silently degrades).
- FIPS provider unavailable, KMS unreachable at boot ⇒ refuse to start.
- No fail-open path exists.

## 9. Testing (local / unit — no cloud, no auditor)

- `BoundaryGuard::Check` — host-glob / CIDR / tag matching, `default_deny`,
  per-`Purpose`, empty-allowlist=air-gap.
- Seam test — a fake cold-tier transport handed an out-of-boundary URL is
  blocked + emits the audit event.
- Startup-gate test — a config with an out-of-boundary sink ⇒ `Create` returns
  an error (fail-closed), and an all-in-boundary config boots.
- Regulated-Mode requirement gate — missing KMS provider / non-FIPS build ⇒
  refuse to start (mockable via the requirement checks).
- The control map is a documentation artifact (no test).

## 10. Deferred / follow-on
- FIPS provider integration + validated-cipher enforcement.
- `IKeyProvider` KMS integration (AWS KMS / Azure Key Vault / on-prem HSM) + DEK rotation.
- Hash-chained audit implementation + anchor.
- Optional at-rest encryption of T1/T2 in-memory tiers (currently accepted risk).
- SSP / ATO package authoring (compliance team; fed by §7).

## 11. Reuse ledger

| Need | Reuse / relation |
|------|------------------|
| At-rest encryption | B3.2 `EncryptingColdTier` (AES-256-GCM) |
| In-transit encryption | N-1 TLS, N-2 mTLS |
| Identity / access control | B8 SPIFFE, A11 workload identity, N-3/4/5 tenant-cert binding |
| Audit sink | `security/audit.cpp` (extend to hash-chain) |
| Telemetry egress (a guarded sink) | J-2 OTLP exporter |
| Cold-tier / KMS egress (guarded sinks) | B3 `RestColdTier`, B5 SigV4 |
| Federation egress (guarded sink) | A9 `ReplicationConsumer` / `ReplicaFetch` |
