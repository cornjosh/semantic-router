# Adaptive routing behind Higress

Status: implementation brief authorized by the user's 2026-10-22 goal. This supersedes the PD and single-coordinator recommendations in the discovery report. Routine implementation choices and plan execution are autonomous; no further design approval gates.

## Outcome and scope

Build a thin Higress Wasm plugin which collects request information, asks an external Python decision service, and applies its decision. Higress forwards the original generation request and streams the response; generation never passes through the decision service. External LLM, Jev/System One, and embedding services remain independently provided HTTP APIs.

First release excludes PD and physical resource reservation. It selects a qualified model, then one of potentially several clusters serving that model, using passive TTFT, latency, failures, freshness and outstanding requests. Measurements describe observed performance, not guaranteed GPU capacity. Configurable active telemetry can supplement passive observations. Cross-node HA remains required in the etcd deployment profile. The default standalone profile embeds SQLite and requires only one container and a local data volume; it does not claim node-failure tolerance. Both profiles provide durable online learning.

The signal layer only extracts information. The policy layer alone selects candidate models and requests early termination of signal processing. Optional signals can be disabled without initializing their provider, making network calls or requiring provider credentials. Tool-result/request shortcuts can avoid all later expensive semantic signals while still passing final capability and cluster checks.

## Versions and repository boundaries

- Python remains compatible with the package's `>=3.10,<3.14` constraint. Existing `BaseRouter`, `IndexedRouter`, `DecisionRouter`, `Chain`, `Merge` APIs stay compatible.
- New Python code lives in `semantic_router/adaptive/`, using the existing aiohttp runtime (no new server dependency).
- Higress target is **v2.2.5**, the latest stable GitHub release checked on 2026-10-22 (published 2026-10-04). Pin release references and build dependencies; do not float `main` or container `latest`.
- Deployment supports standalone Docker with SQLite, and Kubernetes HA with an application etcd cluster. No production cluster credentials or inference endpoints have been provided. Ship complete deployment artifacts and test locally, retaining the distinction between real proxy execution and live Kubernetes/provider validation.
- Work branch: `feat/higress-adaptive-routing`. After each tested feature, commit and push this branch to `origin`; do not push to upstream or merge main automatically.
- Paid provider calls are disabled unless the user supplies a budget. All example endpoints and measurements must be labelled as examples or synthetic where applicable.

## Component boundaries

### Request and signal contracts

`RoutingRequest` contains a request ID, trusted tenant identity, model selector, messages, tools, tool choice, response format, streaming flag and generation limit. Reject malformed/ambiguous requests, non-finite values and unsupported shapes at ingress. A request's identity is bound to a canonical fingerprint; replaying an ID with different content must conflict.

The built-in `request` signal extracts structural facts: requested model, tool-result presence, tool/function requirements, modalities, structured output, input size and output limit. It has no provider and no route table. Other named signals return `SignalResult(name, status, value, source, elapsed_ms)`; unavailable/timeout is explicit, never a made-up confident value. Semantic text selection and full-context capacity accounting are different concerns.

Configured signal stages execute in order; independent signals within a stage may execute concurrently under per-signal and total deadlines. A stage completes before policy inspection. Disabled signals are absent and not constructed. Cancellation propagates. Exceptions are sanitized before returning to callers. Signal providers never import a policy, access a target cluster registry or return a route decision.

### Policy and shortcuts

Ordered declarative rules match signal name, nested field, operator and value. Operators are a small typed set (`eq`, `in`, `gte`, `lte`, `exists`), not arbitrary code. Missing/unavailable signals never satisfy value predicates. A matched rule yields an allowlist of logical model IDs; an explicit `shortcut` flag ends further extraction. Non-shortcut matches remain provisional until extraction completes. A configured fallback provides conservative model candidates or rejection.

Shortcuts change how much evidence is collected, not permission to bypass constraints. The final selection always enforces explicit model selection, tenant permissions, modality/tools/schema support, context/output capacity, model availability and cluster health. If accurate token counting is unavailable, that uncertainty must be explicit; never call a general character estimate exact or silently truncate. Production configuration can require an exact tokenizer/counter or a declared conservative upper bound. Logical model IDs and cluster IDs are distinct; one model may have many clusters.

### Providers

External provider configuration includes protocol, full endpoint/base URL, model, environment variable for credentials, timeout, concurrency and response size limits. Jev supports configurable System One endpoint paths and multi-question category/difficulty extraction. LLM classification validates structured output and treats its certainty as a model estimate, not calibrated task success. Embedding signals use the existing encoder/router abstractions where practical and report score semantics explicitly. No production external model implementation is required. For local validation the user permits downloading lightweight models, preferentially from ModelScope; keep weights outside git and report model/revision and real versus synthetic evidence separately.

Providers share pooled HTTP connections, have a total request budget, bounded response bodies and no uncontrolled retry chain. Disabled providers do no startup calls. Tests use real loopback HTTP servers for wire contracts; these are not claims about supplier quality.

### Model selection and online learning

Hard filtering precedes all optimization and exploration. Start with a configurable contextual Beta posterior per `(tenant, policy/feature version, category/difficulty bucket, model version)`. Update success/failure counts only from authenticated, attributed quality feedback. Use posterior means with configurable epsilon-greedy exploration among qualified models; record the actual selection propensity and candidate set. This makes online updates cheap, inspectable and shared across replicas. Default exploration is zero until explicitly configured.

Learning state is durable and versioned. Feedback must reference an existing decision, match its tenant and actual model/version, and be applied once. Missing quality feedback does not mean success or failure. Gateway 5xx/timeouts update operational reliability and cluster pressure, not semantic capability. Tests must show feedback changes a later choice and is visible immediately from another process.

### Cluster selection and passive load

Within the selected model, choose a configured healthy cluster using normalized predicted TTFT/latency, recent error rate, outstanding requests and freshness. Priors and weights are configuration; compare the same units. Maintain EWMAs, sample counts, timestamps and a separate failure/cooldown state per cluster. Expired observations return to explicit priors or a configured unavailable state. New/unused clusters must have a deliberate cold-start policy rather than accidental starvation.

Gateway observations identify decision, attempt, cluster and event. TTFT is measured from upstream dispatch to the first meaningful SSE output, not classifier start, response headers or arbitrary keepalive bytes. Non-streaming requests can report completion latency with TTFT unknown. No missing timing field is converted to zero. End/reset callbacks are idempotent. Client disconnects, missing observations and service restarts require bounded accounting; this is a load estimate, not a hard resource reservation guarantee.

Optional active polling is disabled by default, uses configured trusted endpoints and bounded timeouts, and records source/freshness. Polling is not performed synchronously on every routing request; HA ownership/leases avoid duplicate polling across replicas.

### Shared state and high availability

The user's later 2026-10-22 instructions supersede the original PostgreSQL design: no PostgreSQL in the new service. Default to embedded SQLite using the Python standard library, FULL synchronous durability (WAL on SQLite >=3.51.3, rollback journal on older system runtimes to avoid the upstream WAL-reset bug) on a local persistent volume. SQLite supports one machine, including local concurrent worker processes; never present independent per-pod files or a shared network filesystem as cross-node HA.

Provide an optional etcd backend for cross-node HA. Use an existing **application** etcd cluster when available, or the supplied three-member deployment. Do not use Kubernetes control-plane etcd. Both backends implement one state contract and the same pure update logic; compare-and-swap transactions protect decisions, events, counters and cluster aggregates. The etcd backend uses its built-in v3 JSON gateway through the existing aiohttp dependency, avoiding a new client runtime. Reads are linearizable and writes atomically compare every read revision; bounded retries fail closed when contention or quorum failure prevents progress. No homemade consensus, no embedded Raft dependency, no PostgreSQL fallback.

Record the complete decision before returning it. Concurrent identical requests must return the same durable result and apply feedback only once. Learning counts survive process restart. Outstanding requests are derived from bounded per-cluster unexpired decision entries; store failures cause readiness failure and bounded service-unavailable responses. Polling leases are advisory and fenced by atomic revisions/owner, with synchronized service clocks required for timestamp/freshness comparisons. Missing quorum never forks learning into per-replica state.

SQLite is the simplest single-container deployment. Cross-node HA necessarily adds shared consensus infrastructure unless etcd already exists; describe this cost explicitly. Provide distinct Docker/SQLite and Kubernetes/etcd examples, not an invalid SQLite replicas>1 default. Preserve the original library's optional Postgres index feature unchanged.

### HTTP service

Provide `/healthz`, `/readyz`, `/metrics`, `POST /v1/route`, `POST /v1/observations` and `POST /v1/feedback`. An optional classification-only endpoint can expose signals without targets. Version the wire schema. Route responses include decision ID, model ID, cluster/route ID, reason, policy version and signal status summary; they never contain arbitrary destinations or provider secrets. Metrics do not use request IDs or raw prompts as labels.

Authentication separates gateway routing/observation credentials from trusted quality-feedback credentials and binds tenants. Preserve fixed deadlines, bounded request bodies/concurrency, validation errors, retryable overload status and draining behavior. API documentation includes curl examples using keyword parameters in Python examples.

### Higress plugin

Go Wasm plugin buffers a bounded request, extracts the classifier envelope and calls the configured decision-service cluster. It validates response schema, policy version as configured, and destination allowlists, then rewrites model and route matching fields. Clear/recompute routes with the supported Higress API and verify real upstream selection. Plugin ordering must work with Higress v2.2.5 model-router/ai-proxy.

Streaming generation bytes remain in the proxy data path. Observe SSE incrementally across fragmented events with a bounded parser; do not buffer the full answer. Report passive outcomes asynchronously. A lost observation must not corrupt global state. Classification failure follows an explicit fail-closed or capability-checked fallback policy; it must not redirect arbitrary fixed-model/tool requests to an incompatible default.

## Configuration and artifacts

Deliver validated YAML examples separating providers/signals, policy rules, model capabilities, cluster routes, load weights and learning settings. Include a same-model/two-cluster example, tool shortcut, disabled expensive signals and quality-feedback example. Secrets reference environment variables or Kubernetes Secrets and are never embedded in images or examples.

Deliver a reproducible Python service image definition, Go build/OCI plugin packaging, Higress v2.2.5 Kubernetes configuration, SQLite schema, optional etcd setup, local test-service setup, end-to-end runner, benchmark runner and Chinese operator documentation. Only `/mnt/workspace` persists here; bootstrap and download manifests must make tool/runtime restoration possible.

## Acceptance and evidence

1. Unit and real HTTP tests prove extraction/policy separation, disabled provider no-work behavior, rule ordering, early exit, deadline/cancellation and conservative failures.
2. Real SQLite and etcd tests from multiple service processes prove shared learning, atomic duplicate events, identical retry decisions, restart persistence and worker failover. A YAML replica count alone is insufficient. Exercise three real etcd members, leader loss, quorum loss and recovery. SQLite tests prove local durability, not cross-node HA.
3. Same-model multi-cluster tests prove passive TTFT/latency feedback changes cluster preference, malformed/stale data is handled, and network failures do not train quality failure.
4. Real Go/Wasm build and proxy host load; actual upstream switching and streaming byte preservation; classifier timeout, overload, malformed/unknown route and reset tests.
5. Kubernetes artifacts render and validate against the pinned release. Run a disposable local API server/cluster when feasible; report deployment evidence precisely instead of treating a standalone proxy as a Kubernetes installation.
6. Reproducible local end-to-end benchmark reports routing and gateway overhead separately, p50/p95/p99, throughput, errors, signal calls avoided and online-learning effects; synthetic workload/feedback is labelled. No production accuracy or SLA claim without data.
7. Existing compatible test suites, lint/type checks, independent final review, requirement audit and verified remote commits for each completed feature.
