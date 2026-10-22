# Adaptive Higress Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. The user authorized autonomous execution and remote pushes after each completed feature; no repeated approval handoff.

**Goal:** Deliver a Higress plugin and highly available external decision service with configurable isolated signals, shortcut policies, passive multi-cluster load routing and online learning.

**Architecture:** Higress retains all generation traffic. A staged signal pipeline supplies facts to declarative policy; constrained model and cluster selection use embedded SQLite or optional shared etcd state. Gateway timing observations and authenticated quality feedback update distinct operational and learning models.

**Tech Stack:** Python 3.10–3.13, Pydantic, aiohttp, FastAPI/uvicorn, stdlib SQLite / optional etcd v3 JSON API; Go Wasm SDK; Higress v2.2.5; Kubernetes.

**Spec:** `docs/superpowers/specs/2026-10-22-adaptive-higress-routing.md`

## Global Constraints

- Python remains compatible with `>=3.10,<3.14`; preserve existing library APIs.
- Target Higress v2.2.5, checked latest release 2026-10-22.
- No PD, inference engine, or generation proxy in the Python service.
- Signal providers never choose a target; policy alone decides shortcuts and candidate models.
- SQLite is the default standalone store; etcd is authoritative across HA replicas. No local fallback on etcd errors and no cross-host shared SQLite files.
- Paid provider calls disabled; synthetic fixtures are not real quality measurements.
- Feature branch `feat/higress-adaptive-routing`; commit and push each verified feature to origin.
- Use `uv run` for project development commands. External tool bootstrap is isolated in `/mnt/workspace/.tools/semantic-router`.

## Review Focus

- Fixed model and tools/schema constraints remain enforced after shortcuts and failures (Tasks 1, 4, 5).
- Cancellation/fragmented SSE/end callbacks do not fabricate TTFT, mutate response bytes or duplicate statistics (Tasks 3, 5, 6).
- Concurrent duplicate requests/events across replicas and lost HTTP acknowledgements do not double-apply updates (Tasks 3, 4, 6).
- Missing/disabled/stale signals and cold-start clusters do not silently appear confident or permanently lose traffic (Tasks 1–4).
- Feedback cannot cross tenants, relabel models or update another policy version (Tasks 3, 4).

## File map

`semantic_router/adaptive/models.py` defines wire and internal facts; `config.py` validates trusted deployment/policy configuration; `signals/` extracts facts; `policy.py` evaluates rules; `pipeline.py` controls staged extraction. `providers.py` and individual signal adapters own external protocols. `selection.py` implements constrained model/cluster scoring. `state/` owns durable transactions and migrations. `service.py`, `app.py`, `__main__.py` compose the API. `plugins/higress-adaptive/` contains the Go plugin. `deploy/adaptive/`, `examples/adaptive/`, `scripts/adaptive/`, `tests/{unit,integration}/adaptive/` contain deployment, examples, tools and tests. Keep files focused; split modules when interfaces become hard to inspect.

### Task 1: Isolated signal pipeline and declarative policy

**Files:** new `semantic_router/adaptive/{__init__,models,config,policy,pipeline}.py`, `signals/{__init__,base,structural}.py`; `tests/unit/adaptive/test_pipeline.py`, `test_adaptive_config.py`; configuration example and user documentation.

**Interfaces:** Produces `RoutingRequest`, `SignalResult`, `SignalSpec`, `PolicyRule`, `PolicyMatch`, `PipelineResult` and `SignalPipeline.run(request)`. Extractors implement async `extract(request) -> dict`; registry factories are invoked only for enabled signals. Built-in `request` facts are available before optional stages. Consumers in Tasks 2/4 use these exact contracts.

- [x] Write behavioral tests: tool-result shortcut skips a later extractor that would fail if called; disabled missing provider never constructs; different policy maps identical facts differently; provisional rule waits for later evidence; unavailable/missing value does not match; stage timeout and total deadline bound execution; cancellation propagates; extraction result cannot mutate previous signals.
- [x] Run `uv run pytest tests/unit/adaptive/test_pipeline.py tests/unit/adaptive/test_adaptive_config.py -q`; expected RED for missing behavior, then implement smallest independent contracts/pipeline/rules.
- [x] Validate duplicate names, invalid rule references, negative limits, unknown models, disabled-signal rule semantics and finite numeric values. Add runnable structural-only example.
- [x] Run focused tests, existing unit/functional suite and ruff/mypy for changed files; record unavailable optional integrations separately.
- [x] Commit `feat(adaptive): separate configurable signals from shortcut policies`; push origin feature branch and verify remote SHA.

**Completion contract:** Real extraction and policy behavior tested; no route table/provider credential coupling; default package import stays compatible.

### Task 2: External semantic signal adapters

**Files:** new `adaptive/providers.py`, `signals/{embedding,systemone,llm}.py`, provider config; `tests/integration/adaptive/test_providers.py`; API examples.

**Interfaces:** Consumes Task 1 extractors; produces category/difficulty/embedding facts with explicit score type and availability. Provider endpoint/model/env-key references live outside rules.

- [x] Start real loopback provider fixtures; write RED tests for System One multi-question wire request, custom endpoint, embedding outputs and structured LLM output.
- [x] Implement pooled HTTP transport, response size bounds, total budgets and provider concurrency; reuse existing semantic-router interfaces without changing public defaults.
- [x] Test malformed/NaN/partial probabilities, errors, oversized response, deadline, cancellation, no secrets in errors, disabled provider with absent credential, and multi-turn/tool text selection.
- [x] Run focused HTTP + Task 1 tests and lint/type checks; commit `feat(adaptive): add external semantic signal providers`; push and verify.

**Completion contract:** Three protocols exercised over HTTP with no real paid calls or invented provider accuracy.

### Task 3: Shared PostgreSQL state, passive load and online learning

**Files:** new `adaptive/state/{base,postgres}.py`, migration SQL, `selection.py`; PostgreSQL local setup; `tests/integration/adaptive/test_shared_state.py`, `tests/unit/adaptive/test_selection.py`.

**Interfaces:** Atomic decision lookup/record by tenant+request ID+fingerprint; observation/feedback insertion with unique event identity; versioned Beta posterior; cluster EWMA/inflight/cooldown/freshness snapshot; optional polling leases. API service consumes transactions, never reconstructs authoritative state in memory.

- [x] Write RED tests against a uniquely named real PostgreSQL schema: two store instances update same events concurrently; feedback changes later model choice; operational failure leaves quality counts unchanged; request-ID conflicts; restart persistence.
- [x] Implement migrations, transactions and atomic update methods; contextual Beta means and epsilon-greedy propensity; explicit priors; separate model and cluster selection.
- [x] Test same-model/two-cluster latency shift, TTFT unknown, stale/out-of-order observations, duplicate terminal events, cooldown/recovery, missing outcomes, cross-tenant and wrong-model/version feedback.
- [x] Implement disabled-by-default bounded active telemetry polling/ownership with a real local telemetry endpoint test.
- [x] Run real multi-process shared-state tests, unit suites and checks; commit `feat(adaptive): share load and online learning across replicas`; push and verify.

**Completion contract:** Actual concurrent processes share durable updates; no simulated database-only proof; production requirements for HA PostgreSQL documented.

### Task 3 revision: SQLite default and optional etcd HA (user steering)

The earlier PostgreSQL feature was tested and pushed, but is superseded by the user's request to avoid PG and deployment complexity. Keep its history; replace it in a forward commit before Task 4.

- [x] Port the existing behavioral contract suite to real SQLite files and actual etcd; keep concurrent and separate-process tests.
- [x] Share domain update logic behind atomic key/value transactions; implement SQLite durable journaling/FULL and etcd linearizable reads/CAS with bounded contention/network deadlines.
- [x] Remove the new PostgreSQL implementation/setup/SQL and server dependency; preserve the library's existing Postgres index and extra.
- [x] Test restart, duplicate events, leases, bounded pending state, lock contention, three-member etcd leader loss/quorum loss/recovery. Document the single-node versus HA boundary.
- [x] Run regression/lint/types; commit and push the completed replacement feature.

### Task 4: Decision HTTP service with hard constraints and HA behavior

**Files:** `adaptive/{service,app,__main__}.py`, server extra, API schemas/config, integration HTTP tests and service image.

**Interfaces:** `POST /v1/route`, `/v1/observations`, `/v1/feedback`, health/readiness/metrics. Route decision supplies a logical target from trusted configuration, never an arbitrary URL. Strict auth roles and tenant binding.

- [ ] Write RED end-to-end HTTP tests using two server processes plus etcd (and SQLite standalone coverage): identical concurrent requests return the same persisted decision, kill/restart a worker, learning visible from the survivor.
- [ ] Implement composition, capability/model/context filters, request fingerprint/idempotency, authentication, bounded concurrency and deadlines, graceful shutdown.
- [ ] Test fixed models, tool shortcuts, unsupported modality/schema/context, missing exact count policy, invalid auth/tenant, DB outage, overload, input bounds and secret redaction.
- [ ] Add usable YAML examples, including one model in two clusters. Verify commands and API docs against running service.
- [ ] Run focused HA/HTTP suites and regressions; commit `feat(adaptive): serve durable routing decisions over HTTP`; push and verify.

**Completion contract:** Multi-process failover is observed; only facts enter policy; downstream generation never traverses this service.

### Task 5: Higress Go Wasm request routing and observations

**Files:** `plugins/higress-adaptive/` Go module/build/config/tests; static proxy test configuration; scripts to acquire pinned toolchain/runtime.

**Interfaces:** Consumes Task 4 versioned schema. Uses a configurable classifier cluster and allowlisted logical target map. Emits observations with deterministic event/attempt IDs.

- [ ] Inspect v2.2.5 SDK and runtime APIs; write RED host tests for request envelope, route/model rewrite, asynchronous resume exactly once, unknown targets and deadlines.
- [ ] Implement plugin stages and bounded body handling. Test trusted header reset, fixed-model fallback constraints and interaction with model-router/ai-proxy.
- [ ] Add bounded incremental SSE observation parser; test split JSON/events, keepalives, content/tool deltas, nonstream responses, reset/end de-duplication and exact response byte preservation.
- [ ] Build actual Wasm, run with a real compatible proxy binary and loopback backends; verify distinct upstreams receive rewritten model requests and stream bytes unchanged.
- [ ] Run Go/race/host tests and build; commit `feat(higress): route through external adaptive decisions`; push and verify.

**Completion contract:** Real Wasm loads and forwards to selected upstream; a Go-only emulator run is not sufficient for final gateway evidence.

### Task 6: Kubernetes deployment and complete local chain

**Files:** `deploy/adaptive/`, local/e2e scripts, pinned runtime manifest, deployment docs, CI workflow.

- [ ] Add Python image, plugin OCI packaging, v2.2.5 WasmPlugin and ingress/service mappings, replicas/PDB/spread/probes/secrets/config and standalone SQLite and optional three-member etcd profiles.
- [ ] Render/validate manifests against exact CRDs; exercise local Kubernetes API or disposable cluster if feasible. Document deployment evidence and platform limits exactly.
- [ ] Run proxy→two service replicas→etcd→HTTP providers/upstreams with failures, SSE and feedback; verify counts/choices after one replica exits.
- [ ] Add CI jobs for changed Python/Go/integration artifacts with paid APIs excluded.
- [ ] Commit `feat(deploy): package adaptive routing for Higress on Kubernetes`; push and verify.

**Completion contract:** Deployment artifacts consumed by real validators; complete local chain tested; no production deploy or registry publish unless separately authorized.

### Task 7: Benchmark, operator documentation and final audit

**Files:** benchmark scripts/results, Chinese usage/troubleshooting docs, final evidence matrix.

- [ ] Evaluate real scenario classification on public human-labelled datasets (Chinese first), with pinned data/license, independent train/dev/test splits, accuracy/macro-F1, OOS rejection, confusion/error analysis, and latency. Do not equate intent labels with best-model/difficulty ground truth.
- [ ] Download pinned lightweight models preferentially from ModelScope; run real local classification/embedding adapter validation, recording provenance and measured results without claiming general accuracy.
- [ ] Run deterministic synthetic workloads comparing static routing, load-aware routing, learning updates and shortcut signal avoidance. Report errors and p50/p95/p99 with sample counts, warmup, concurrency, code/config versions.
- [ ] Run full applicable regression/lint/type/Go/build/integration suites; list failures and genuine environmental gaps instead of silently skipping.
- [ ] Use a fresh reviewer per executing-plans final-review instructions; address important findings with regression tests and one coherent fix pass, then rerun affected suites.
- [ ] Audit every numbered user requirement against authoritative code/runtime/remote state. Commit and push documentation/results and each necessary fix.

**Completion contract:** Remote branch matches verified local state; every user requirement has scoped evidence; unresolved required runtime evidence leaves the goal active.
