# Route-Explain Endpoint — Detailed Implementation Plan

Phase 1 of [`plans/adaptive-routing-explainability-agent-access.md`](./adaptive-routing-explainability-agent-access.md), expanded to file/function-level detail.

**Status: implemented** on `feat/route-explain-endpoint` (backend, Studio UI, and docs). A few decisions changed from the original plan during implementation — each section below is annotated with what actually shipped and why. Not yet committed. Tracked by [lakeops-org/queryflux#176](https://github.com/lakeops-org/queryflux/issues/176) — link the PR to that issue when it's opened.

**Post-implementation review found and fixed three real bugs:**
1. `routing_trace.denied` was never set for authorization-stage denials (only router-stage ones), so Studio rendered a contradictory "Final group: success" trace footer alongside a "would be denied" banner. Fixed in `empty_route_explain_response`; regression test added (`route_explain_auth_denial_sets_trace_denied_too`).
2. Guard preview evaluated guards against **untranslated** SQL; real dispatch (`dispatch_query`) runs guards *after* translation, against the final SQL. Fixed — `build_route_explain_response` now calls `TranslationService::maybe_translate` before building `GuardContext`, same as production.
3. `RoutingTraceView`'s extraction introduced a rendering regression: a matched-but-empty-result decision (reachable via a buggy `pythonScript` router returning `""`) rendered as "no match" instead of nothing extra. Fixed the ternary.

**Known gap, deliberately not fixed (not planned):** `would_queue` doesn't check the group's `maxQueuedQueries` limit or current queue depth (that check lives behind a persistence call — `count_active_queued_before` — that the preview doesn't have wired up). A group at running capacity with an already-full queue reports `would_queue: true` when real dispatch would reject with `QueueFull`.

On review, this was deliberately deprioritized rather than fixed: `routing_trace`/`guard_actions` are deterministic and config-driven (the same answer regardless of when you ask), which is the actual reason to reach for this endpoint — testing an `allowGroups` rule, debugging a routing decision, catching a guardrail before a client hits it. `capacity`/`would_queue` is fundamentally different — live runtime state that can change before the caller acts on it no matter how precisely it's computed. Spending effort making a field that's inherently stale-on-arrival slightly less wrong wasn't worth it; instead the docstrings, TS types, and Studio UI now label `capacity` explicitly as a best-effort, moment-in-time bonus signal, distinct from the config-driven verdicts that are this endpoint's actual purpose. This sub-item is tracked and closed as not planned in the [#176](https://github.com/lakeops-org/queryflux/issues/176) comment thread — the issue itself stays open as the feature's tracking issue.

**Structural gap fixed: guard-running and authorization-check logic no longer duplicated across dispatch.rs and admin.rs.** `build_route_explain_response` originally hand-copied dispatch's guard-chain loop and authorization check — the same duplication that caused bugs #2 and (indirectly) #1 above. Rather than leave that standing risk in place, this was consolidated into two shared helpers used by *every* call site, not just this endpoint's:

- `queryflux_guardrails::run_guard_chains` (new, in `queryflux-guardrails/src/chain.rs`) — runs a set of chains (global, then per-group) for a layer, stopping at the first deny. Now the single definition of "what does it mean to guard-check a query," used by `dispatch_query`, `run_plan_guards` (internally — its external signature is unchanged), `execute_to_sink_inner`, and `build_route_explain_response`.
- `crate::routing_resolve::check_group_authorized` (new, alongside `resolve_routed_group`) — the unconditional per-group authorization check, previously copy-pasted with an identical message format in `dispatch_query`, `execute_to_sink`, and now `admin.rs`.

This touches the hot dispatch path (`dispatch.rs`), so it was done as a **separate branch/PR** (`fix/consolidate-guard-authz-pipeline`, in a sibling git worktree at `../queryflux-guard-consolidation` based on `main`) rather than bundled into this one — a mistake in a shared helper used by every query has much higher blast radius than a mistake in an admin preview endpoint. Both branches were updated in lockstep (identical helper additions) so `feat/route-explain-endpoint`'s `admin.rs` already consumes the same shared functions; whichever branch merges first, the other picks them up cleanly. Full workspace test suite (106+ frontend tests, 73 guardrails tests) passes identically before and after in both branches — the refactor is behavior-preserving by construction (same loop body, same order, same break-on-deny semantics), not a rewrite.

Deliberately *not* touched: the deeper asymmetry where `run_plan_guards`'s speculative pre-flight check (before cluster selection, using `EngineType::Cache` as a "no engine yet" marker) doesn't write an audit record on deny, while the authoritative check in `execute_to_sink_inner` does. That's a genuine pre-existing gap in dispatch.rs's cache pre-check path, unrelated to route-explain — noted here so it isn't lost, but out of scope for this consolidation.

**Known gap, not yet resolved: heterogeneous engine types within a resolved group.** Confirmed mixed-engine cluster groups are a real, supported configuration. `build_route_explain_response` picks `members.first()`'s engine type to stand in for the whole group when translating SQL and guard-checking — but that "first" member is whatever order `all_cluster_states()` happens to return, not necessarily the member the group's load-balancing strategy would actually pick for a given request. For a homogeneous group this doesn't matter; for a mixed one, the guard verdict and shown translated SQL can be wrong for whichever member dispatch actually selects. Decided direction: check every *distinct* engine type present in the group (usually 1, occasionally more) — report a single verdict when they agree (the common case), and surface an explicit "this depends on which engine gets picked" breakdown when they don't, rather than silently guessing. Not yet implemented.

This surfaced a deeper, separate question during discussion: *should guardrails even run after translation, or before it?* Guard policies like `read_only`/`row_limit` are arguably about client intent, independent of which engine executes the query — checking pre-translation, client-dialect SQL would make guard verdicts engine-independent and sidestep the mixed-engine ambiguity entirely for guard checks. But that would change real production dispatch behavior (what gets blocked/allowed for live queries), not just this preview endpoint, and risks missing violations introduced by the `pythonScript` post-translation fixup hook. Deliberately **not** decided or implemented as a side effect of this work — tracked separately as [#177](https://github.com/lakeops-org/queryflux/issues/177) for whoever owns guardrail semantics to weigh in on.

## Goal

Given SQL text plus a hypothetical caller identity, answer "where would this query go, and what would happen to it" **without executing it or consuming any capacity**. Every question mark in the query lifecycle answered by this one call:

- Which router matched (or did the fallback fire)?
- Would authorization allow it, for this user/groups?
- Would any guardrail rewrite, warn on, or block it?
- Is the resolved cluster group actually able to take it right now, or would it queue?

## Non-goals (v1)

- Not a query cost estimator — that's Phase 2 (size-aware routing) and depends on new adapter plumbing (`EXPLAIN` support) this phase doesn't need.
- ~~Not a SQL dialect preview — guard evaluation uses the same un-translated SQL...~~ — **superseded by the post-implementation review fix**: guard evaluation now does call `TranslationService`, same as real dispatch. See the fix log at the top of this doc.
- Not query-plan `EXPLAIN` against a backend engine — nothing here touches an adapter or a live cluster; every signal comes from in-memory config/state already loaded into `LiveConfig`.

## Why this is cheap

Every piece this endpoint needs already exists and is already reachable from `AdminState`:

| Need | Existing source |
|---|---|
| Run the router chain and get a full trace | `RouterChain::route_with_trace` (`crates/queryflux-routing/src/chain.rs`) |
| Authorization-aware fallback resolution | `routing_resolve::resolve_routed_group` (`crates/queryflux-frontend/src/routing_resolve.rs`) — already `pub` |
| Non-fallback authorization enforcement | `LiveConfig.authorization: Arc<dyn AuthorizationChecker>` |
| Guard preview (read-only, no engine call) | `run_plan_guards` in `crates/queryflux-frontend/src/dispatch.rs` — currently private, needs `pub(crate)` |
| Tag merging | `queryflux_core::tags::merge_tags` — already `pub` |
| Live capacity per cluster | `ClusterGroupManager::all_cluster_states()` — already used by `clusters_handler` |

So this phase is almost entirely glue code in `admin.rs`: no new trait methods, no new crate, no new background task, no new config schema.

---

## Request / response contract

Following the existing admin-DTO convention (see `ClusterStateDto`): core types are converted to plain strings/JSON at the API boundary rather than adding `utoipa::ToSchema` to `queryflux-core` types.

```rust
/// POST /admin/route-explain
#[derive(Debug, Deserialize, ToSchema)]
pub struct RouteExplainRequest {
    pub sql: String,
    /// One of: trinoHttp, postgresWire, mysqlWire, clickhouseHttp, flightSql,
    /// snowflakeHttp, snowflakeSqlApi. Same wire values as elsewhere in the admin API.
    pub protocol: String,
    /// Simulated identity for authorization-aware routing/guard preview.
    /// Not verified against any AuthProvider — see "Simulated identity" below.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub database: Option<String>,
    /// Query tags, e.g. {"team": "eng", "batch": null} — same shape as QueryTags.
    #[serde(default)]
    pub tags: HashMap<String, Option<String>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RouteExplainResponse {
    /// Same shape Studio already renders for historical queries' routing_trace.
    pub routing_trace: RoutingTrace,
    /// Present only when a router or authorization check would deny the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied: Option<String>,
    /// Guard verdicts from the pre-dispatch guard pass (global + per-group chain),
    /// same shape as QueryRecord.guard_actions. Empty if no guards configured.
    pub guard_actions: Vec<queryflux_persistence::GuardAction>,
    /// True if any guard action was a deny (query would never reach dispatch).
    pub would_be_guard_blocked: bool,
    /// Live capacity snapshot of the resolved group's members at the moment of the call.
    /// Absent when routing itself was denied (no group was resolved).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<GroupCapacityDto>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GroupCapacityDto {
    pub group_name: String,
    pub members: Vec<ClusterStateDto>,  // reuse the existing DTO from clusters_handler
    /// True if no member is currently enabled+healthy+under max_running_queries —
    /// i.e. the query would sit in the queue (or hit sync retry-with-backoff) rather
    /// than dispatch immediately.
    pub would_queue: bool,
}
```

---

## Handler flow (as implemented)

This mirrors the real dispatch order exactly, so the answer this endpoint gives matches what would actually happen. Order matters — routing, then fallback resolution, then authorization, then guards, then capacity, same as `dispatch_query` / `execute_to_sink`.

**Shipped shape, differs from the original sketch in two ways** (both discovered while implementing, both improvements):

1. **The handler is a thin wrapper.** `route_explain_handler` only does the `LiveConfig` snapshot (one lock acquisition, matching `dispatch_query`'s discipline) and then delegates to a pure(-ish) helper, `build_route_explain_response(router_chain, authorization, guard_chain, group_guard_chains, group_default_tags, group_order, cluster_manager, &req) -> Result<RouteExplainResponse>`, which takes every dependency as an explicit parameter instead of an `AdminState`. This was done specifically so the routing/authz/guard/capacity logic could be unit-tested with lightweight fixtures (`SimpleClusterGroupManager` + `RoundRobinStrategy` + `ClusterState`, the same pattern `routing_resolve.rs`'s own tests use) instead of constructing a full `AdminState` (which has no existing test-construction helper anywhere in `admin.rs` — see "Testing" below).
2. **Guard preview does not reuse `run_plan_guards`.** See "Code changes needed" #1 — grounding the plan against the real `dispatch.rs` turned up three independent guard-running call sites, not one shared function, and the resolved group's *real* engine type was already sitting in the same `all_cluster_states()` call the capacity check needs. Using it is both simpler (no `dispatch.rs` changes at all) and more accurate than the placeholder one particular call site uses pre-cluster-selection.

Protocol parsing needed no custom code: `RouteExplainRequest.protocol` is typed as `FrontendProtocol` directly (it already derives `Deserialize` with the right camelCase rename), so an invalid protocol string is rejected by axum's `Json<T>` extractor with a 400 before the handler body even runs — the planned `parse_frontend_protocol` helper turned out to be unnecessary.

---

## Code changes made

### 1. `crates/queryflux-frontend/src/dispatch.rs` — **not touched**

The original plan called for making `run_plan_guards` `pub(crate)` and changing its return type. During implementation this turned out to be unnecessary: grounding the plan against the real `dispatch.rs` showed guard evaluation isn't centralized behind one function at all — there are **three** independent guard-running call sites (an inline block in `dispatch_query`, the `run_plan_guards` function used only by `execute_to_sink`'s cache-check branch, and another inline block in `execute_to_sink_inner`), and only one of them (`run_plan_guards`) uses the `EngineType::Cache` placeholder the original plan assumed was universal.

Since the capacity check already needs to call `all_cluster_states()` and filter to the resolved group, that same call also yields the group's **real** `engine_type` (from the first member) — more accurate for guard dialect parsing than any placeholder, and free. So the shipped guard preview is a small inline loop in `admin.rs` (same `for chain in [guard_chain, group_guard_chain].flatten() { chain.run(...) }` shape all three existing call sites share) using the real engine type, and `dispatch.rs` has zero changes.

### 2. `crates/queryflux-frontend/src/admin.rs`

- Added `RouteExplainRequest`, `RouteExplainResponse`, `GroupCapacityDto` (protocol field typed as `FrontendProtocol` directly with `#[schema(value_type = String)]` for the OpenAPI doc, rather than a raw `String` + manual parser).
- Added `route_explain_handler` (thin wrapper) and `build_route_explain_response` (the testable core — see "Handler flow" above).
- Added `empty_route_explain_response(trace, denied) -> RouteExplainResponse` helper — the deny path is hit from three places (router deny, fallback-resolve error, per-group authz check) and always builds the same shape.
- Registered `.route("/admin/route-explain", post(route_explain_handler))` in the `protected` router (same Basic-auth gate as every other admin endpoint) and added it to `#[openapi(paths(...))]` + `components(schemas(...))`.
- No `parse_frontend_protocol` helper needed — see "Handler flow" above.

### 3. `RouterChain` clonability — resolved as **(b)**, `Arc`-wrapped

`LiveConfig.router_chain` is now `Arc<RouterChain>`, matching `cluster_manager`/`authorization`. All five construction sites were updated (`main.rs` ×2, `state.rs` test fixture, `routing_resolve.rs` test fixture, `trino_http/handlers.rs` test fixture) plus four more found in `queryflux-e2e-tests/src/harness.rs` that the original plan didn't know about (grep for `LiveConfig {` across the workspace, not just `queryflux-frontend`, would have caught these up front). All six existing read call sites (`live.router_chain.route(...)` / `.route_with_trace(...)` in the five frontend protocol handlers) needed zero changes — method calls resolve through `Arc`'s `Deref` transparently. `cargo check --workspace` was the way this got verified, not manual inspection.

### 4. Studio (`queryflux-studio`) — done, as its own nav page rather than embedded

Shipped as a dedicated **Route Explain** page (`app/route-explain/`) in the left nav, not embedded in the Clusters page — it didn't fit naturally as a sub-panel there and a dedicated page keeps the (fairly busy) Clusters page uncluttered. `RoutingTraceView` was extracted out of `query-detail.tsx` into `components/routing-trace-view.tsx` so the historical-query view and the dry-run preview render identically; it also gained deny-message rendering (`RoutingDecision.deny_message`, `RoutingTrace.denied`) which the Rust side already produced but the TS types and renderer didn't expose yet — a small pre-existing gap, fixed as part of this since the explain panel's "denied by router" case needed it to be useful at all.

### 5. CLI (`queryflux-cli`) — deferred

Not built in this phase. Low cost if wanted later, but nothing in Phase 1's scope depended on it and the Studio page covers the interactive use case.

---

## Correctness pitfalls to design around

1. **Never call `ClusterGroupManager::acquire_cluster`.** It's not a query — it's `Pick a cluster and increment its running count`. Calling it from an explain endpoint would silently consume real capacity that's never released (there's no matching query to trigger `release_cluster`), inflating `running_queries` until the next restart or a manual fix. `all_cluster_states()` is the only capacity read this endpoint should ever touch.
2. **Guard evaluation uses the resolved group's real engine type, not a placeholder — a deliberate improvement over the original plan.** Some of production's own pre-dispatch guard-evaluation call sites use `EngineType::Cache` as a placeholder because they run *before* cluster selection. The explain endpoint doesn't have that constraint — it already resolves a group and reads its members' live state for the capacity check, so the same data gives it the real `engine_type` for guard dialect parsing too. SQL is now translated before guard evaluation (fixed post-review — see top of doc), using that same real engine type as the translation target. One remaining documented limitation: a group with heterogeneous engine types across members only sees the first member's dialect for both translation and guard parsing.
3. **Simulated identity is a documented capability, not a bug.** `user`/`groups` in the request body are never checked against a real `AuthProvider` — this endpoint answers "if a query came in as user X with groups Y, what would happen," which is exactly what makes it useful for testing authorization rules. Because the endpoint sits behind the same Basic-auth admin gate as every other `/admin/*` route, this means **admin credential holders can probe authorization outcomes for any simulated user**. That's the intended use (testing `allowGroups`/`allowUsers` rules before rolling them out) — call it out explicitly in the endpoint's doc comment and in Studio's UI copy so it isn't mistaken for real per-user authentication.
4. **Timeouts.** All current router implementations are fast except `pythonScript` (arbitrary user code) — no new timeout is needed for this phase since production doesn't have one either, but if a Python router has an unbounded loop, this endpoint inherits that risk exactly as query dispatch already does. Not a new problem introduced here, just worth knowing it isn't solved by this phase.

---

## Testing — as implemented

Five `#[tokio::test]`s in `admin.rs`'s existing test module, calling `build_route_explain_response` directly (not the HTTP handler — there's no existing pattern anywhere in `admin.rs` for constructing a full `AdminState` in tests, since every other test in the file exercises pure helpers or async functions with narrow dependencies, not full handlers; matching that convention is what made extracting `build_route_explain_response` the right call, not just a testability nicety):

- `route_explain_denied_by_router_skips_capacity_and_guards` — a `queryRegex` deny rule → `denied` is set, `capacity`/`guard_actions` are empty.
- `route_explain_fallback_reroutes_to_authorized_group` — empty router list (fallback fires) + a `SimpleAuthorizationPolicy` that excludes the fallback group but allows another → resolves to the authorized group, `used_fallback: true`.
- `route_explain_read_only_guard_blocks_write` — `ReadOnlyGuard` on the group, `INSERT ...` → `would_be_guard_blocked: true`, guard action has `code: "READ_ONLY_VIOLATION"`.
- `route_explain_would_queue_true_when_group_is_full` — single member at `running == max` → `would_queue: true`.
- `route_explain_would_queue_false_with_one_healthy_member_under_capacity` — one disabled + one healthy-under-capacity member → `would_queue: false`, both still listed.

Fixtures reuse the exact pattern `routing_resolve.rs`'s own tests already use (`SimpleClusterGroupManager` + `RoundRobinStrategy` + `ClusterState`, `AllowAllAuthorization`/`SimpleAuthorizationPolicy`) — no new test infrastructure needed. `ClusterState::set_running_queries`/`set_healthy`/`set_enabled` (all pre-existing) made the capacity fixtures straightforward.

Not done: an integration test asserting the explain endpoint's answer matches a *real* dispatched query's outcome. Worth adding once `queryflux-e2e-tests` has a scenario this could hook into — flagged as a gap, not silently dropped.

---

## Rollout

No config schema changes, no migration, no new background task — this is purely additive (`admin.rs` handler is fine to release without a feature flag). Restarting or hot-reloading is unaffected. Safe to ship in a normal release without a phased rollout.

---

## Open questions

1. ~~Lock-holding option~~ — **resolved**: `Arc<RouterChain>` (option b), see "Code changes made" #3.
2. **Should `/admin/route-explain` require the caller to *already* be authorized for the resolved group** (i.e., should this itself be gated by something beyond admin Basic auth)? Shipped as: no — the whole point is letting an admin check routing for *other* users. Still open: whether calls should be audit-logged (reusing `QueryRecord`-adjacent patterns) — not done, since this endpoint deliberately persists nothing (see "does this need a migration" below).
3. ~~Does Studio's trace component assume a `QueryRecord`?~~ — **resolved**: it didn't; extraction to `components/routing-trace-view.tsx` needed no `QueryRecord`-shaped input, only a `RoutingTrace`.
4. **No integration test** verifying explain output matches real dispatch output — see "Testing" above. Would need a `queryflux-e2e-tests` scenario; not blocking for v1 since the same code paths (`route_with_trace`, `resolve_routed_group`, `GuardChain::run`) are already covered by their own unit tests elsewhere.
