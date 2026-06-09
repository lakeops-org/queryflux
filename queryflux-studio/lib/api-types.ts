/**
 * Types matching the QueryFlux Admin API (queryflux-persistence::query_history).
 * Regenerate with: npm run generate-api (requires proxy running on :9000)
 *
 * Persisted cluster config (`/admin/config/clusters/*`) uses **camelCase** JSON
 * (`engineKey`, …) per `#[serde(rename_all = "camelCase")]` in Rust. Live cluster
 * snapshots (`/admin/clusters`) still use snake_case field names.
 */

export interface ClusterStateDto {
  group_name: string;
  cluster_name: string;
  engine_type: string;
  /** HTTP endpoint of the cluster. Null for engines without a network endpoint (e.g. DuckDB). */
  endpoint: string | null;
  running_queries: number;
  queued_queries: number;
  max_running_queries: number;
  /** Whether the most recent health check (every 30s) passed. */
  is_healthy: boolean;
  /** Whether this cluster is administratively enabled. */
  enabled: boolean;
}

/**
 * Live snapshot optionally merged with a row synthesized from Postgres
 * (`GET /admin/config/clusters`) when the cluster is missing from `/admin/clusters`.
 */
export type ClusterDisplayRow = ClusterStateDto & {
  configPending?: boolean;
  /**
   * When true with `configPending`: cluster exists in `cluster_configs` but is not a member
   * of any **enabled** cluster group — the proxy never loads it (restart does not help).
   */
  notInAnyGroup?: boolean;
  /**
   * Per-cluster cap from Postgres (`null` = inherit group limit). Omitted when unknown
   * (e.g. config list failed).
   */
  persisted_max_running_queries?: number | null;
};

export interface DashboardStats {
  queries_last_hour: number;
  error_rate_last_hour: number;       // fraction 0.0–1.0
  avg_duration_ms_last_hour: number;
  translation_rate_last_hour: number; // fraction 0.0–1.0
}

/** `GET /admin/frontends` — effective protocol listeners from startup config (snake_case JSON). */
export interface ProtocolFrontendDto {
  id: string;
  label: string;
  short_description: string;
  enabled: boolean;
  port: number | null;
}

export interface FrontendsStatusDto {
  external_address: string | null;
  admin_api_port: number;
  protocols: ProtocolFrontendDto[];
}

export interface GuardAction {
  guard: string;
  action: "allow" | "warn" | "deny";
  reason: string | null;
  code: string | null;
  /** Free-form key/value metadata returned by the guard (e.g. matched rule, estimated rows). */
  metadata?: Record<string, string> | null;
}

export interface QueryHistoryRecord {
  id: number;
  proxy_query_id: string;
  /** The query ID assigned by the backend engine (e.g. Trino's query ID). */
  backend_query_id: string | null;
  cluster_group: string;
  cluster_name: string;
  engine_type: string;
  frontend_protocol: string;
  was_translated: boolean;
  username: string | null;
  sql_preview: string;
  /** The SQL after dialect translation. Only present when was_translated is true. */
  translated_sql: string | null;
  status: string;
  source_dialect: string;
  target_dialect: string;
  routing_trace: RoutingTrace | null;
  queue_duration_ms: number;
  execution_duration_ms: number;
  rows_returned: number | null;
  error_message: string | null;
  created_at: string; // ISO 8601
  // Engine-reported execution stats (null for engines that don't expose them)
  cpu_time_ms: number | null;
  processed_rows: number | null;
  processed_bytes: number | null;
  physical_input_bytes: number | null;
  peak_memory_bytes: number | null;
  spilled_bytes: number | null;
  total_splits: number | null;
  /** Tags attached at submit time. Key-only tags have null value. Null for older rows. */
  query_tags: Record<string, string | null> | null;
  /** Agent identity — present when query originated from an AI agent. */
  agent_id: string | null;
  conversation_id: string | null;
  step_index: number | null;
  query_intent: string | null;
  /** Guard actions collected during the query's guard chain evaluation. */
  guard_actions: GuardAction[] | null;
  was_guard_blocked: boolean;
}

export interface AgentSummary {
  agent_id: string;
  query_count: number;
  conversation_count: number;
  first_seen: string;
  last_seen: string;
}

export interface ConversationSummary {
  conversation_id: string;
  agent_id: string | null;
  step_count: number;
  first_seen: string;
  last_seen: string;
  has_blocked: boolean;
}

export interface AgentListParams {
  limit?: number;
  offset?: number;
}

export interface ConversationListParams {
  agent_id?: string;
  limit?: number;
  offset?: number;
}

export interface RoutingTrace {
  decisions: RoutingDecision[];
  final_group: string;
  used_fallback: boolean;
}

export interface RoutingDecision {
  router_type: string;
  matched: boolean;
  result?: string | null;
}

/** Per-cluster-group aggregated stats from `GET /admin/group-stats`. */
export interface GroupStatRow {
  cluster_group: string;
  engine_type: string;
  total_queries: number;
  successful_queries: number;
  failed_queries: number;
  cancelled_queries: number;
  avg_execution_ms: number;
  min_execution_ms: number;
  max_execution_ms: number;
  avg_queue_ms: number;
  translated_queries: number;
  total_rows_returned: number;
}

/** Per-engine aggregated stats from `GET /admin/engine-stats`. */
export interface EngineStatRow {
  engine_type: string;
  total_queries: number;
  successful_queries: number;
  failed_queries: number;
  cancelled_queries: number;
  /** Average execution time in milliseconds. */
  avg_execution_ms: number;
  /** Minimum execution time in milliseconds. */
  min_execution_ms: number;
  /** Maximum execution time in milliseconds. */
  max_execution_ms: number;
  /** Average time spent queued before execution, in milliseconds. */
  avg_queue_ms: number;
  translated_queries: number;
  total_rows_returned: number;
}

export interface ClusterUpdateRequest {
  enabled?: boolean;
  max_running_queries?: number;
}

// ---------------------------------------------------------------------------
// Persisted cluster / group config (requires Postgres persistence)
// ---------------------------------------------------------------------------

/** Matches QueryFlux Admin API JSON (`#[serde(rename_all = "camelCase")]` on the Rust structs). */
export interface ClusterConfigRecord {
  /** Stable surrogate key; group members reference these ids in Postgres. */
  id: number;
  name: string;
  engineKey: string;
  enabled: boolean;
  /** Per-cluster cap; absent or `null` means inherit from the cluster group. */
  maxRunningQueries?: number | null;
  /** All engine-specific connection details (endpoint, auth, TLS, region, …). */
  config: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface UpsertClusterConfig {
  engineKey: string;
  enabled?: boolean;
  /** Omit or `null` to use the cluster group's `max_running_queries`. */
  maxRunningQueries?: number | null;
  /** Engine-specific connection details. Schema depends on engineKey. */
  config: Record<string, unknown>;
}

/** Body for PATCH `/admin/config/clusters/{name}` and `/admin/config/groups/{name}`. */
export interface RenameConfigRequest {
  newName: string;
}

/** Matches admin API JSON (`#[serde(rename_all = "camelCase")]`). */
export interface ClusterGroupConfigRecord {
  /** Stable surrogate key (Postgres); used for routing FKs. */
  id: number;
  name: string;
  enabled: boolean;
  members: string[];
  maxRunningQueries: number;
  maxQueuedQueries: number | null;
  strategy: Record<string, unknown> | null;
  allowGroups: string[];
  allowUsers: string[];
  /** Ordered `user_scripts` ids (translation_fixup) applied after sqlglot for this group. */
  translationScriptIds: number[];
  /** Default tags merged into every query in this group. `null` values are key-only tags. */
  defaultTags: Record<string, string | null>;
  createdAt: string;
  updatedAt: string;
}

/** Request body for PUT `/admin/config/groups/{name}` (camelCase JSON). */
export interface UpsertClusterGroupConfig {
  enabled?: boolean;
  members: string[];
  maxRunningQueries: number;
  maxQueuedQueries?: number | null;
  strategy?: Record<string, unknown> | null;
  allowGroups?: string[];
  allowUsers?: string[];
  translationScriptIds?: number[];
  defaultTags?: Record<string, string | null>;
}

/** Reusable Python snippet from `GET /admin/config/scripts`. */
export interface UserScriptRecord {
  id: number;
  name: string;
  description: string;
  /** `translation_fixup` | `routing` */
  kind: string;
  body: string;
  createdAt: string;
  updatedAt: string;
}

export interface UpsertUserScript {
  name: string;
  description?: string;
  kind: string;
  body: string;
}

export interface QueryListParams {
  search?: string;
  status?: string;
  cluster_group?: string;
  engine?: string;
  limit?: number;
  offset?: number;
}

// ---------------------------------------------------------------------------
// Security & Routing config (GET /admin/config/security, /admin/config/routing)
// Sanitized — no secrets.
// ---------------------------------------------------------------------------

export interface OidcConfigDto {
  issuer: string;
  jwks_uri: string;
  audience: string | null;
  groups_claim: string;
  roles_claim: string | null;
}

export interface LdapConfigDto {
  url: string;
  bind_dn: string;
  user_search_base: string;
  user_search_filter: string;
  user_dn_template: string | null;
  group_search_base: string | null;
  group_name_attribute: string;
}

export interface OpenFgaConfigDto {
  url: string;
  store_id: string;
  /** "api_key" | "client_credentials" | null */
  credentials_method: string | null;
}

export interface GroupAuthzDto {
  allow_groups: string[];
  allow_users: string[];
}

export interface SecurityConfigDto {
  /** "none" | "static" | "oidc" | "ldap" */
  auth_provider: string;
  auth_required: boolean;
  oidc: OidcConfigDto | null;
  ldap: LdapConfigDto | null;
  /** Count of users when provider = "static". Passwords are never exposed. */
  static_user_count: number | null;
  /** "none" | "openfga" */
  authorization_provider: string;
  openfga: OpenFgaConfigDto | null;
  /** Per-cluster-group allow-lists (used when authorization_provider = "none"). */
  group_authorization: Record<string, GroupAuthzDto>;
}

export interface RoutingConfigDto {
  /** Admin API JSON uses camelCase (`routingFallback`). */
  routingFallback?: string;
  /** Stable id of the fallback cluster group when known. */
  routingFallbackGroupId?: number | null;
  /** Legacy rows / older responses may use snake_case. */
  routing_fallback?: string;
  /** Raw router config objects (type-tagged). */
  routers: RouterConfigEntry[];
}

/** Sub-condition inside a `type: "compound"` router (camelCase from admin API). */
export interface CompoundConditionEntry {
  type: "protocol" | "header" | "user" | "clientTag" | "queryRegex";
  protocol?: string;
  headerName?: string;
  headerValue?: string;
  username?: string;
  tag?: string;
  regex?: string;
}

/** One rule in the new `tags` router: match ALL tags in the map → route to target_group. */
export interface TagRoutingRule {
  /** Tag key → value to match. A null value means key-only match (any value). */
  tags: Record<string, string | null>;
  target_group?: string;
  targetGroup?: string;
  targetGroupId?: number;
}

export interface RouterConfigEntry {
  type: string;
  // protocolBased — value may be group name (legacy) or numeric id
  trino_http?: string | number | null;
  postgres_wire?: string | number | null;
  mysql_wire?: string | number | null;
  clickhouse_http?: string | number | null;
  flight_sql?: string | number | null;
  // header
  header_name?: string;
  header_value_to_group?: Record<string, string | number>;
  headerValueToGroupId?: Record<string, number>;
  // userGroup
  user_to_group?: Record<string, string | number>;
  userToGroupId?: Record<string, number>;
  // queryRegex: elements have `regex`. Tags router: elements have `tags` (RouterConfig::Tags).
  rules?: Array<
    | {
        regex: string;
        target_group?: string;
        targetGroup?: string;
        targetGroupId?: number;
      }
    | TagRoutingRule
  >;
  tag_to_group?: Record<string, string | number>;
  tagToGroupId?: Record<string, number>;
  // pythonScript
  script?: string;
  script_file?: string | null;
  // compound (AND/OR)
  combine?: "all" | "any";
  conditions?: CompoundConditionEntry[];
  targetGroup?: string;
  target_group?: string;
  targetGroupId?: number;
  /** @deprecated Prefer `rules`; still sent by some clients and accepted by the API. */
  tag_rules?: TagRoutingRule[];
}

export interface UpsertSecurityConfig {
  auth_provider: string;
  auth_required: boolean;
  oidc?: {
    issuer: string;
    jwks_uri: string;
    audience?: string | null;
    groups_claim: string;
    roles_claim?: string | null;
  } | null;
  ldap?: {
    url: string;
    bind_dn?: string;
    bind_password?: string | null;
    user_search_base: string;
    user_search_filter?: string;
    user_dn_template?: string | null;
    group_search_base?: string | null;
    group_name_attribute?: string;
  } | null;
  static_users?: Record<string, { password: string; groups?: string[]; roles?: string[] }> | null;
  authorization_provider: string;
  openfga?: {
    url: string;
    store_id: string;
    credentials?: {
      method: string;
      api_key?: string;
      client_id?: string;
      client_secret?: string;
      token_endpoint?: string;
    } | null;
  } | null;
}

export interface UpsertRoutingConfig {
  routingFallback?: string;
  routingFallbackGroupId?: number | null;
  routers: RouterConfigEntry[];
}

// ---------------------------------------------------------------------------
// Guardrails config (GET /admin/config/guardrails, PUT /admin/config/guardrails)
// ---------------------------------------------------------------------------

export type GuardKind = "built_in" | "http_webhook" | "python_script";

export interface GuardSpecDto {
  kind: GuardKind;
  /** Built-in guard name: "read_only" | "row_limit" | "require_predicate" */
  name?: string;
  /** row_limit: max rows before blocking */
  max_rows?: number | null;
  /** require_predicate: glob patterns for table names this guard applies to */
  applies_to?: string[] | null;
  /** http_webhook: endpoint URL */
  url?: string;
  /** http_webhook / python_script: timeout in ms */
  timeout_ms?: number | null;
  /** python_script: numeric id of a guard script (kind="guard") managed on the Guardrails page */
  script_id?: number;
  /** http_webhook: "deny" (default) | "allow" when the webhook is unreachable */
  fail_behavior?: "deny" | "allow" | null;
  /** http_webhook: extra request headers sent with every call */
  headers?: Record<string, string> | null;
}

export interface GuardrailsConfig {
  /** Guards that run for every query regardless of cluster group. */
  global: GuardSpecDto[];
  /** Per-group additional guards. Group name → guard list. */
  groups: Record<string, GuardSpecDto[]>;
}
