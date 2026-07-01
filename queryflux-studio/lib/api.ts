import type {
  AgentListParams,
  AgentSummary,
  ClusterStateDto,
  ConversationListParams,
  ConversationSummary,
  DashboardStats,
  EngineStatRow,
  FrontendsStatusDto,
  GroupStatRow,
  GuardrailsConfig,
  QueryHistoryRecord,
  QueryListParams,
} from "./api-types";
import {
  SESSION_COOKIE_NAME,
  basicAuthFromCookieValue,
} from "./admin-session-codec";
import { normalizeClusterConfigRecord } from "./cluster-config-helpers";
import { normalizeClusterGroupRecord } from "./group-config-helpers";

const ADMIN_DIRECT = process.env.ADMIN_API_URL ?? "http://localhost:9000";

/**
 * Low-level fetch to the admin API.
 * - Browser: same-origin `/api/admin-proxy` + `credentials: 'include'` (HttpOnly session).
 * - Server: prefer looping back through `/api/admin-proxy` with the incoming `Cookie` header so
 *   auth matches the browser path. Fallback: direct `ADMIN_API_URL` + Authorization from session.
 */
async function adminFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const p = path.startsWith("/") ? path : `/${path}`;

  if (typeof window !== "undefined") {
    return fetch(`/api/admin-proxy${p}`, {
      ...init,
      credentials: "include",
      cache: "no-store",
    });
  }

  try {
    const { cookies, headers } = await import("next/headers");
    const cookieStore = await cookies();
    const h = await headers();
    const host = h.get("x-forwarded-host") ?? h.get("host");
    const proto = h.get("x-forwarded-proto") ?? "http";
    const cookieStr = cookieStore
      .getAll()
      .map((c) => `${c.name}=${encodeURIComponent(c.value)}`)
      .join("; ");
    if (host) {
      const url = `${proto}://${host}/api/admin-proxy${p}`;
      const merged = new Headers(init.headers);
      if (cookieStr) merged.set("cookie", cookieStr);
      return fetch(url, { ...init, headers: merged, cache: "no-store" });
    }
  } catch {
    // fall through to direct admin
  }

  const hdrs = await serverDirectAuthHeaders();
  const merged = new Headers(init.headers);
  for (const [k, v] of Object.entries(hdrs)) merged.set(k, v);
  return fetch(`${ADMIN_DIRECT}${p}`, { ...init, headers: merged, cache: "no-store" });
}

/** When loopback proxy is unavailable, attach Authorization from the session cookie.
 *  Falls back to ADMIN_API_USERNAME / ADMIN_API_PASSWORD env vars (bootstrap credentials
 *  for docker-compose examples where no user session exists yet).
 */
async function serverDirectAuthHeaders(): Promise<Record<string, string>> {
  const headers: Record<string, string> = {};
  try {
    const { cookies } = await import("next/headers");
    const cookieStore = await cookies();
    const session = cookieStore.get(SESSION_COOKIE_NAME)?.value;
    if (session) {
      const auth = basicAuthFromCookieValue(session);
      if (auth) headers["authorization"] = auth;
      return headers;
    }
  } catch {
    // ignore
  }
  // Fallback: bootstrap credentials from env vars (no session yet).
  const envUser = process.env.ADMIN_API_USERNAME;
  const envPass = process.env.ADMIN_API_PASSWORD;
  if (envUser && envPass) {
    const { basicAuthorizationHeader } = await import("./admin-session-codec");
    headers["authorization"] = basicAuthorizationHeader(envUser, envPass);
  }
  return headers;
}

export class UnauthorizedError extends Error {
  constructor() {
    super("Unauthorized");
    this.name = "UnauthorizedError";
  }
}

function dispatchUnauthorized() {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event("qf:unauthorized"));
  }
}

async function apiFetch<T>(path: string): Promise<T> {
  const res = await adminFetch(path, { method: "GET" });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPatch<T>(path: string, body: unknown): Promise<T> {
  const res = await adminFetch(path, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API PATCH ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const res = await adminFetch(path, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API PUT ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await adminFetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API POST ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPutNoContent(path: string, body: unknown): Promise<void> {
  const res = await adminFetch(path, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API PUT ${path} → ${res.status}: ${await res.text()}`);
  }
}

async function apiDelete(path: string): Promise<void> {
  const res = await adminFetch(path, { method: "DELETE" });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok && res.status !== 204) {
    throw new Error(`Admin API DELETE ${path} → ${res.status}: ${await res.text()}`);
  }
}

export async function getClusters(): Promise<ClusterStateDto[]> {
  return apiFetch<ClusterStateDto[]>("/admin/clusters");
}

export async function getDashboardStats(): Promise<DashboardStats> {
  return apiFetch<DashboardStats>("/admin/stats");
}

export async function getFrontendsStatus(): Promise<FrontendsStatusDto> {
  return apiFetch<FrontendsStatusDto>("/admin/frontends");
}

export async function getQueries(params: QueryListParams = {}): Promise<QueryHistoryRecord[]> {
  const qs = new URLSearchParams();
  if (params.search) qs.set("search", params.search);
  if (params.status) qs.set("status", params.status);
  if (params.cluster_group) qs.set("cluster_group", params.cluster_group);
  if (params.engine) qs.set("engine", params.engine);
  if (params.limit != null) qs.set("limit", String(params.limit));
  if (params.offset != null) qs.set("offset", String(params.offset));
  const query = qs.toString() ? `?${qs}` : "";
  return apiFetch<QueryHistoryRecord[]>(`/admin/queries${query}`);
}

export async function getDistinctEngines(): Promise<string[]> {
  return apiFetch<string[]>("/admin/engines");
}

export async function getAgents(params: AgentListParams = {}): Promise<AgentSummary[]> {
  const qs = new URLSearchParams();
  if (params.limit != null) qs.set("limit", String(params.limit));
  if (params.offset != null) qs.set("offset", String(params.offset));
  const query = qs.toString() ? `?${qs}` : "";
  return apiFetch<AgentSummary[]>(`/admin/agents${query}`);
}

export async function getConversations(params: ConversationListParams = {}): Promise<ConversationSummary[]> {
  const qs = new URLSearchParams();
  if (params.agent_id) qs.set("agent_id", params.agent_id);
  if (params.limit != null) qs.set("limit", String(params.limit));
  if (params.offset != null) qs.set("offset", String(params.offset));
  const query = qs.toString() ? `?${qs}` : "";
  return apiFetch<ConversationSummary[]>(`/admin/conversations${query}`);
}

export async function getConversationDetail(conversationId: string): Promise<QueryHistoryRecord[]> {
  return apiFetch<QueryHistoryRecord[]>(`/admin/conversations/${encodeURIComponent(conversationId)}`);
}

export async function getGuardrailsConfig(): Promise<GuardrailsConfig> {
  return apiFetch<GuardrailsConfig>("/admin/config/guardrails");
}

export async function putGuardrailsConfig(config: GuardrailsConfig): Promise<void> {
  return apiPutNoContent("/admin/config/guardrails", config);
}

export async function getEngineStats(hours = 24): Promise<EngineStatRow[]> {
  return apiFetch<EngineStatRow[]>(`/admin/engine-stats?hours=${hours}`);
}

export async function getGroupStats(hours = 24): Promise<GroupStatRow[]> {
  return apiFetch<GroupStatRow[]>(`/admin/group-stats?hours=${hours}`);
}

// ---------------------------------------------------------------------------
// Persisted cluster config CRUD
// ---------------------------------------------------------------------------

export async function listClusterConfigs(): Promise<import("./api-types").ClusterConfigRecord[]> {
  const raw = await apiFetch<unknown[]>("/admin/config/clusters");
  return Array.isArray(raw) ? raw.map(normalizeClusterConfigRecord) : [];
}

export async function getClusterConfig(name: string): Promise<import("./api-types").ClusterConfigRecord> {
  const raw = await apiFetch<unknown>(
    `/admin/config/clusters/${encodeURIComponent(name)}`,
  );
  return normalizeClusterConfigRecord(raw);
}

export async function upsertClusterConfig(
  name: string,
  body: import("./api-types").UpsertClusterConfig,
): Promise<import("./api-types").ClusterConfigRecord> {
  const raw = await apiPut<unknown>(
    `/admin/config/clusters/${encodeURIComponent(name)}`,
    body,
  );
  return normalizeClusterConfigRecord(raw);
}

export async function renameClusterConfig(
  currentName: string,
  body: import("./api-types").RenameConfigRequest,
): Promise<import("./api-types").ClusterConfigRecord> {
  const raw = await apiPatch<unknown>(
    `/admin/config/clusters/${encodeURIComponent(currentName)}`,
    body,
  );
  return normalizeClusterConfigRecord(raw);
}

export async function deleteClusterConfig(name: string): Promise<void> {
  return apiDelete(`/admin/config/clusters/${encodeURIComponent(name)}`);
}

// ---------------------------------------------------------------------------
// Persisted cluster group config CRUD
// ---------------------------------------------------------------------------

export async function listGroupConfigs(): Promise<import("./api-types").ClusterGroupConfigRecord[]> {
  const raw = await apiFetch<unknown[]>("/admin/config/groups");
  return raw.map((row) => normalizeClusterGroupRecord(row));
}

export async function getGroupConfig(name: string): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiFetch<unknown>(`/admin/config/groups/${encodeURIComponent(name)}`);
  return normalizeClusterGroupRecord(raw);
}

export async function upsertGroupConfig(
  name: string,
  body: import("./api-types").UpsertClusterGroupConfig,
): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiPut<unknown>(
    `/admin/config/groups/${encodeURIComponent(name)}`,
    body,
  );
  return normalizeClusterGroupRecord(raw);
}

export async function renameGroupConfig(
  currentName: string,
  body: import("./api-types").RenameConfigRequest,
): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiPatch<unknown>(
    `/admin/config/groups/${encodeURIComponent(currentName)}`,
    body,
  );
  return normalizeClusterGroupRecord(raw);
}

export async function deleteGroupConfig(name: string): Promise<void> {
  return apiDelete(`/admin/config/groups/${encodeURIComponent(name)}`);
}

// ---------------------------------------------------------------------------
// Query result cache
// ---------------------------------------------------------------------------

export async function invalidateGroupCache(group: string): Promise<{ deleted: number }> {
  const res = await adminFetch(`/admin/cache/${encodeURIComponent(group)}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Cache invalidation failed: ${res.status}`);
  return res.json();
}

export async function invalidateAllCache(): Promise<{ deleted: number }> {
  const res = await adminFetch(`/admin/cache`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Cache invalidation failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// User script library (`user_scripts` table)
// ---------------------------------------------------------------------------

export async function listUserScripts(kind?: string): Promise<import("./api-types").UserScriptRecord[]> {
  const q = kind ? `?kind=${encodeURIComponent(kind)}` : "";
  return apiFetch<import("./api-types").UserScriptRecord[]>(`/admin/config/scripts${q}`);
}

export async function createUserScript(
  body: import("./api-types").UpsertUserScript,
): Promise<import("./api-types").UserScriptRecord> {
  return apiPost(`/admin/config/scripts`, body);
}

export async function updateUserScript(
  id: number,
  body: import("./api-types").UpsertUserScript,
): Promise<import("./api-types").UserScriptRecord> {
  return apiPut(`/admin/config/scripts/${id}`, body);
}

export async function deleteUserScript(id: number): Promise<void> {
  return apiDelete(`/admin/config/scripts/${id}`);
}

export async function updateCluster(
  group: string,
  cluster: string,
  update: import("./api-types").ClusterUpdateRequest,
): Promise<import("./api-types").ClusterStateDto> {
  return apiPatch<import("./api-types").ClusterStateDto>(
    `/admin/clusters/${encodeURIComponent(group)}/${encodeURIComponent(cluster)}`,
    update,
  );
}

// ---------------------------------------------------------------------------
// Security and routing config
// ---------------------------------------------------------------------------

export async function getSecurityConfig(): Promise<import("./api-types").SecurityConfigDto> {
  return apiFetch("/admin/config/security");
}

export async function getRoutingConfig(): Promise<import("./api-types").RoutingConfigDto> {
  return apiFetch("/admin/config/routing");
}

export async function putSecurityConfig(body: import("./api-types").UpsertSecurityConfig): Promise<void> {
  return apiPutNoContent("/admin/config/security", body);
}

export async function putRoutingConfig(body: import("./api-types").UpsertRoutingConfig): Promise<void> {
  return apiPutNoContent("/admin/config/routing", body);
}

// ---------------------------------------------------------------------------
// Test connection
// ---------------------------------------------------------------------------

export interface TestClusterConfigResponse {
  ok: boolean;
  message: string;
}

export async function testClusterConfig(
  engineKey: string,
  config: Record<string, unknown>,
): Promise<TestClusterConfigResponse> {
  return apiPost("/admin/config/clusters/test", { engine_key: engineKey, config });
}

// ---------------------------------------------------------------------------
// Auth management
// ---------------------------------------------------------------------------

export async function getAuthStatus(): Promise<{ db_override: boolean }> {
  return apiFetch("/admin/auth/status");
}

export async function changePassword(
  current_password: string,
  new_password: string,
): Promise<void> {
  await apiPost("/admin/auth/change-password", { current_password, new_password });
}

// Re-export types so pages can import from one place
export type {
  ClusterConfigRecord,
  ClusterGroupConfigRecord,
  ClusterStateDto,
  ClusterUpdateRequest,
  DashboardStats,
  EngineStatRow,
  FrontendsStatusDto,
  GroupStatRow,
  AgentListParams,
  AgentSummary,
  ConversationListParams,
  ConversationSummary,
  GuardAction,
  GuardSpecDto,
  GuardrailsConfig,
  ProtocolFrontendDto,
  GroupAuthzDto,
  LdapConfigDto,
  OidcConfigDto,
  OpenFgaConfigDto,
  QueryHistoryRecord,
  QueryListParams,
  RouterConfigEntry,
  RoutingConfigDto,
  RoutingTrace,
  RoutingDecision,
  SecurityConfigDto,
  UpsertClusterConfig,
  UpsertClusterGroupConfig,
  UpsertRoutingConfig,
  UpsertSecurityConfig,
  UpsertUserScript,
  UserScriptRecord,
} from "./api-types";
