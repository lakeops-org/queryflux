/**
 * Maps persisted cluster `config` JSON (admin API / Postgres) ↔ flat form state
 * used by {@link EngineClusterConfig}, plus validation helpers shared with add-cluster.
 *
 * Engine-specific flat-form validation lives in `lib/studio-engines/validate-flat.ts` (per-engine modules).
 */

import type { ClusterConfigRecord, ClusterVariant, UpsertClusterConfig } from "@/lib/api-types";

export { validateEngineSpecific } from "@/lib/studio-engines/validate-flat";

/** Keys we round-trip through Studio forms; other JSON keys are preserved on save. */
export const MANAGED_CONFIG_JSON_KEYS = new Set([
  "endpoint",
  "databasePath",
  "driver",
  "uri",
  "username",
  "password",
  "dbKwargs",
  "flightSqlClusterDialect",
  /** @deprecated Prefer flightSqlClusterDialect; stripped on save from Studio. */
  "flightSqlEngine",
  "authType",
  "authUsername",
  "authPassword",
  "authToken",
  "tlsInsecureSkipVerify",
  "region",
  "s3OutputLocation",
  "workgroup",
  "catalog",
  "account",
  "warehouse",
  "role",
  "schema",
  "poolSize",
  "maxResultBufferBytes",
]);

function jsonScalarToString(v: unknown): string {
  if (v === undefined || v === null) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return "";
}

function jsonObjectToString(v: unknown): string {
  if (!v || typeof v !== "object" || Array.isArray(v)) return "";
  try {
    return JSON.stringify(v);
  } catch {
    return "";
  }
}

function parseJsonObjectString(s: string | undefined): Record<string, string> | undefined {
  if (s === undefined) return undefined;
  const t = s.trim();
  if (!t) return undefined;
  try {
    const parsed = JSON.parse(t) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === "string") out[k] = v;
    }
    return out;
  } catch {
    return undefined;
  }
}

/** Remove Studio UI-only keys from dbKwargs before persisting to the API (ADBC drivers must not see them). */
function stripStudioDbKwargsMeta(
  o: Record<string, string> | undefined,
): Record<string, string> | undefined {
  if (!o) return undefined;
  const next: Record<string, string> = {};
  for (const [k, v] of Object.entries(o)) {
    if (k.startsWith("__qf_")) continue;
    next[k] = v;
  }
  return Object.keys(next).length ? next : undefined;
}

/** PostgreSQL ADBC uses userinfo in the URI; separate username/password fields are not used. */
export function isAdbcPostgresqlDriver(flat: Record<string, string>): boolean {
  return flat.driver?.trim().toLowerCase() === "postgresql";
}

/** Positive integer only; rejects floats, NaN, and non-integer `number` values. */
export function parsePositiveIntString(s: string | undefined): number | undefined {
  if (s === undefined) return undefined;
  const t = s.trim();
  if (t === "") return undefined;
  const n = Number(t);
  if (!Number.isInteger(n) || n < 1) return undefined;
  return n;
}

/** DB / API `config` object → flat keys expected by cluster-config components. */
export function persistedClusterConfigToFlat(
  config: Record<string, unknown>,
  descriptor?: { configFields: { key: string }[] },
): Record<string, string> {
  const flat: Record<string, string> = {};

  flat.endpoint = jsonScalarToString(config.endpoint);
  flat.databasePath = jsonScalarToString(config.databasePath);
  flat.driver = jsonScalarToString(config.driver);
  flat.uri = jsonScalarToString(config.uri);
  flat.username = jsonScalarToString(config.username);
  flat.password = jsonScalarToString(config.password);
  flat.dbKwargs = jsonObjectToString(config.dbKwargs);
  flat.flightSqlClusterDialect =
    jsonScalarToString(config.flightSqlClusterDialect) ||
    jsonScalarToString(config.flightSqlEngine);
  flat.region = jsonScalarToString(config.region);
  flat.s3OutputLocation = jsonScalarToString(config.s3OutputLocation);
  flat.workgroup = jsonScalarToString(config.workgroup);
  flat.catalog = jsonScalarToString(config.catalog);
  flat.account = jsonScalarToString(config.account);
  flat.warehouse = jsonScalarToString(config.warehouse);
  flat.role = jsonScalarToString(config.role);
  flat.schema = jsonScalarToString(config.schema);

  const authType = jsonScalarToString(config.authType);
  if (authType) flat["auth.type"] = authType;
  flat["auth.username"] = jsonScalarToString(config.authUsername);
  flat["auth.password"] = jsonScalarToString(config.authPassword);
  flat["auth.token"] = jsonScalarToString(config.authToken);

  flat["tls.insecureSkipVerify"] =
    config.tlsInsecureSkipVerify === true ? "true" : "false";

  flat.poolSize = jsonScalarToString(config.poolSize);
  flat.maxResultBufferBytes = jsonScalarToString(config.maxResultBufferBytes);

  flat.healthCheckQuery = jsonScalarToString(config.healthCheckQuery);
  flat.reconcileQuery = jsonScalarToString(config.reconcileQuery);

  if (descriptor) {
    for (const f of descriptor.configFields) {
      if (flat[f.key] === undefined) flat[f.key] = "";
    }
  }

  return flat;
}

/** Flat form → persisted `config` JSON (same shape as add-cluster save). */
export function flatToPersistedConfig(flat: Record<string, string>): Record<string, unknown> {
  const cfg: Record<string, unknown> = {};
  if (flat.endpoint) cfg.endpoint = flat.endpoint;
  if (flat.databasePath !== undefined && flat.databasePath !== "") {
    cfg.databasePath = flat.databasePath || null;
  }
  if (flat.driver?.trim()) cfg.driver = flat.driver.trim();
  if (flat.uri?.trim()) cfg.uri = flat.uri.trim();
  if (!isAdbcPostgresqlDriver(flat)) {
    if (flat.username) cfg.username = flat.username;
    if (flat.password) cfg.password = flat.password;
  }
  const dbKwargs = stripStudioDbKwargsMeta(parseJsonObjectString(flat.dbKwargs));
  if (dbKwargs !== undefined) cfg.dbKwargs = dbKwargs;
  if (flat.flightSqlClusterDialect?.trim()) {
    cfg.flightSqlClusterDialect = flat.flightSqlClusterDialect.trim();
  }
  if (flat["auth.type"]) cfg.authType = flat["auth.type"] || null;
  if (flat["auth.username"]) cfg.authUsername = flat["auth.username"];
  if (flat["auth.password"]) cfg.authPassword = flat["auth.password"];
  if (flat["auth.token"]) cfg.authToken = flat["auth.token"];
  if (flat["tls.insecureSkipVerify"] === "true") cfg.tlsInsecureSkipVerify = true;
  else if (flat["tls.insecureSkipVerify"] === "false") cfg.tlsInsecureSkipVerify = false;
  if (flat.region?.trim()) cfg.region = flat.region.trim();
  if (flat.s3OutputLocation?.trim()) cfg.s3OutputLocation = flat.s3OutputLocation.trim();
  if (flat.workgroup?.trim()) cfg.workgroup = flat.workgroup.trim();
  if (flat.catalog?.trim()) cfg.catalog = flat.catalog.trim();
  if (flat.account?.trim()) cfg.account = flat.account.trim();
  if (flat.warehouse?.trim()) cfg.warehouse = flat.warehouse.trim();
  if (flat.role?.trim()) cfg.role = flat.role.trim();
  if (flat.schema?.trim()) cfg.schema = flat.schema.trim();
  const poolN = parsePositiveIntString(flat.poolSize);
  if (poolN !== undefined) cfg.poolSize = poolN;
  const bufN = parsePositiveIntString(flat.maxResultBufferBytes);
  if (bufN !== undefined) cfg.maxResultBufferBytes = bufN;
  return cfg;
}

/**
 * Apply form state onto an existing persisted `config` object (edit path).
 * Starts from `prev` so unknown JSON keys are kept; clears managed keys when the flat field is empty.
 */
export function mergeClusterConfigFromFlat(
  prev: Record<string, unknown>,
  flat: Record<string, string>,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...prev };

  const setOrDel = (key: string, value: string | undefined, jsonKey: string) => {
    if (value === undefined) return;
    const t = value.trim();
    if (t) out[jsonKey] = t;
    else delete out[jsonKey];
  };

  setOrDel("endpoint", flat.endpoint, "endpoint");
  if (flat.databasePath !== undefined) {
    const t = flat.databasePath.trim();
    if (t) out.databasePath = t;
    else delete out.databasePath;
  }
  setOrDel("driver", flat.driver, "driver");
  setOrDel("uri", flat.uri, "uri");
  if (!isAdbcPostgresqlDriver(flat)) {
    setOrDel("username", flat.username, "username");
    if (flat.password !== undefined && flat.password !== "") {
      out.password = flat.password;
    }
  } else {
    delete out.username;
    delete out.password;
  }
  if (flat.dbKwargs !== undefined) {
    const t = flat.dbKwargs.trim();
    if (t === "") {
      delete out.dbKwargs;
    } else {
      const parsed = stripStudioDbKwargsMeta(parseJsonObjectString(flat.dbKwargs));
      if (parsed !== undefined) out.dbKwargs = parsed;
      else delete out.dbKwargs;
    }
  }
  setOrDel(
    "flightSqlClusterDialect",
    flat.flightSqlClusterDialect,
    "flightSqlClusterDialect",
  );
  delete out.flightSqlEngine;
  if (flat["auth.type"] !== undefined) {
    const t = flat["auth.type"].trim();
    if (t) out.authType = t;
    else delete out.authType;
  }
  setOrDel("auth.username", flat["auth.username"], "authUsername");
  if (flat["auth.password"] !== undefined && flat["auth.password"] !== "") {
    out.authPassword = flat["auth.password"];
  }
  if (flat["auth.token"] !== undefined && flat["auth.token"] !== "") {
    out.authToken = flat["auth.token"];
  }
  if (flat["tls.insecureSkipVerify"] === "true") out.tlsInsecureSkipVerify = true;
  else if (flat["tls.insecureSkipVerify"] === "false") delete out.tlsInsecureSkipVerify;
  else if (flat["tls.insecureSkipVerify"] === "") delete out.tlsInsecureSkipVerify;

  setOrDel("region", flat.region, "region");
  setOrDel("s3OutputLocation", flat.s3OutputLocation, "s3OutputLocation");
  setOrDel("workgroup", flat.workgroup, "workgroup");
  setOrDel("catalog", flat.catalog, "catalog");
  setOrDel("account", flat.account, "account");
  setOrDel("warehouse", flat.warehouse, "warehouse");
  setOrDel("role", flat.role, "role");
  setOrDel("schema", flat.schema, "schema");

  if (flat.poolSize !== undefined) {
    const t = flat.poolSize.trim();
    if (t) {
      const n = parsePositiveIntString(flat.poolSize);
      if (n !== undefined) out.poolSize = n;
      else delete out.poolSize;
    } else delete out.poolSize;
  }

  if (flat.maxResultBufferBytes !== undefined) {
    const t = flat.maxResultBufferBytes.trim();
    if (t) {
      const n = parsePositiveIntString(flat.maxResultBufferBytes);
      if (n !== undefined) out.maxResultBufferBytes = n;
      else delete out.maxResultBufferBytes;
    } else delete out.maxResultBufferBytes;
  }

  setOrDel("healthCheckQuery", flat.healthCheckQuery, "healthCheckQuery");
  setOrDel("reconcileQuery", flat.reconcileQuery, "reconcileQuery");

  return out;
}

export function buildClusterUpsertFromForm(
  record: ClusterConfigRecord,
  flat: Record<string, string>,
  opts: {
    enabled: boolean;
    maxRunningQueriesInput: string;
    variants?: ClusterVariant[];
  },
): UpsertClusterConfig {
  const prev = record.config as Record<string, unknown>;
  const config = mergeClusterConfigFromFlat(prev, flat);

  const body: UpsertClusterConfig = {
    engineKey: record.engineKey,
    enabled: opts.enabled,
    config,
  };
  const maxTrim = opts.maxRunningQueriesInput.trim();
  body.maxRunningQueries = maxTrim === "" ? null : Number.parseInt(maxTrim, 10);
  if (opts.variants !== undefined) body.variants = opts.variants;
  else if (record.variants) body.variants = record.variants;
  return body;
}

/** Shape expected by {@link validateClusterConfig} (nested `auth`, `tls`). */
export function buildValidateShape(flat: Record<string, string>): Record<string, unknown> {
  const o: Record<string, unknown> = {};
  if (flat.endpoint) o.endpoint = flat.endpoint;
  if (flat.databasePath !== undefined && flat.databasePath !== "") {
    o.databasePath = flat.databasePath;
  }
  if (flat.region) o.region = flat.region;
  if (flat.driver) o.driver = flat.driver;
  if (flat.uri) o.uri = flat.uri;
  if (!isAdbcPostgresqlDriver(flat)) {
    if (flat.username) o.username = flat.username;
    if (flat.password) o.password = flat.password;
  }
  const dbKwargs = stripStudioDbKwargsMeta(parseJsonObjectString(flat.dbKwargs));
  if (dbKwargs !== undefined) o.dbKwargs = dbKwargs;
  if (flat.flightSqlClusterDialect) {
    o.flightSqlClusterDialect = flat.flightSqlClusterDialect;
  }
  if (flat.s3OutputLocation) o.s3OutputLocation = flat.s3OutputLocation;
  if (flat.account) o.account = flat.account;
  if (flat.warehouse) o.warehouse = flat.warehouse;
  if (flat.role) o.role = flat.role;
  if (flat.schema) o.schema = flat.schema;
  const poolN = parsePositiveIntString(flat.poolSize);
  if (poolN !== undefined) o.poolSize = poolN;
  const bufN = parsePositiveIntString(flat.maxResultBufferBytes);
  if (bufN !== undefined) o.maxResultBufferBytes = bufN;
  const auth: Record<string, string> = {};
  if (flat["auth.type"]) auth.type = flat["auth.type"];
  if (flat["auth.username"]) auth.username = flat["auth.username"];
  if (flat["auth.password"]) auth.password = flat["auth.password"];
  if (flat["auth.token"]) auth.token = flat["auth.token"];
  if (Object.keys(auth).length > 0) o.auth = auth;
  if (flat["tls.insecureSkipVerify"] === "true") {
    o.tls = { insecureSkipVerify: true };
  }
  return o;
}

export function toUpsertBody(
  engineKey: string,
  flat: Record<string, string>,
  runtime: {
    enabled: boolean;
    maxRunningQueriesInput: string;
    variants?: ClusterVariant[];
    healthCheckQuery?: string;
    reconcileQuery?: string;
  },
): UpsertClusterConfig {
  const config = flatToPersistedConfig(flat);
  const hcq = runtime.healthCheckQuery?.trim();
  if (hcq) config.healthCheckQuery = hcq;
  const rq = runtime.reconcileQuery?.trim();
  if (rq) config.reconcileQuery = rq;
  const body: UpsertClusterConfig = { engineKey, enabled: runtime.enabled, config };
  const maxTrim = runtime.maxRunningQueriesInput.trim();
  if (maxTrim !== "") {
    body.maxRunningQueries = Number.parseInt(maxTrim, 10);
  }
  if (runtime.variants !== undefined) {
    body.variants = runtime.variants;
  }
  return body;
}
