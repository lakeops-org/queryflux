import type { ClusterGroupConfigRecord, UpsertClusterGroupConfig } from "@/lib/api-types";

/** Parse API JSON whether the server used camelCase or legacy snake_case. */
export function normalizeClusterGroupRecord(raw: unknown): ClusterGroupConfigRecord {
  const r = raw as Record<string, unknown>;
  const num = (camel: string, snake: string, fallback: number) => {
    const a = r[camel];
    if (typeof a === "number" && Number.isFinite(a)) return a;
    const b = r[snake];
    if (typeof b === "number" && Number.isFinite(b)) return b;
    return fallback;
  };
  const strOrNull = (camel: string, snake: string) => {
    const a = r[camel];
    if (typeof a === "string") return a;
    const b = r[snake];
    if (typeof b === "string") return b;
    return "";
  };
  const maybeNullNum = (camel: string, snake: string): number | null => {
    const a = r[camel];
    if (a === null || a === undefined) {
      const b = r[snake];
      if (b === null || b === undefined) return null;
      return typeof b === "number" && Number.isFinite(b) ? b : null;
    }
    return typeof a === "number" && Number.isFinite(a) ? a : null;
  };
  const strArr = (camel: string, snake: string): string[] => {
    const a = r[camel];
    if (Array.isArray(a)) return a.filter((x): x is string => typeof x === "string");
    const b = r[snake];
    if (Array.isArray(b)) return b.filter((x): x is string => typeof x === "string");
    return [];
  };
  const members = (() => {
    const m = r.members;
    if (Array.isArray(m)) return m.filter((x): x is string => typeof x === "string");
    return [];
  })();
  const strategyRaw = r.strategy;
  const strategy =
    strategyRaw !== null &&
    strategyRaw !== undefined &&
    typeof strategyRaw === "object" &&
    !Array.isArray(strategyRaw)
      ? (strategyRaw as Record<string, unknown>)
      : null;

  const translationScriptIds = (() => {
    const a = r.translationScriptIds;
    const b = r.translation_script_ids;
    const arr = Array.isArray(a) ? a : Array.isArray(b) ? b : [];
    return arr
      .map((x) => (typeof x === "number" && Number.isFinite(x) ? x : Number(x)))
      .filter((x) => Number.isFinite(x));
  })();

  const defaultTags = (() => {
    const raw = r.defaultTags ?? r.default_tags;
    if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
      return Object.fromEntries(
        Object.entries(raw as Record<string, unknown>)
          .filter(([, v]) => typeof v === "string" || v === null)
          .map(([k, v]) => [k, v as string | null]),
      ) as Record<string, string | null>;
    }
    return {} as Record<string, string | null>;
  })();

  return {
    id: typeof r.id === "number" ? r.id : Number(r.id) || 0,
    name: typeof r.name === "string" ? r.name : "",
    enabled: r.enabled !== false,
    members,
    maxRunningQueries: num("maxRunningQueries", "max_running_queries", 10),
    maxQueuedQueries: maybeNullNum("maxQueuedQueries", "max_queued_queries"),
    strategy,
    allowGroups: strArr("allowGroups", "allow_groups"),
    allowUsers: strArr("allowUsers", "allow_users"),
    translationScriptIds,
    defaultTags,
    cache: ((r as Record<string, unknown>).cache as import("./api-types").GroupCacheConfig | null) ?? null,
    createdAt: strOrNull("createdAt", "created_at") || new Date().toISOString(),
    updatedAt: strOrNull("updatedAt", "updated_at") || new Date().toISOString(),
  };
}

export function clusterGroupRecordToUpsert(
  r: ClusterGroupConfigRecord,
  overrides: Partial<UpsertClusterGroupConfig>,
): UpsertClusterGroupConfig {
  return {
    enabled: overrides.enabled ?? r.enabled,
    members: overrides.members ?? [...r.members],
    maxRunningQueries: overrides.maxRunningQueries ?? r.maxRunningQueries,
    maxQueuedQueries:
      overrides.maxQueuedQueries !== undefined
        ? overrides.maxQueuedQueries
        : r.maxQueuedQueries,
    strategy:
      overrides.strategy !== undefined ? overrides.strategy : r.strategy,
    allowGroups: overrides.allowGroups ?? [...r.allowGroups],
    allowUsers: overrides.allowUsers ?? [...r.allowUsers],
    translationScriptIds:
      overrides.translationScriptIds !== undefined
        ? overrides.translationScriptIds
        : [...r.translationScriptIds],
    defaultTags:
      overrides.defaultTags !== undefined ? overrides.defaultTags : { ...r.defaultTags },
  };
}
