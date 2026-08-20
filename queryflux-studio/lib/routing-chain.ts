/**
 * Order-preserving routing chain ↔ admin API `routers` array.
 * The proxy evaluates `routers` in order; this module avoids merging/reordering.
 */

import type {
  CompoundConditionEntry,
  RouterConfigEntry,
  TagRoutingRule,
} from "@/lib/api-types";

export type ChainItem =
  | { id: string; kind: "protocol"; protocol: string; targetGroupId: number | null }
  | { id: string; kind: "header"; headerName: string; headerValue: string; targetGroupId: number | null }
  | { id: string; kind: "user"; username: string; targetGroupId: number | null }
  | {
      id: string;
      kind: "tag";
      /** Tag key to match. */
      tagKey: string;
      /** Tag value to match. Empty string = key-only (any value). */
      tagValue: string;
      targetGroupId: number | null;
    }
  | {
      id: string;
      kind: "regex";
      regex: string;
      /** `route` (default) or `deny`. */
      action: "route" | "deny";
      /** Client-visible message when action is deny. */
      error: string;
      targetGroupId: number | null;
    }
  | {
      id: string;
      kind: "compound";
      combine: "all" | "any";
      conditions: CompoundConditionEntry[];
      targetGroupId: number | null;
    }
  | { id: string; kind: "pythonScript"; script: string; script_file: string | null }
  | { id: string; kind: "passthrough"; router: RouterConfigEntry };

function uid(): string {
  return Math.random().toString(36).slice(2);
}

export function cellToGroupId(
  v: string | number | undefined | null,
  byName: Map<string, number>,
): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v !== "" && byName.has(v)) return byName.get(v)!;
  return null;
}

/** Expand persisted routers into UI rows in **array order**. */
export function routersToChainItems(
  routers: RouterConfigEntry[],
  byName: Map<string, number>,
): ChainItem[] {
  const out: ChainItem[] = [];

  for (const router of routers) {
    switch (router.type) {
      case "protocolBased": {
        const pairs: [string, string | number | null | undefined][] = [
          ["trinoHttp", router.trino_http],
          ["postgresWire", router.postgres_wire],
          ["mysqlWire", router.mysql_wire],
          ["clickHouseHttp", router.clickhouse_http],
          ["flightSql", router.flight_sql],
        ];
        for (const [protocol, raw] of pairs) {
          if (raw == null || raw === "") continue;
          out.push({
            id: uid(),
            kind: "protocol",
            protocol,
            targetGroupId: cellToGroupId(raw, byName),
          });
        }
        break;
      }
      case "header": {
        const name = router.header_name ?? "";
        const idMap = router.headerValueToGroupId;
        const vals = router.header_value_to_group ?? {};
        for (const [val, group] of Object.entries(vals)) {
          const gid =
            (idMap && typeof idMap[val] === "number" ? idMap[val] : undefined) ??
            cellToGroupId(group as string | number, byName);
          out.push({
            id: uid(),
            kind: "header",
            headerName: name,
            headerValue: val,
            targetGroupId: gid,
          });
        }
        break;
      }
      case "userGroup": {
        const idMap = router.userToGroupId;
        const vals = router.user_to_group ?? {};
        for (const [user, group] of Object.entries(vals)) {
          const gid =
            (idMap && typeof idMap[user] === "number" ? idMap[user] : undefined) ??
            cellToGroupId(group as string | number, byName);
          out.push({
            id: uid(),
            kind: "user",
            username: user,
            targetGroupId: gid,
          });
        }
        break;
      }
      case "tags": {
        // Canonical API shape: `rules` (serde RouterConfig::Tags). Legacy Studio used `tag_rules`.
        // We expand each tag key in a rule into a separate UI row (one tag per row).
        const tagRules = router.rules ?? router.tag_rules ?? [];
        for (const rule of tagRules) {
          if (!rule || typeof rule !== "object" || !("tags" in rule)) continue;
          const tr = rule as TagRoutingRule;
          const gid =
            (typeof tr.targetGroupId === "number" ? tr.targetGroupId : undefined) ??
            cellToGroupId(tr.targetGroup ?? tr.target_group, byName);
          for (const [key, val] of Object.entries(tr.tags)) {
            const tagValue = val === null || val === undefined ? "" : val;
            out.push({
              id: uid(),
              kind: "tag",
              tagKey: key,
              tagValue,
              targetGroupId: gid,
            });
          }
        }
        break;
      }
      case "clientTags": {
        // Legacy format: key-only tag → group. Read-only — we always save back as "tags".
        const idMap = router.tagToGroupId;
        const vals = router.tag_to_group ?? {};
        for (const [tag, group] of Object.entries(vals)) {
          const gid =
            (idMap && typeof idMap[tag] === "number" ? idMap[tag] : undefined) ??
            cellToGroupId(group as string | number, byName);
          out.push({
            id: uid(),
            kind: "tag",
            tagKey: tag,
            tagValue: "",
            targetGroupId: gid,
          });
        }
        break;
      }
      case "queryRegex": {
        for (const rule of router.rules ?? []) {
          if (!rule || typeof rule !== "object" || !("regex" in rule)) continue;
          const action = rule.action === "deny" ? "deny" : "route";
          const tgName = rule.targetGroup ?? rule.target_group;
          const gid =
            action === "deny"
              ? null
              : (typeof rule.targetGroupId === "number" ? rule.targetGroupId : undefined) ??
                cellToGroupId(tgName, byName);
          out.push({
            id: uid(),
            kind: "regex",
            regex: rule.regex,
            action,
            error: typeof rule.error === "string" ? rule.error : "",
            targetGroupId: gid,
          });
        }
        break;
      }
      case "compound": {
        const raw = router.conditions ?? [];
        const tgName = router.targetGroup ?? router.target_group;
        const gid =
          (typeof router.targetGroupId === "number" ? router.targetGroupId : undefined) ??
          cellToGroupId(tgName, byName);
        out.push({
          id: uid(),
          kind: "compound",
          combine: router.combine === "any" ? "any" : "all",
          conditions: raw.length > 0 ? [...raw] : [],
          targetGroupId: gid,
        });
        break;
      }
      case "pythonScript": {
        out.push({
          id: uid(),
          kind: "pythonScript",
          script: router.script ?? "",
          script_file: router.script_file ?? null,
        });
        break;
      }
      default:
        out.push({ id: uid(), kind: "passthrough", router: { ...router } });
    }
  }

  return out;
}

function protoApiKey(protocol: string): keyof RouterConfigEntry {
  switch (protocol) {
    case "trinoHttp":
      return "trino_http";
    case "postgresWire":
      return "postgres_wire";
    case "mysqlWire":
      return "mysql_wire";
    case "clickHouseHttp":
      return "clickhouse_http";
    case "flightSql":
      return "flight_sql";
    default:
      return "trino_http";
  }
}

/** Serialize UI rows to API routers in **row order** (one router per row where possible). */
export function chainItemsToRouters(items: ChainItem[]): RouterConfigEntry[] {
  const result: RouterConfigEntry[] = [];

  for (const item of items) {
    switch (item.kind) {
      case "protocol": {
        if (item.targetGroupId == null) continue;
        const key = protoApiKey(item.protocol);
        const entry: RouterConfigEntry = { type: "protocolBased" };
        (entry as unknown as Record<string, unknown>)[key] = item.targetGroupId;
        result.push(entry);
        break;
      }
      case "header": {
        if (item.targetGroupId == null) continue;
        result.push({
          type: "header",
          header_name: item.headerName,
          header_value_to_group: { [item.headerValue]: item.targetGroupId },
          headerValueToGroupId: { [item.headerValue]: item.targetGroupId },
        });
        break;
      }
      case "user": {
        if (item.targetGroupId == null) continue;
        result.push({
          type: "userGroup",
          user_to_group: { [item.username]: item.targetGroupId },
          userToGroupId: { [item.username]: item.targetGroupId },
        });
        break;
      }
      case "tag": {
        if (item.targetGroupId == null) continue;
        const tagRule: TagRoutingRule = {
          tags: { [item.tagKey]: item.tagValue === "" ? null : item.tagValue },
          targetGroupId: item.targetGroupId,
        };
        result.push({
          type: "tags",
          rules: [tagRule],
        });
        break;
      }
      case "regex": {
        if (item.action === "deny") {
          result.push({
            type: "queryRegex",
            rules: [
              {
                regex: item.regex,
                action: "deny",
                ...(item.error.trim() !== "" ? { error: item.error.trim() } : {}),
              },
            ],
          });
          break;
        }
        if (item.targetGroupId == null) continue;
        result.push({
          type: "queryRegex",
          rules: [
            {
              regex: item.regex,
              targetGroupId: item.targetGroupId,
            },
          ],
        });
        break;
      }
      case "compound": {
        if (item.targetGroupId == null) continue;
        const conditions =
          item.conditions.length > 0
            ? item.conditions
            : [{ type: "protocol" as const, protocol: "trinoHttp" }];
        result.push({
          type: "compound",
          combine: item.combine,
          conditions,
          targetGroupId: item.targetGroupId,
        });
        break;
      }
      case "pythonScript": {
        result.push({
          type: "pythonScript",
          script: item.script,
          script_file: item.script_file,
        });
        break;
      }
      case "passthrough":
        result.push(item.router);
        break;
    }
  }

  return result;
}
