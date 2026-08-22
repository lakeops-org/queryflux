"use client";

import { useState } from "react";
import { Play, ShieldX, Clock3, Loader2, AlertTriangle } from "lucide-react";
import { explainRoute } from "@/lib/api";
import type { RouteExplainResponse } from "@/lib/api-types";
import { RoutingTraceView } from "@/components/routing-trace-view";
import { GuardActionsList } from "@/components/guard-actions-list";

const PROTOCOLS = [
  { value: "trinoHttp", label: "Trino HTTP" },
  { value: "postgresWire", label: "Postgres wire" },
  { value: "mysqlWire", label: "MySQL wire" },
  { value: "clickhouseHttp", label: "ClickHouse HTTP" },
  { value: "flightSql", label: "Flight SQL" },
  { value: "snowflakeHttp", label: "Snowflake HTTP" },
  { value: "snowflakeSqlApi", label: "Snowflake SQL API v2" },
];

const inputClass =
  "h-9 w-full px-3 rounded-lg border border-slate-200 text-sm bg-slate-50 text-slate-700 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all";

export function RouteExplainPanel() {
  const [sql, setSql] = useState("SELECT 1");
  const [protocol, setProtocol] = useState("trinoHttp");
  const [user, setUser] = useState("");
  const [groups, setGroups] = useState("");
  const [tags, setTags] = useState("");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<RouteExplainResponse | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const resp = await explainRoute({
        sql,
        protocol,
        user: user.trim() || undefined,
        groups: groups
          .split(",")
          .map((g) => g.trim())
          .filter(Boolean),
        tags: parseTags(tags),
      });
      setResult(resp);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
      {/* Form */}
      <form
        onSubmit={handleSubmit}
        className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs space-y-4"
      >
        <div>
          <label className="text-xs font-medium text-slate-500 mb-1.5 block">SQL</label>
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            rows={5}
            className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm font-mono bg-slate-50 text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all resize-y"
            placeholder="SELECT * FROM prod.orders WHERE …"
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-xs font-medium text-slate-500 mb-1.5 block">Protocol</label>
            <select
              value={protocol}
              onChange={(e) => setProtocol(e.target.value)}
              className={`${inputClass} cursor-pointer`}
            >
              {PROTOCOLS.map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 mb-1.5 block">
              Simulated user
            </label>
            <input
              value={user}
              onChange={(e) => setUser(e.target.value)}
              placeholder="anonymous"
              className={inputClass}
            />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-xs font-medium text-slate-500 mb-1.5 block">
              Simulated groups
            </label>
            <input
              value={groups}
              onChange={(e) => setGroups(e.target.value)}
              placeholder="team-a, analytics"
              className={inputClass}
            />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 mb-1.5 block">
              Tags <span className="text-slate-300">(k:v, k:v)</span>
            </label>
            <input
              value={tags}
              onChange={(e) => setTags(e.target.value)}
              placeholder="team:eng, batch"
              className={inputClass}
            />
          </div>
        </div>

        <p className="text-[11px] text-slate-400 leading-relaxed">
          The simulated user and groups above are not verified against any auth provider — this
          previews what routing/authorization/guardrails would do <em>if</em> a query came in
          with this identity. Nothing here is executed or persisted.
        </p>

        <button
          type="submit"
          disabled={loading || !sql.trim()}
          className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm transition-colors"
        >
          {loading ? <Loader2 size={15} className="animate-spin" /> : <Play size={15} />}
          Explain routing
        </button>
      </form>

      {/* Result */}
      <div className="space-y-4">
        {error && (
          <div className="bg-red-50 border border-red-100 rounded-xl p-4 flex items-start gap-2.5">
            <AlertTriangle size={15} className="text-red-500 flex-shrink-0 mt-0.5" />
            <p className="text-sm text-red-700">{error}</p>
          </div>
        )}

        {!error && !result && (
          <div className="bg-white rounded-xl border border-dashed border-slate-200 p-8 text-center text-sm text-slate-400">
            Submit a query to preview its routing decision.
          </div>
        )}

        {result && (
          <>
            <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs">
              <SectionLabel>Routing trace</SectionLabel>
              <RoutingTraceView trace={result.routing_trace} />
            </div>

            {result.denied && (
              <div className="bg-red-50 border border-red-100 rounded-xl p-4 flex items-start gap-2.5">
                <ShieldX size={15} className="text-red-500 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-sm font-medium text-red-800">Query would be denied</p>
                  <p className="text-xs text-red-600 mt-0.5">{result.denied}</p>
                </div>
              </div>
            )}

            {!result.denied && result.guard_actions.length > 0 && (
              <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs">
                <SectionLabel>
                  Guard actions{" "}
                  {result.would_be_guard_blocked && (
                    <span className="ml-1.5 text-red-600 font-normal normal-case tracking-normal">
                      — would block
                    </span>
                  )}
                </SectionLabel>
                <GuardActionsList actions={result.guard_actions} />
              </div>
            )}

            {result.capacity && (
              <div className="bg-white rounded-xl border border-slate-200 p-5 shadow-xs">
                <SectionLabel>
                  Capacity (live snapshot) —{" "}
                  <span className="font-mono normal-case tracking-normal text-slate-600">
                    {result.capacity.group_name}
                  </span>
                </SectionLabel>
                <p className="text-[11px] text-slate-400 mb-3 -mt-1.5">
                  Unlike the routing/guard verdicts above, this reflects live state at the
                  moment of this call and can change before you act on it — treat as advisory.
                </p>
                {result.capacity.would_queue && (
                  <div className="flex items-center gap-1.5 text-xs text-amber-700 bg-amber-50 border border-amber-100 rounded-lg px-3 py-2 mb-3">
                    <Clock3 size={12} />
                    No member has room right now — would not dispatch immediately (this doesn't
                    check the queue-admission limit, so it could also be rejected outright).
                  </div>
                )}
                <div className="space-y-1.5">
                  {result.capacity.members.map((m) => (
                    <div
                      key={m.cluster_name}
                      className="flex items-center justify-between text-xs px-3 py-2 rounded-lg bg-slate-50 border border-slate-100"
                    >
                      <span className="font-mono text-slate-700">{m.cluster_name}</span>
                      <div className="flex items-center gap-3 text-slate-500">
                        <span>{m.running_queries}/{m.max_running_queries} running</span>
                        <span className={m.is_healthy ? "text-emerald-600" : "text-red-600"}>
                          {m.is_healthy ? "healthy" : "unhealthy"}
                        </span>
                        <span className={m.enabled ? "text-slate-500" : "text-slate-400"}>
                          {m.enabled ? "enabled" : "disabled"}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-[11px] font-semibold uppercase tracking-widest mb-3 text-slate-400">
      {children}
    </p>
  );
}

/** Parses "k:v, k2, k3:v3" into QueryTags shape — key-only entries get a null value. */
function parseTags(raw: string): Record<string, string | null> {
  const tags: Record<string, string | null> = {};
  for (const part of raw.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const idx = trimmed.indexOf(":");
    if (idx === -1) {
      tags[trimmed] = null;
    } else {
      tags[trimmed.slice(0, idx).trim()] = trimmed.slice(idx + 1).trim();
    }
  }
  return tags;
}
