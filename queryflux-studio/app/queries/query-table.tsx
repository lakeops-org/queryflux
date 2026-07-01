"use client";

import { useState } from "react";
import type { QueryHistoryRecord } from "@/lib/api-types";
import { StatusBadge, EngineBadge, formatDuration, formatDateTime } from "@/components/ui-helpers";
import { GuardSummaryBadge } from "@/components/guard-actions-list";
import { QueryDetail } from "./query-detail";
import { Bot, Database, Tag } from "lucide-react";

export function QueryTable({ queries }: { queries: QueryHistoryRecord[] }) {
  const [selected, setSelected] = useState<QueryHistoryRecord | null>(null);

  return (
    <>
      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs">
        {queries.length === 0 ? (
          <div className="px-6 py-16 text-center">
            <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
              <Database size={18} className="text-slate-400" />
            </div>
            <p className="text-sm font-medium text-slate-500">No queries found</p>
            <p className="text-xs text-slate-400 mt-1">Try adjusting your filters</p>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
                {["Time", "SQL", "Engine", "Cluster", "Duration", "Tags", "Guard", "Status"].map((h) => (
                  <th key={h} className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {queries.map((q, i) => (
                <tr
                  key={q.id}
                  onClick={() => setSelected(q)}
                  className={`cursor-pointer hover:bg-indigo-50/60 active:bg-indigo-100/50 transition-colors ${
                    i !== queries.length - 1 ? "border-b border-slate-50" : ""
                  }`}
                >
                  <td className="px-4 py-3 text-xs text-slate-400 whitespace-nowrap font-mono">
                    {formatDateTime(new Date(q.created_at))}
                  </td>
                  <td className="px-4 py-3 max-w-xs">
                    <span className="block truncate font-mono text-xs text-slate-700">
                      {q.sql_preview || <span className="text-slate-300 italic">—</span>}
                    </span>
                    <div className="flex items-center gap-1.5 mt-0.5 flex-wrap">
                      {q.cache_hit && (
                        <span className="inline-flex items-center gap-0.5 text-[10px] font-semibold text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded-md border border-amber-200">
                          CACHED
                        </span>
                      )}
                      {q.was_translated && (
                        <span className="text-[10px] font-medium text-violet-500 flex items-center gap-0.5">
                          <span className="w-1 h-1 rounded-full bg-violet-400 inline-block"></span>
                          translated
                        </span>
                      )}
                      {q.agent_id && (
                        <span className="inline-flex items-center gap-0.5 text-[10px] font-medium text-indigo-500 bg-indigo-50 px-1.5 py-0.5 rounded-md">
                          <Bot size={9} />
                          {q.agent_id.length > 12 ? `${q.agent_id.slice(0, 12)}…` : q.agent_id}
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3"><EngineBadge engine={q.engine_type} /></td>
                  <td className="px-4 py-3 text-xs text-slate-500 font-mono">{q.cluster_name}</td>
                  <td className="px-4 py-3 text-xs text-slate-600 tabular-nums font-medium">
                    {formatDuration(q.execution_duration_ms)}
                  </td>
                  <td className="px-4 py-3 max-w-[220px]">
                    {q.query_tags && Object.keys(q.query_tags).length > 0 ? (
                      <div className="flex flex-wrap gap-1">
                        {Object.entries(q.query_tags).map(([key, value]) => (
                          <span
                            key={key}
                            className="inline-flex items-center gap-1 rounded-md border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700"
                          >
                            <Tag size={9} />
                            {value == null ? key : `${key}:${value}`}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-xs text-slate-300">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <GuardSummaryBadge actions={q.guard_actions} wasBlocked={q.was_guard_blocked} />
                  </td>
                  <td className="px-4 py-3"><StatusBadge status={q.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {selected && (
        <QueryDetail query={selected} onClose={() => setSelected(null)} />
      )}
    </>
  );
}
