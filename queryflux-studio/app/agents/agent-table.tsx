"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import type { AgentSummary } from "@/lib/api-types";
import { formatDateTime } from "@/components/ui-helpers";
import { Bot, Copy, Search, ArrowUpDown, X } from "lucide-react";

type SortKey = "last_seen" | "query_count" | "conversation_count" | "agent_id";

export function AgentTable({ agents }: { agents: AgentSummary[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("last_seen");


  const filteredAgents = useMemo(() => {
    const q = query.trim().toLowerCase();
    const next = q ? agents.filter((a) => a.agent_id.toLowerCase().includes(q)) : agents.slice();

    next.sort((a, b) => {
      switch (sortKey) {
        case "agent_id":
          return a.agent_id.localeCompare(b.agent_id);
        case "conversation_count":
          return b.conversation_count - a.conversation_count;
        case "query_count":
          return b.query_count - a.query_count;
        case "last_seen":
        default:
          return new Date(b.last_seen).getTime() - new Date(a.last_seen).getTime();
      }
    });

    return next;
  }, [agents, query, sortKey]);

  async function copyAgentId(agentId: string) {
    try {
      await navigator.clipboard.writeText(agentId);
    } catch {
      // ignore clipboard failures (permissions / unsupported)
    }
  }

  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs">
        {/* Toolbar */}
        <div className="px-4 py-3 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-2">
              <div className="relative flex-1 min-w-[220px]">
                <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search agent id…"
                  className="w-full h-9 pl-8 pr-8 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
                />
                {query.trim().length > 0 && (
                  <button
                    type="button"
                    onClick={() => setQuery("")}
                    className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
                    title="Clear"
                  >
                    <X size={14} />
                  </button>
                )}
              </div>

              <div className="hidden sm:flex items-center gap-2">
                <ArrowUpDown size={14} className="text-slate-400" />
                <select
                  value={sortKey}
                  onChange={(e) => setSortKey(e.target.value as SortKey)}
                  className="h-9 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 px-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
                  aria-label="Sort"
                >
                  <option value="last_seen">Last seen</option>
                  <option value="query_count">Queries</option>
                  <option value="conversation_count">Conversations</option>
                  <option value="agent_id">Agent id</option>
                </select>
              </div>
            </div>

            <div className="flex items-center justify-between sm:justify-end gap-3">
              <span className="text-xs text-slate-400 tabular-nums">
                {filteredAgents.length.toLocaleString()} / {agents.length.toLocaleString()} agents
              </span>
              <button
                type="button"
                onClick={() => router.push("/conversations")}
                className="text-xs font-medium text-indigo-600 hover:text-indigo-800 transition-colors"
              >
                View conversations
              </button>
            </div>
          </div>
        </div>

      {agents.length === 0 ? (
        <div className="px-6 py-16 text-center">
          <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
            <Bot size={18} className="text-slate-400" />
          </div>
          <p className="text-sm font-medium text-slate-500">No agent activity yet</p>
          <p className="text-xs text-slate-400 mt-1">
            Queries with <code className="font-mono text-[11px]">X-Agent-Id</code> headers will
            appear here.
          </p>
        </div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
              {["Agent ID", "Queries", "Conversations", "First Seen", "Last Seen"].map((h) => (
                <th
                  key={h}
                  className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider"
                >
                  {h}
                </th>
              ))}
              <th className="px-4 py-3" />
            </tr>
          </thead>
          <tbody>
            {filteredAgents.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-6 py-12 text-center">
                  <p className="text-sm font-medium text-slate-600">No matches</p>
                  <p className="text-xs text-slate-400 mt-1">Try a different agent id fragment.</p>
                </td>
              </tr>
            ) : (
              filteredAgents.map((a, i) => (
              <tr
                key={a.agent_id}
                onClick={() => router.push(`/agents/${encodeURIComponent(a.agent_id)}`)}
                className={`cursor-pointer hover:bg-indigo-50/60 active:bg-indigo-100/50 transition-colors ${
                  i !== filteredAgents.length - 1 ? "border-b border-slate-50" : ""
                }`}
              >
                <td className="px-4 py-3">
                  <span className="inline-flex items-center gap-1.5 font-mono text-xs text-slate-800 font-medium">
                    <Bot size={12} className="text-indigo-400 flex-shrink-0" />
                    {a.agent_id}
                  </span>
                </td>
                <td className="px-4 py-3 text-xs text-slate-600 tabular-nums font-medium">
                  {a.query_count.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-xs text-slate-600 tabular-nums font-medium">
                  {a.conversation_count.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-xs text-slate-400 font-mono whitespace-nowrap">
                  {formatDateTime(new Date(a.first_seen))}
                </td>
                <td className="px-4 py-3 text-xs text-slate-400 font-mono whitespace-nowrap">
                  {formatDateTime(new Date(a.last_seen))}
                </td>
                <td className="px-4 py-3 text-right">
                  <button
                    type="button"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      void copyAgentId(a.agent_id);
                    }}
                    className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors"
                    title="Copy agent id"
                  >
                    <Copy size={14} />
                  </button>
                </td>
              </tr>
              ))
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}
