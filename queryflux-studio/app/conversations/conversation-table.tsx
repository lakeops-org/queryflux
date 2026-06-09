"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import type { ConversationSummary } from "@/lib/api-types";
import { formatDateTime } from "@/components/ui-helpers";
import {
  Bot,
  Copy,
  ExternalLink,
  MessageSquare,
  Search,
  ArrowUpDown,
  X,
} from "lucide-react";

type SortKey = "last_seen" | "first_seen" | "step_count";

export function ConversationTable({ conversations }: { conversations: ConversationSummary[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("last_seen");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const next = q
      ? conversations.filter(
          (c) =>
            c.conversation_id.toLowerCase().includes(q) ||
            (c.agent_id?.toLowerCase().includes(q) ?? false),
        )
      : conversations.slice();

    next.sort((a, b) => {
      switch (sortKey) {
        case "first_seen":
          return new Date(b.first_seen).getTime() - new Date(a.first_seen).getTime();
        case "step_count":
          return b.step_count - a.step_count;
        case "last_seen":
        default:
          return new Date(b.last_seen).getTime() - new Date(a.last_seen).getTime();
      }
    });

    return next;
  }, [conversations, query, sortKey]);

  async function copyConversationId(id: string) {
    try {
      await navigator.clipboard.writeText(id);
    } catch {
      // ignore
    }
  }

  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs">
      <div className="px-4 py-3 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-2">
            <div className="relative flex-1 min-w-[220px]">
              <Search
                size={14}
                className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400"
              />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search conversation or agent id…"
                className="w-full h-9 pl-8 pr-8 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
              />
              {query.trim().length > 0 && (
                <button
                  type="button"
                  onClick={() => setQuery("")}
                  className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
                  title="Clear"
                  aria-label="Clear search"
                >
                  <X size={14} aria-hidden="true" />
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
                <option value="last_seen">Last query</option>
                <option value="first_seen">Started</option>
                <option value="step_count">Steps</option>
              </select>
            </div>
          </div>

          <div className="flex items-center justify-between sm:justify-end gap-3">
            <span className="text-xs text-slate-400 tabular-nums">
              {filtered.length.toLocaleString()} / {conversations.length.toLocaleString()} conversations
            </span>
            <button
              type="button"
              onClick={() => router.push("/agents")}
              className="text-xs font-medium text-indigo-600 hover:text-indigo-800 transition-colors"
            >
              Agent activity
            </button>
          </div>
        </div>
      </div>

      {conversations.length === 0 ? (
        <div className="px-6 py-16 text-center">
          <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
            <MessageSquare size={18} className="text-slate-400" />
          </div>
          <p className="text-sm font-medium text-slate-500">No conversations yet</p>
        </div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
              {["Conversation ID", "Agent ID", "Steps", "Started", "Last Query"].map((h) => (
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
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-6 py-12 text-center">
                  <p className="text-sm font-medium text-slate-600">No matches</p>
                  <p className="text-xs text-slate-400 mt-1">Try a different search term.</p>
                </td>
              </tr>
            ) : (
              filtered.map((c, i) => (
                <tr
                  key={c.conversation_id}
                  onClick={() =>
                    router.push(`/conversations/${encodeURIComponent(c.conversation_id)}`)
                  }
                  className={`cursor-pointer hover:bg-indigo-50/60 active:bg-indigo-100/50 transition-colors ${
                    i !== filtered.length - 1 ? "border-b border-slate-50" : ""
                  }`}
                >
                  <td className="px-4 py-3">
                    <span className="inline-flex items-center gap-1.5 font-mono text-xs text-slate-800 font-medium">
                      <MessageSquare size={12} className="text-indigo-400 flex-shrink-0" />
                      {c.conversation_id}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    {c.agent_id ? (
                      <Link
                        href={`/agents/${encodeURIComponent(c.agent_id)}`}
                        className="inline-flex items-center gap-1 font-mono text-xs text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded-md hover:bg-indigo-100 transition-colors"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <Bot size={10} />
                        {c.agent_id}
                      </Link>
                    ) : (
                      <span className="text-xs text-slate-400">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-600 tabular-nums font-medium">
                    {c.step_count.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-400 font-mono whitespace-nowrap">
                    {formatDateTime(new Date(c.first_seen))}
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-400 font-mono whitespace-nowrap">
                    {formatDateTime(new Date(c.last_seen))}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <div className="inline-flex items-center gap-1.5">
                      <button
                        type="button"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          void copyConversationId(c.conversation_id);
                        }}
                        className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors"
                        title="Copy conversation id"
                      >
                        <Copy size={14} />
                      </button>
                      <button
                        type="button"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          router.push(`/conversations/${encodeURIComponent(c.conversation_id)}`);
                        }}
                        className="p-1.5 rounded-lg text-slate-400 hover:text-indigo-700 hover:bg-indigo-50 transition-colors"
                        title="Open conversation"
                      >
                        <ExternalLink size={14} />
                      </button>
                    </div>
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
