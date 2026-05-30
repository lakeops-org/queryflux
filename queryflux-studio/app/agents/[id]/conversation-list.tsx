"use client";

import { useMemo, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Loader2,
  MessageSquare,
  Search,
  X,
  ShieldAlert,
  Maximize2,
  Minimize2,
  ArrowUpDown,
} from "lucide-react";
import type { ConversationSummary, QueryHistoryRecord } from "@/lib/api-types";
import { getConversationDetail } from "@/lib/api";
import {
  StatusBadge,
  EngineBadge,
  formatDateTime,
  formatDuration,
} from "@/components/ui-helpers";
import { GuardSummaryBadge } from "@/components/guard-actions-list";
import { QueryDetailContent } from "@/app/queries/query-detail";

type SortKey = "last_seen" | "first_seen" | "step_count";
type BlockedFilter = "all" | "blocked" | "not_blocked";

// ---------------------------------------------------------------------------
// Step dot strip — visual summary of a conversation's steps before expanding
// ---------------------------------------------------------------------------

function StepDots({ count, hasBlocked }: { count: number; hasBlocked: boolean }) {
  const max = 12;
  const dots = Math.min(count, max);
  return (
    <div className="flex items-center gap-0.5">
      {Array.from({ length: dots }).map((_, i) => (
        <span
          key={i}
          className={`inline-block w-1.5 h-1.5 rounded-full ${
            hasBlocked && i === dots - 1 && count <= max
              ? "bg-red-400"
              : "bg-indigo-200"
          }`}
        />
      ))}
      {count > max && (
        <span className="text-[10px] text-slate-400 ml-0.5">+{count - max}</span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step row — timeline layout with vertical connector
// ---------------------------------------------------------------------------

function StepRow({
  step,
  index,
  isLast,
}: {
  step: QueryHistoryRecord;
  index: number;
  isLast: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const isBlocked = step.was_guard_blocked;

  const dotColor = isBlocked
    ? "bg-red-400 ring-red-200"
    : step.status === "SUCCESS"
    ? "bg-emerald-400 ring-emerald-200"
    : step.status === "FAILED"
    ? "bg-red-400 ring-red-200"
    : step.status === "CANCELLED"
    ? "bg-amber-400 ring-amber-200"
    : "bg-slate-300 ring-slate-200";

  return (
    <div className="flex gap-3 px-4">
      {/* Timeline column */}
      <div className="flex flex-col items-center pt-3 shrink-0">
        <span className={`w-2.5 h-2.5 rounded-full ring-2 shrink-0 ${dotColor}`} />
        {!isLast && <span className="w-px flex-1 mt-1 bg-slate-100 min-h-[16px]" />}
      </div>

      {/* Content */}
      <div className={`flex-1 min-w-0 pb-3 ${!isLast ? "" : ""}`}>
        <div
          className={`rounded-xl border overflow-hidden transition-colors ${
            isBlocked
              ? "border-red-100"
              : expanded
              ? "border-indigo-100"
              : "border-slate-100"
          }`}
        >
          {/* Step header row — full row is clickable */}
          <button
            onClick={() => setExpanded((p) => !p)}
            className={`w-full flex items-center gap-2.5 px-3 py-2.5 text-left transition-colors group/step ${
              isBlocked
                ? "bg-red-50/40 hover:bg-red-50/70"
                : expanded
                ? "bg-indigo-50/20 hover:bg-indigo-50/40"
                : "bg-white hover:bg-slate-50"
            }`}
          >
            <span className="text-[10px] font-bold text-slate-400 tabular-nums w-4 shrink-0">
              {step.step_index ?? index + 1}
            </span>
            <StatusBadge status={step.status} />
            <EngineBadge engine={step.engine_type} />
            <GuardSummaryBadge
              actions={step.guard_actions}
              wasBlocked={step.was_guard_blocked}
            />
            <pre
              className={`flex-1 text-xs font-mono truncate min-w-0 ${
                isBlocked ? "text-red-700" : "text-slate-600"
              }`}
            >
              {step.sql_preview}
            </pre>
            <span className="text-[11px] text-slate-300 font-mono whitespace-nowrap shrink-0 tabular-nums">
              {formatDuration(step.execution_duration_ms)}
            </span>
            <span
              className={`p-1 rounded-md transition-colors shrink-0 ${
                expanded
                  ? "text-indigo-500"
                  : "text-slate-200 group-hover/step:text-slate-400"
              }`}
            >
              {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            </span>
          </button>

          {/* Expanded query detail */}
          {expanded && (
            <div className="border-t border-slate-100">
              <QueryDetailContent query={step} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Conversation card
// ---------------------------------------------------------------------------

function ConversationCard({
  conv,
  expanded,
  onToggle,
}: {
  conv: ConversationSummary;
  expanded: boolean;
  onToggle: () => void;
}) {
  const [steps, setSteps] = useState<QueryHistoryRecord[] | null>(null);
  const [loading, setLoading] = useState(false);

  async function toggle() {
    if (!expanded && steps === null) {
      setLoading(true);
      try {
        const data = await getConversationDetail(conv.conversation_id);
        setSteps(data);
      } catch {
        setSteps([]);
      } finally {
        setLoading(false);
      }
    }
    onToggle();
  }

  return (
    <div
      className={`rounded-xl border shadow-xs overflow-hidden transition-colors ${
        conv.has_blocked ? "border-red-100" : "border-slate-200"
      } bg-white`}
    >
      {/* Card header */}
      <button
        onClick={toggle}
        className={`w-full flex items-center gap-3 px-4 py-3.5 text-left transition-colors ${
          expanded
            ? "bg-gradient-to-r from-slate-50 to-white border-b border-slate-100"
            : "hover:bg-slate-50/60"
        }`}
      >
        <span className="text-slate-300 flex-shrink-0">
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </span>

        <MessageSquare
          size={13}
          className={`flex-shrink-0 transition-colors ${
            expanded ? "text-indigo-500" : "text-slate-300"
          }`}
        />

        <span className="font-mono text-sm font-semibold text-slate-800 flex-1 truncate min-w-0">
          {conv.conversation_id}
        </span>

        {/* Step dot strip */}
        <StepDots count={conv.step_count} hasBlocked={conv.has_blocked} />

        <span className="text-xs text-slate-400 tabular-nums shrink-0">
          {conv.step_count} step{conv.step_count !== 1 ? "s" : ""}
        </span>

        {conv.has_blocked && (
          <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-1.5 py-0.5 rounded-md bg-red-50 text-red-600 border border-red-200 shrink-0">
            blocked
          </span>
        )}

        <span className="text-xs text-slate-400 font-mono whitespace-nowrap shrink-0">
          {formatDateTime(new Date(conv.last_seen))}
        </span>
      </button>

      {/* Steps */}
      {expanded && (
        <div className="pt-3 pb-1">
          {loading ? (
            <div className="flex items-center justify-center gap-2 py-8 text-xs text-slate-400">
              <Loader2 size={13} className="animate-spin" />
              Loading steps…
            </div>
          ) : !steps || steps.length === 0 ? (
            <p className="text-xs text-slate-400 px-6 py-4 italic">No steps recorded.</p>
          ) : (
            steps.map((s, i) => (
              <StepRow key={s.proxy_query_id} step={s} index={i} isLast={i === steps.length - 1} />
            ))
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Exported list with toolbar
// ---------------------------------------------------------------------------

export function ConversationList({ conversations }: { conversations: ConversationSummary[] }) {
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("last_seen");
  const [blockedFilter, setBlockedFilter] = useState<BlockedFilter>("all");
  const [expandedIds, setExpandedIds] = useState<Set<string>>(() => new Set());

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    let next = q
      ? conversations.filter((c) => c.conversation_id.toLowerCase().includes(q))
      : conversations.slice();

    if (blockedFilter === "blocked") next = next.filter((c) => c.has_blocked);
    if (blockedFilter === "not_blocked") next = next.filter((c) => !c.has_blocked);

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
  }, [blockedFilter, conversations, query, sortKey]);

  if (conversations.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center w-full">
        <p className="text-sm text-slate-400">No conversations recorded for this agent.</p>
      </div>
    );
  }

  const allExpanded =
    filtered.length > 0 && filtered.every((c) => expandedIds.has(c.conversation_id));

  function toggleExpandAll() {
    if (allExpanded) {
      setExpandedIds(new Set());
    } else {
      setExpandedIds(new Set(filtered.map((c) => c.conversation_id)));
    }
  }

  function toggleOne(id: string) {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <div className="space-y-3 w-full">
      {/* Toolbar */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-xs px-4 py-3 w-full">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-2">
            <div className="relative min-w-[220px]">
              <Search
                size={14}
                className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400"
              />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search conversations…"
                className="w-full h-9 pl-8 pr-8 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
              />
              {query.trim().length > 0 && (
                <button
                  type="button"
                  onClick={() => setQuery("")}
                  className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors"
                >
                  <X size={14} />
                </button>
              )}
            </div>

            <div className="hidden sm:flex items-center gap-2">
              <ArrowUpDown size={13} className="text-slate-400" />
              <select
                value={sortKey}
                onChange={(e) => setSortKey(e.target.value as SortKey)}
                className="h-9 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 px-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
              >
                <option value="last_seen">Last seen</option>
                <option value="first_seen">Started</option>
                <option value="step_count">Steps</option>
              </select>

              <div className="w-px h-5 bg-slate-200" />

              <ShieldAlert size={13} className="text-slate-400" />
              <select
                value={blockedFilter}
                onChange={(e) => setBlockedFilter(e.target.value as BlockedFilter)}
                className="h-9 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 px-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-200 focus:border-indigo-300"
              >
                <option value="all">All</option>
                <option value="blocked">Blocked only</option>
                <option value="not_blocked">Not blocked</option>
              </select>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-400 tabular-nums">
              {filtered.length} / {conversations.length}
            </span>
            <button
              type="button"
              onClick={toggleExpandAll}
              className="inline-flex items-center gap-1.5 text-xs font-medium text-indigo-600 hover:text-indigo-800 transition-colors"
            >
              {allExpanded ? <Minimize2 size={13} /> : <Maximize2 size={13} />}
              {allExpanded ? "Collapse all" : "Expand all"}
            </button>
          </div>
        </div>
      </div>

      {/* Conversation cards */}
      {filtered.length === 0 ? (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-12 text-center">
          <p className="text-sm font-medium text-slate-600">No matches</p>
          <p className="text-xs text-slate-400 mt-1">Try a different search term or filter.</p>
        </div>
      ) : (
        filtered.map((c) => (
          <ConversationCard
            key={c.conversation_id}
            conv={c}
            expanded={expandedIds.has(c.conversation_id)}
            onToggle={() => toggleOne(c.conversation_id)}
          />
        ))
      )}
    </div>
  );
}
