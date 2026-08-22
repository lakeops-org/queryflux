"use client";

import { X, Clock, Server, User, Rows3, Hash, ArrowRight, Fingerprint, Cpu, Database, MemoryStick, HardDrive, Tag, Bot, Shield } from "lucide-react";
import type { QueryHistoryRecord, RoutingTrace } from "@/lib/api-types";
import { StatusBadge, EngineBadge, formatDuration, formatDateTime } from "@/components/ui-helpers";
import { GuardActionsList } from "@/components/guard-actions-list";
import { RoutingTraceView } from "@/components/routing-trace-view";

/** Reusable detail body — used both in the slide-over panel and inline in agent conversations. */
export function QueryDetailContent({ query }: { query: QueryHistoryRecord }) {
  return (
    <>
      {/* Meta grid */}
      <div className="px-6 py-5 grid grid-cols-2 gap-4 border-b border-slate-100">
        <MetaItem icon={<Clock size={13} />} label="Time" value={formatDateTime(new Date(query.created_at))} />
        <MetaItem icon={<Clock size={13} />} label="Duration" value={formatDuration(query.execution_duration_ms)} />
        <MetaItem icon={<Server size={13} />} label="Cluster" value={`${query.cluster_group} / ${query.cluster_name}`} />
        <MetaItem icon={<Hash size={13} />} label="Protocol" value={query.frontend_protocol} />
        <MetaItem icon={<User size={13} />} label="User" value={query.username ?? "—"} />
        <MetaItem icon={<Rows3 size={13} />} label="Rows" value={query.rows_returned?.toLocaleString() ?? "—"} />
        <MetaItem icon={<Fingerprint size={13} />} label="Proxy ID" value={query.proxy_query_id} />
        {query.backend_query_id && (
          <MetaItem icon={<Fingerprint size={13} />} label="Engine Query ID" value={query.backend_query_id} />
        )}
      </div>

      <div className="px-6 py-5 space-y-6">
        {/* Agent identity */}
        {query.agent_id && (
          <div>
            <SectionLabel>Agent</SectionLabel>
            <div className="grid grid-cols-2 gap-3">
              <MetaItem icon={<Bot size={13} />} label="Agent ID" value={query.agent_id} />
              {query.conversation_id && (
                <MetaItem icon={<Bot size={13} />} label="Conversation" value={query.conversation_id} />
              )}
              {query.step_index != null && (
                <MetaItem icon={<Hash size={13} />} label="Step" value={String(query.step_index)} />
              )}
              {query.query_intent && (
                <MetaItem icon={<Bot size={13} />} label="Intent" value={query.query_intent} />
              )}
            </div>
          </div>
        )}

        {/* Guard actions */}
        {query.guard_actions && query.guard_actions.length > 0 && (
          <div>
            <SectionLabel>
              <span className="flex items-center gap-1.5">
                <Shield size={11} />
                Guard Actions
              </span>
            </SectionLabel>
            <GuardActionsList actions={query.guard_actions} />
          </div>
        )}

        {/* Tags */}
        {query.query_tags && Object.keys(query.query_tags).length > 0 && (
          <div>
            <SectionLabel>Tags</SectionLabel>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(query.query_tags).map(([key, val]) => (
                <span
                  key={key}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium"
                >
                  <Tag size={9} className="flex-shrink-0" />
                  {val != null ? `${key}: ${val}` : key}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* SQL */}
        <div>
          <SectionLabel>SQL</SectionLabel>
          <pre className="bg-slate-950 text-emerald-300 rounded-xl p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap leading-relaxed border border-slate-800">
            {query.sql_preview || "—"}
          </pre>
        </div>

        {/* Translation */}
        {query.was_translated && (
          <div>
            <SectionLabel>Translated SQL</SectionLabel>
            <div className="flex items-center gap-2 mb-3">
              <span className="px-2 py-0.5 rounded-md bg-violet-100 text-violet-700 text-xs font-mono">{query.source_dialect}</span>
              <ArrowRight size={12} className="text-violet-400" />
              <span className="px-2 py-0.5 rounded-md bg-violet-200 text-violet-800 text-xs font-mono font-semibold">{query.target_dialect}</span>
            </div>
            <pre className="bg-slate-950 text-violet-300 rounded-xl p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap leading-relaxed border border-violet-900/40">
              {query.translated_sql ?? "—"}
            </pre>
          </div>
        )}

        {/* Routing trace */}
        {query.routing_trace && (
          <div>
            <SectionLabel>Routing Trace</SectionLabel>
            <RoutingTraceView trace={query.routing_trace as RoutingTrace} />
          </div>
        )}

        {/* Engine stats */}
        {hasEngineStats(query) && (
          <div>
            <SectionLabel>Engine Stats</SectionLabel>
            <EngineStatsView query={query} />
          </div>
        )}

        {/* Error */}
        {query.error_message && (
          <div>
            <SectionLabel className="text-red-500">Error</SectionLabel>
            <pre className="bg-red-50 text-red-700 rounded-xl p-4 text-xs font-mono whitespace-pre-wrap border border-red-100">
              {query.error_message}
            </pre>
          </div>
        )}
      </div>
    </>
  );
}

export function QueryDetail({
  query,
  onClose,
}: {
  query: QueryHistoryRecord;
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex">
      {/* Backdrop */}
      <div className="flex-1 bg-slate-900/40 backdrop-blur-sm" onClick={onClose} />

      {/* Panel */}
      <div className="w-[580px] bg-white shadow-2xl flex flex-col overflow-hidden border-l border-slate-200">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-100 bg-gradient-to-r from-white to-slate-50">
          <div className="flex items-center gap-2.5 flex-wrap">
            <StatusBadge status={query.status} />
            <EngineBadge engine={query.engine_type} />
            {query.was_translated && (
              <span className="text-xs px-2.5 py-0.5 rounded-full bg-violet-50 text-violet-700 ring-1 ring-violet-200 font-medium">
                translated
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto">
          <QueryDetailContent query={query} />
        </div>
      </div>
    </div>
  );
}

function SectionLabel({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <p className={`text-[11px] font-semibold uppercase tracking-widest mb-3 ${className ?? "text-slate-400"}`}>
      {children}
    </p>
  );
}

function MetaItem({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 text-slate-400 mb-0.5">
        {icon}
        <p className="text-[11px] font-medium uppercase tracking-wide">{label}</p>
      </div>
      <p className="text-sm text-slate-800 font-mono truncate">{value}</p>
    </div>
  );
}

function hasEngineStats(q: QueryHistoryRecord): boolean {
  return (
    q.cpu_time_ms != null ||
    q.processed_rows != null ||
    q.processed_bytes != null ||
    q.physical_input_bytes != null ||
    q.peak_memory_bytes != null ||
    q.spilled_bytes != null ||
    q.total_splits != null
  );
}

function formatBytes(bytes: number | null): string {
  if (bytes == null) return "—";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log2(bytes) / 10), units.length - 1);
  const value = bytes / Math.pow(1024, i);
  return `${value % 1 === 0 ? value : value.toFixed(1)} ${units[i]}`;
}

function StatCell({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1 bg-slate-50 rounded-xl px-3 py-2.5 border border-slate-100">
      <div className="flex items-center gap-1.5 text-slate-400">
        {icon}
        <span className="text-[10px] font-semibold uppercase tracking-wide">{label}</span>
      </div>
      <span className="text-sm font-mono text-slate-800">{value}</span>
    </div>
  );
}

function EngineStatsView({ query }: { query: QueryHistoryRecord }) {
  return (
    <div className="grid grid-cols-2 gap-2">
      <StatCell
        icon={<Cpu size={11} />}
        label="CPU Time"
        value={query.cpu_time_ms != null ? formatDuration(query.cpu_time_ms) : "—"}
      />
      <StatCell
        icon={<Hash size={11} />}
        label="Splits"
        value={query.total_splits?.toLocaleString() ?? "—"}
      />
      <StatCell
        icon={<Rows3 size={11} />}
        label="Rows Scanned"
        value={query.processed_rows?.toLocaleString() ?? "—"}
      />
      <StatCell
        icon={<Database size={11} />}
        label="Bytes Processed"
        value={formatBytes(query.processed_bytes)}
      />
      <StatCell
        icon={<HardDrive size={11} />}
        label="Physical Read"
        value={formatBytes(query.physical_input_bytes)}
      />
      <StatCell
        icon={<MemoryStick size={11} />}
        label="Peak Memory"
        value={formatBytes(query.peak_memory_bytes)}
      />
      {query.spilled_bytes != null && query.spilled_bytes > 0 && (
        <StatCell
          icon={<HardDrive size={11} />}
          label="Spilled"
          value={formatBytes(query.spilled_bytes)}
        />
      )}
    </div>
  );
}

