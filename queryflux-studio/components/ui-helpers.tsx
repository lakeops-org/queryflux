import type { ClusterConfigRecord } from "@/lib/api-types";
import { resolveEngineDefForBadge } from "@/lib/engine-badge-def";
import { EngineIcon } from "./engine-icon";

export function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    Success: "bg-emerald-50 text-emerald-700 ring-emerald-200",
    Failed: "bg-red-50 text-red-600 ring-red-200",
    Cancelled: "bg-slate-100 text-slate-500 ring-slate-200",
    Denied: "bg-amber-50 text-amber-800 ring-amber-200",
  };
  const dots: Record<string, string> = {
    Success: "bg-emerald-400",
    Failed: "bg-red-400",
    Cancelled: "bg-slate-400",
    Denied: "bg-amber-500",
  };
  const cls = styles[status] ?? "bg-slate-100 text-slate-500 ring-slate-200";
  const dot = dots[status] ?? "bg-slate-400";
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium ring-1 ${cls}`}>
      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${dot}`}></span>
      {status}
    </span>
  );
}

export function EngineBadge({
  engine,
  clusterConfig,
}: {
  engine: string;
  clusterConfig?: ClusterConfigRecord | null;
}) {
  const def = resolveEngineDefForBadge(engine, clusterConfig);
  const label = def?.name ?? engine;

  if (def) {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-lg text-xs font-semibold bg-white border border-slate-200 text-slate-700 whitespace-nowrap">
        <EngineIcon engine={def} size={16} />
        {label}
      </span>
    );
  }

  return (
    <span className="inline-flex items-center px-2.5 py-0.5 rounded-lg text-xs font-semibold bg-slate-100 text-slate-600 ring-1 ring-slate-200">
      {engine}
    </span>
  );
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

export function formatTime(date: Date): string {
  return new Date(date).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function formatDateTime(date: Date): string {
  return new Date(date).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}
