import type { GuardAction } from "@/lib/api-types";
import { ShieldCheck, ShieldAlert, ShieldX } from "lucide-react";

export function GuardBadge({ action }: { action: "allow" | "warn" | "deny" | string }) {
  if (action === "deny") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-red-50 text-red-700 ring-1 ring-red-200">
        <ShieldX size={10} className="flex-shrink-0" />
        blocked
      </span>
    );
  }
  if (action === "warn") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-700 ring-1 ring-amber-200">
        <ShieldAlert size={10} className="flex-shrink-0" />
        warned
      </span>
    );
  }
  return null;
}

export function GuardActionsList({ actions }: { actions: GuardAction[] }) {
  if (actions.length === 0) return null;

  return (
    <div className="space-y-2">
      {actions.map((a, i) => (
        <div
          key={i}
          className={`flex items-start gap-3 rounded-xl p-3 text-xs border ${
            a.action === "deny"
              ? "bg-red-50 border-red-100"
              : a.action === "warn"
              ? "bg-amber-50 border-amber-100"
              : "bg-slate-50 border-slate-100"
          }`}
        >
          <span className="mt-0.5 flex-shrink-0">
            {a.action === "deny" ? (
              <ShieldX size={14} className="text-red-500" />
            ) : a.action === "warn" ? (
              <ShieldAlert size={14} className="text-amber-500" />
            ) : (
              <ShieldCheck size={14} className="text-emerald-500" />
            )}
          </span>
          <div className="min-w-0 flex-1 space-y-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="font-semibold font-mono text-slate-700">{a.guard}</span>
              <GuardBadge action={a.action} />
              {a.code && (
                <span className="font-mono text-[10px] text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded">
                  {a.code}
                </span>
              )}
            </div>
            {a.reason && (
              <p className={`text-[11px] ${a.action === "deny" ? "text-red-700" : a.action === "warn" ? "text-amber-700" : "text-slate-500"}`}>
                {a.reason}
              </p>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

export function GuardSummaryBadge({ actions, wasBlocked }: { actions: GuardAction[] | null; wasBlocked: boolean }) {
  if (!actions || actions.length === 0) return null;
  if (wasBlocked) {
    return <GuardBadge action="deny" />;
  }
  const hasWarn = actions.some((a) => a.action === "warn");
  if (hasWarn) return <GuardBadge action="warn" />;
  return null;
}
