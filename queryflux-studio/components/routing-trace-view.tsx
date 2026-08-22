import { ArrowRight, ShieldX } from "lucide-react";
import type { RoutingTrace } from "@/lib/api-types";

/** Reusable trace renderer — used by the query history detail panel and the
 * route-explain preview panel, so a historical trace and a dry-run trace look identical. */
export function RoutingTraceView({ trace }: { trace: RoutingTrace }) {
  return (
    <div className="space-y-2">
      {trace.decisions.map((d, i) => (
        <div
          key={i}
          className={`flex items-start gap-3 rounded-xl p-3 text-xs border ${
            d.deny_message
              ? "bg-red-50 border-red-100 text-red-800"
              : d.matched
              ? "bg-indigo-50 border-indigo-100 text-indigo-800"
              : "bg-slate-50 border-slate-100 text-slate-500"
          }`}
        >
          <span
            className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0 ${
              d.deny_message
                ? "bg-red-200 text-red-700"
                : d.matched
                ? "bg-indigo-200 text-indigo-700"
                : "bg-slate-200 text-slate-500"
            }`}
          >
            {i + 1}
          </span>
          <div className="min-w-0 flex-1 space-y-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="font-semibold">{d.router_type}</span>
              {d.deny_message ? (
                <span className="flex items-center gap-1 text-red-600">
                  <ShieldX size={10} />
                  denied
                </span>
              ) : d.matched ? (
                d.result && (
                  <span className="flex items-center gap-1 text-indigo-600">
                    <ArrowRight size={10} />
                    <span className="font-mono">{d.result}</span>
                  </span>
                )
              ) : (
                <span className="text-slate-400 italic">no match</span>
              )}
            </div>
            {d.deny_message && <p className="text-[11px] text-red-700">{d.deny_message}</p>}
          </div>
        </div>
      ))}
      <div className="flex items-center gap-2 pt-2 text-xs border-t border-slate-100 mt-2 flex-wrap">
        {trace.denied ? (
          <>
            <span className="text-slate-400 font-medium">Result</span>
            <span className="text-red-700 bg-red-50 px-2 py-0.5 rounded-md border border-red-100">
              denied — {trace.denied}
            </span>
          </>
        ) : (
          <>
            <span className="text-slate-400 font-medium">Final group</span>
            <span className="font-mono font-semibold text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded-md">
              {trace.final_group}
            </span>
            {trace.used_fallback && (
              <span className="text-amber-600 text-[11px] bg-amber-50 px-1.5 py-0.5 rounded-md border border-amber-100">
                fallback
              </span>
            )}
          </>
        )}
      </div>
    </div>
  );
}
