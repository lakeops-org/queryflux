import { Compass } from "lucide-react";
import { RouteExplainPanel } from "./route-explain-panel";

export default function RouteExplainPage() {
  return (
    <div className="p-6 sm:p-8 space-y-6 max-w-5xl mx-auto">
      <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-white to-indigo-50/60 px-6 py-5 shadow-xs">
        <div className="flex items-center gap-2">
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
            <Compass size={12} />
            Dry run
          </span>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Route Explain</h1>
        </div>
        <p className="text-sm text-slate-500 mt-2 max-w-2xl">
          See which cluster group a query would hit, whether it would be authorized, whether any
          guardrail would block it, and whether that group has room for it right now — without
          running the query or consuming any capacity.
        </p>
      </div>

      <RouteExplainPanel />
    </div>
  );
}
