import { getAgents } from "@/lib/api";
import { AgentTable } from "./agent-table";
import { ChevronLeft, ChevronRight, Bot } from "lucide-react";

export const revalidate = 0;

interface Props {
  searchParams: Promise<{ page?: string }>;
}

export default async function AgentsPage({ searchParams }: Props) {
  const params = await searchParams;
  const parsedPage = parseInt(params.page ?? "1", 10);
  const page = Number.isNaN(parsedPage) ? 1 : Math.max(1, parsedPage);
  const limit = 50;
  const offset = (page - 1) * limit;

  const agents = await getAgents({ limit, offset }).catch(() => []);

  return (
    <div className="p-6 sm:p-8 space-y-6 max-w-7xl mx-auto">
      <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-white to-indigo-50/60 px-6 py-5 shadow-xs">
        <div className="flex items-start justify-between gap-6 flex-wrap">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
                <Bot size={12} />
                Agents
              </span>
              <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Agent Activity</h1>
            </div>
            <p className="text-sm text-slate-500 mt-2 max-w-2xl">
              Explore who’s running queries through QueryFlux. Search and sort the list, open the
              detail drawer for a quick look, or jump into an agent page for deep inspection.
            </p>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-400">
              Page <span className="tabular-nums">{page}</span>
            </span>
            <span className="w-px h-4 bg-slate-200" />
            <span className="text-xs text-slate-400">
              Showing <span className="tabular-nums">{agents.length}</span>
              {agents.length === limit ? "+" : ""} results
            </span>
          </div>
        </div>
      </div>

      <AgentTable agents={agents} />

      {(page > 1 || agents.length === limit) && (
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <span className="text-xs text-slate-400 tabular-nums">
            Page {page} · {agents.length} result{agents.length !== 1 ? "s" : ""}
          </span>
          <div className="flex items-center gap-2">
            {page > 1 && (
              <a
                href={`/agents?page=${page - 1}`}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-600 hover:bg-white hover:border-indigo-300 hover:text-indigo-600 transition-all shadow-xs"
              >
                <ChevronLeft size={14} />
                Previous
              </a>
            )}
            {agents.length === limit && (
              <a
                href={`/agents?page=${page + 1}`}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-600 hover:bg-white hover:border-indigo-300 hover:text-indigo-600 transition-all shadow-xs"
              >
                Next
                <ChevronRight size={14} />
              </a>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
