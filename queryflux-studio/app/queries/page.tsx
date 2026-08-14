import { getQueries, getDistinctEngines, getClusters } from "@/lib/api";
import { QueryTable } from "./query-table";
import { Search, SlidersHorizontal, ChevronLeft, ChevronRight, History } from "lucide-react";

export const revalidate = 0;

interface Props {
  searchParams: Promise<{ search?: string; status?: string; engine?: string; cluster_group?: string; page?: string }>;
}

export default async function QueriesPage({ searchParams }: Props) {
  const params = await searchParams;
  const page = Math.max(1, parseInt(params.page ?? "1"));
  const limit = 50;
  const offset = (page - 1) * limit;

  const [queries, engines, clusters] = await Promise.all([
    getQueries({
      search: params.search,
      status: params.status,
      engine: params.engine,
      cluster_group: params.cluster_group,
      limit,
      offset,
    }),
    getDistinctEngines().catch(() => [] as string[]),
    getClusters().catch(() => []),
  ]);

  // Unique group names from live clusters
  const groups = [...new Set(clusters.map((c) => c.group_name))].sort();

  const hasFilters = !!(params.search || params.status || params.engine || params.cluster_group);

  // Base query string preserving all active filters (excluding page)
  const filterQs = new URLSearchParams();
  if (params.search) filterQs.set("search", params.search);
  if (params.status) filterQs.set("status", params.status);
  if (params.engine) filterQs.set("engine", params.engine);
  if (params.cluster_group) filterQs.set("cluster_group", params.cluster_group);
  const filterBase = filterQs.toString() ? `?${filterQs}&` : "?";

  return (
    <div className="p-6 sm:p-8 space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-white to-indigo-50/60 px-6 py-5 shadow-xs">
        <div className="flex items-start justify-between gap-6 flex-wrap">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
                <History size={12} />
                History
              </span>
              <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Query History</h1>
            </div>
            <p className="text-sm text-slate-500 mt-2 max-w-2xl">Click a row to inspect routing trace, SQL, and timing.</p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-400">Page <span className="tabular-nums">{page}</span></span>
            <span className="w-px h-4 bg-slate-200" />
            <span className="text-xs text-slate-400">Showing <span className="tabular-nums">{queries.length}</span>{queries.length === limit ? "+" : ""} results</span>
          </div>
        </div>
      </div>

      {/* Filter bar */}
      <div className="bg-white rounded-xl border border-slate-200 p-4 shadow-xs">
        <form className="flex gap-4 items-center flex-wrap">
          <div className="relative flex-1 min-w-48">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
            <input
              name="search"
              defaultValue={params.search}
              placeholder="Search SQL…"
              className="h-9 w-full pl-8 pr-3 rounded-lg border border-slate-200 text-sm bg-slate-50 text-slate-700 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all"
            />
          </div>
          <select
            name="status"
            defaultValue={params.status ?? ""}
            className="h-9 px-3 rounded-lg border border-slate-200 text-sm bg-slate-50 text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all cursor-pointer min-w-36"
          >
            <option value="">All statuses</option>
            <option value="Success">Success</option>
            <option value="Failed">Failed</option>
            <option value="Cancelled">Cancelled</option>
            <option value="Denied">Denied</option>
          </select>
          <select
            name="engine"
            defaultValue={params.engine ?? ""}
            className="h-9 px-3 rounded-lg border border-slate-200 text-sm bg-slate-50 text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all cursor-pointer min-w-36"
          >
            <option value="">All engines</option>
            {engines.map((e) => (
              <option key={e} value={e}>{e}</option>
            ))}
          </select>
          {groups.length > 0 && (
            <select
              name="cluster_group"
              defaultValue={params.cluster_group ?? ""}
              className="h-9 px-3 rounded-lg border border-slate-200 text-sm bg-slate-50 text-slate-700 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 focus:bg-white transition-all cursor-pointer min-w-36"
            >
              <option value="">All groups</option>
              {groups.map((g) => (
                <option key={g} value={g}>{g}</option>
              ))}
            </select>
          )}
          <button
            type="submit"
            className="h-9 px-4 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 active:bg-indigo-800 transition-colors flex items-center gap-2 shadow-xs"
          >
            <SlidersHorizontal size={13} />
            Filter
          </button>
          {hasFilters && (
            <a
              href="/queries"
              className="h-9 px-4 rounded-lg border border-slate-200 text-sm text-slate-500 flex items-center hover:bg-slate-50 hover:text-slate-700 transition-colors"
            >
              Clear
            </a>
          )}
        </form>
      </div>

      <QueryTable queries={queries} />

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-slate-400">
          Page {page} · {queries.length} result{queries.length !== 1 ? "s" : ""}
        </span>
        <div className="flex items-center gap-2">
          {page > 1 ? (
            <a
              href={`/queries${filterBase}page=${page - 1}`}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-600 hover:bg-white hover:border-indigo-300 hover:text-indigo-600 transition-all shadow-xs"
            >
              <ChevronLeft size={14} />
              Previous
            </a>
          ) : (
            <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-300 cursor-not-allowed">
              <ChevronLeft size={14} />
              Previous
            </span>
          )}
          {queries.length === limit ? (
            <a
              href={`/queries${filterBase}page=${page + 1}`}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-600 hover:bg-white hover:border-indigo-300 hover:text-indigo-600 transition-all shadow-xs"
            >
              Next
              <ChevronRight size={14} />
            </a>
          ) : (
            <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-300 cursor-not-allowed">
              Next
              <ChevronRight size={14} />
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
