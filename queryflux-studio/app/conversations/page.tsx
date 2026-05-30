import { getConversations } from "@/lib/api";
import { ConversationTable } from "./conversation-table";
import { ChevronLeft, ChevronRight, MessageSquare } from "lucide-react";

export const revalidate = 0;

interface Props {
  searchParams: Promise<{ agent_id?: string; page?: string }>;
}

function conversationsHref(page: number, agentId?: string) {
  const q = new URLSearchParams();
  if (agentId) q.set("agent_id", agentId);
  if (page > 1) q.set("page", String(page));
  const s = q.toString();
  return s ? `/conversations?${s}` : "/conversations";
}

export default async function ConversationsPage({ searchParams }: Props) {
  const params = await searchParams;
  const parsedPage = parseInt(params.page ?? "1", 10);
  const page = Number.isNaN(parsedPage) ? 1 : Math.max(1, parsedPage);
  const limit = 50;
  const offset = (page - 1) * limit;
  const agentFilter = params.agent_id;

  const conversations = await getConversations({
    agent_id: agentFilter,
    limit,
    offset,
  }).catch(() => []);

  return (
    <div className="p-6 sm:p-8 space-y-6 max-w-7xl mx-auto">
      <div className="rounded-2xl border border-slate-200 bg-gradient-to-br from-white via-white to-indigo-50/60 px-6 py-5 shadow-xs">
        <div className="flex items-start justify-between gap-6 flex-wrap">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
                <MessageSquare size={12} />
                Conversations
              </span>
              <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Conversations</h1>
            </div>
            <p className="text-sm text-slate-500 mt-2 max-w-2xl">
              Multi-step agent conversations grouped by conversation ID. Search and sort the list,
              click a row to open the timeline, or jump to an agent from the agent pill.
            </p>
          </div>

          <div className="flex flex-col items-end gap-1 sm:flex-row sm:items-center sm:gap-2">
            <span className="text-xs text-slate-400">
              Page <span className="tabular-nums">{page}</span>
            </span>
            <span className="hidden sm:inline w-px h-4 bg-slate-200" />
            <span className="text-xs text-slate-400">
              Showing <span className="tabular-nums">{conversations.length}</span>
              {conversations.length === limit ? "+" : ""} results
            </span>
            {agentFilter && (
              <>
                <span className="hidden sm:inline w-px h-4 bg-slate-200" />
                <span className="text-[11px] text-slate-400 font-mono max-w-[220px] truncate" title={agentFilter}>
                  Agent <span className="text-indigo-600">{agentFilter}</span>
                </span>
              </>
            )}
          </div>
        </div>
      </div>

      <ConversationTable conversations={conversations} />

      {(page > 1 || conversations.length === limit) && (
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <span className="text-xs text-slate-400 tabular-nums">
            Page {page} · {conversations.length} result{conversations.length !== 1 ? "s" : ""}
          </span>
          <div className="flex items-center gap-2">
            {page > 1 && (
              <a
                href={conversationsHref(page - 1, agentFilter)}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-sm text-slate-600 hover:bg-white hover:border-indigo-300 hover:text-indigo-600 transition-all shadow-xs"
              >
                <ChevronLeft size={14} />
                Previous
              </a>
            )}
            {conversations.length === limit && (
              <a
                href={conversationsHref(page + 1, agentFilter)}
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
