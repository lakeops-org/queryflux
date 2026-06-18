"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  AlertCircle,
  Loader2,
  Pencil,
  Plus,
  Trash2,
} from "lucide-react";
import type { ClusterGroupConfigRecord } from "@/lib/api-types";
import { formatStrategySummary } from "@/lib/cluster-group-strategy";
import { deleteGroupConfig } from "@/lib/api";
import { GroupFormDialog } from "@/components/group-form-dialog";

type Props = {
  groups: ClusterGroupConfigRecord[];
  clusterNames: string[];
};

export function GroupsConfigPanel({ groups, clusterNames }: Props) {
  const sortedGroups = [...groups].sort((a, b) => a.name.localeCompare(b.name));
  const router = useRouter();
  const [formOpen, setFormOpen] = useState(false);
  const [formMode, setFormMode] = useState<"create" | "edit">("create");
  const [editTarget, setEditTarget] = useState<ClusterGroupConfigRecord | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ClusterGroupConfigRecord | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const existingGroupNames = sortedGroups.map((g) => g.name);

  function openCreate() {
    setFormMode("create");
    setEditTarget(null);
    setFormOpen(true);
  }

  function openEdit(g: ClusterGroupConfigRecord) {
    setFormMode("edit");
    setEditTarget(g);
    setFormOpen(true);
  }

  async function executeDelete() {
    if (!deleteTarget) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      await deleteGroupConfig(deleteTarget.name);
      setDeleteTarget(null);
      router.refresh();
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeleting(false);
    }
  }

  useEffect(() => {
    if (!deleteTarget) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !deleting) {
        setDeleteTarget(null);
        setDeleteError(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [deleteTarget, deleting]);

  return (
    <section className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs mb-8">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 px-6 py-4 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
        <div>
          <h2 className="text-sm font-bold text-slate-900">Cluster group configuration</h2>
          <p className="text-xs text-slate-500 mt-0.5 max-w-xl">
            Full CRUD for groups in Postgres (members, limits, strategy). The proxy reloads
            config after each change. Deleting a group referenced by routing rules will fail until you
            update routing.
          </p>
        </div>
        <button
          type="button"
          onClick={openCreate}
          className="inline-flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold text-white bg-indigo-600 hover:bg-indigo-700 shadow-sm transition-colors flex-shrink-0"
        >
          <Plus size={16} />
          Add group
        </button>
      </div>

      {sortedGroups.length === 0 ? (
        <div className="px-6 py-12 text-center">
          <p className="text-sm text-slate-500">No cluster groups in Postgres yet.</p>
          <p className="text-xs text-slate-400 mt-1">
            Create one above, or seed from YAML on first proxy start.
          </p>
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 text-left text-[10px] font-semibold text-slate-400 uppercase tracking-wider">
                <th className="px-6 py-3">Name</th>
                <th className="px-4 py-3">Enabled</th>
                <th className="px-4 py-3">Members</th>
                <th className="px-4 py-3 min-w-[140px]">Strategy</th>
                <th className="px-4 py-3 w-24">Max run</th>
                <th className="px-4 py-3 w-28">Max queue</th>
                <th className="px-4 py-3 w-28">Queue timeout</th>
                <th className="px-6 py-3 text-right w-40">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {sortedGroups.map((g) => (
                <tr key={g.name} className="hover:bg-slate-50/80">
                  <td className="px-6 py-3 font-mono font-semibold text-slate-800">{g.name}</td>
                  <td className="px-4 py-3">
                    <span
                      className={`text-[11px] font-semibold px-2 py-0.5 rounded-md border ${
                        g.enabled
                          ? "text-emerald-800 bg-emerald-50 border-emerald-200"
                          : "text-slate-600 bg-slate-100 border-slate-200"
                      }`}
                    >
                      {g.enabled ? "Yes" : "No"}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-slate-600 max-w-md">
                    <span className="text-xs line-clamp-2" title={g.members.join(", ")}>
                      {g.members.length === 0
                        ? "—"
                        : g.members.join(", ")}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-700 max-w-[200px]">
                    <span
                      className="line-clamp-3"
                      title={formatStrategySummary(g.strategy)}
                    >
                      {formatStrategySummary(g.strategy)}
                    </span>
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-slate-700">
                    {g.maxRunningQueries}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-slate-600">
                    {g.maxQueuedQueries ?? "—"}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-slate-600">
                    {g.queueTimeoutMs != null ? `${g.queueTimeoutMs}ms` : "—"}
                  </td>
                  <td className="px-6 py-3 text-right">
                    <div className="flex items-center justify-end gap-1">
                      <button
                        type="button"
                        onClick={() => openEdit(g)}
                        className="p-2 rounded-lg text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 transition-colors"
                        title="Edit group"
                      >
                        <Pencil size={15} />
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setDeleteError(null);
                          setDeleteTarget(g);
                        }}
                        className="p-2 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                        title="Delete group"
                      >
                        <Trash2 size={15} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <GroupFormDialog
        open={formOpen}
        onClose={() => {
          setFormOpen(false);
          setEditTarget(null);
        }}
        mode={formMode}
        initial={formMode === "edit" ? editTarget : null}
        clusterNames={clusterNames}
        existingGroupNames={existingGroupNames}
      />

      {deleteTarget ? (
        <div className="fixed inset-0 z-[75] flex items-center justify-center p-4">
          <button
            type="button"
            aria-label="Dismiss"
            disabled={deleting}
            className="absolute inset-0 bg-slate-900/50 backdrop-blur-[2px] disabled:cursor-wait"
            onClick={() => {
              if (!deleting) {
                setDeleteTarget(null);
                setDeleteError(null);
              }
            }}
          />
          <div
            className="relative w-full max-w-md rounded-2xl bg-white shadow-2xl border border-red-200/90 overflow-hidden"
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-group-title"
          >
            <div className="px-5 py-4 border-b border-red-100 bg-gradient-to-r from-red-50 to-white">
              <h2
                id="delete-group-title"
                className="text-sm font-bold text-red-950"
              >
                Delete cluster group?
              </h2>
              <p className="text-xs text-red-900/85 mt-2">
                Permanently delete{" "}
                <span className="font-mono font-semibold">{deleteTarget.name}</span>? This cannot be
                undone. If routing still references this group, the API will return an error.
              </p>
            </div>
            {deleteError ? (
              <div className="px-5 py-3 border-b border-slate-100">
                <p className="text-xs text-red-600 flex items-start gap-1.5">
                  <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
                  {deleteError}
                </p>
              </div>
            ) : null}
            <div className="flex gap-2 px-5 py-4 bg-slate-50/90 border-t border-slate-100">
              <button
                type="button"
                disabled={deleting}
                onClick={() => {
                  setDeleteTarget(null);
                  setDeleteError(null);
                }}
                className="flex-1 px-3 py-2.5 rounded-xl text-xs font-semibold text-slate-700 bg-white border border-slate-200 hover:bg-slate-50 disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={deleting}
                onClick={() => void executeDelete()}
                className="flex-1 flex items-center justify-center gap-2 px-3 py-2.5 rounded-xl text-xs font-semibold text-white bg-red-600 hover:bg-red-700 disabled:opacity-60"
              >
                {deleting ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
                {deleting ? "Deleting…" : "Delete group"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
