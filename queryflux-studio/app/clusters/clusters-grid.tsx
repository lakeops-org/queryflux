"use client";

import { useEffect, useMemo, useState } from "react";
import type { ClusterConfigRecord, ClusterDisplayRow } from "@/lib/api-types";
import {
  deleteClusterConfig,
  getClusterConfig,
  getGroupConfig,
  renameClusterConfig,
  updateCluster,
  upsertClusterConfig,
} from "@/lib/api";
import { readGroupMaxRunningQueries } from "@/lib/cluster-config-helpers";
import {
  buildClusterUpsertFromForm,
  buildValidateShape,
  isAdbcPostgresqlDriver,
  MANAGED_CONFIG_JSON_KEYS,
  persistedClusterConfigToFlat,
  validateEngineSpecific,
} from "@/lib/cluster-persist-form";
import { hiddenAdbcFieldKeysForDriver } from "@/lib/adbc-driver-spec";
import {
  isSaasVariantDriver,
  rowsToVariants,
  validateVariantRows,
  variantsToRows,
} from "@/lib/adbc-saas-variants";
import { AdbcHealthReconcileFields, AdbcSaasVariantsEditor, EngineClusterConfig } from "@/components/cluster-config";
import {
  findEngineDescriptor,
  validateClusterConfig,
  type ConnectionType,
} from "@/lib/engine-registry";
import { EngineBadge } from "@/components/ui-helpers";
import {
  AlertCircle,
  CheckCircle2,
  Database,
  Edit2,
  ExternalLink,
  Layers,
  Loader2,
  Server,
  Trash2,
  Wifi,
  X,
  Zap,
} from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";

/** Same rule as new cluster / group names in Studio when renaming. */
const CLUSTER_NAME_RE = /^[a-zA-Z][a-zA-Z0-9_-]{0,62}$/;

/** Synthesized row from Postgres but missing from `GET /admin/clusters`. */
function clusterDbOverlay(c: ClusterDisplayRow): {
  unassigned: boolean;
  badge: string;
  cardSubtitle: string;
} | null {
  if (!c.configPending) return null;
  const unassigned = !!c.notInAnyGroup;
  return {
    unassigned,
    badge: unassigned ? "Not in any group" : "Not in live state",
    cardSubtitle: unassigned
      ? "In Postgres · add to a cluster group to load"
      : "In Postgres · in a group but proxy did not register it",
  };
}

/** `persisted_max_running_queries === null` means no per-cluster cap (inherit group). */
function clusterUsesInheritedMaxCap(c: ClusterDisplayRow): boolean {
  return c.persisted_max_running_queries === null;
}

function clusterMaxCapDenominator(c: ClusterDisplayRow): string {
  if (c.configPending) return "—";
  if (clusterUsesInheritedMaxCap(c)) return "∞";
  return String(c.max_running_queries);
}

function clusterMaxCapTooltip(c: ClusterDisplayRow): string | undefined {
  if (c.configPending) return undefined;
  if (!clusterUsesInheritedMaxCap(c)) return undefined;
  return `No per-cluster limit — effective cap is ${c.max_running_queries} (cluster group default).`;
}

/** Prefer group+cluster; index suffix handles duplicate members or legacy API duplicates. */
function clusterRowKey(c: ClusterDisplayRow, index: number): string {
  return `${c.group_name}\0${c.cluster_name}\0${index}`;
}

// ---------------------------------------------------------------------------
// Grid
// ---------------------------------------------------------------------------

export function ClustersGrid({
  clusters,
  clusterConfigs,
}: {
  clusters: ClusterDisplayRow[];
  clusterConfigs: ClusterConfigRecord[];
}) {
  const [selected, setSelected] = useState<ClusterDisplayRow | null>(null);

  const configByName = useMemo(
    () => new Map(clusterConfigs.map((r) => [r.name, r])),
    [clusterConfigs],
  );

  if (clusters.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center">
        <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
          <Server size={18} className="text-slate-400" />
        </div>
        <p className="text-sm font-medium text-slate-500">No clusters found</p>
        <p className="text-xs text-slate-400 mt-1">
          Ensure the proxy is running, Postgres persistence is enabled, and clusters exist in YAML or
          the config DB.
        </p>
      </div>
    );
  }

  return (
    <>
      <div className="grid grid-cols-3 gap-4">
        {clusters.map((c, i) => (
          <ClusterCard
            key={clusterRowKey(c, i)}
            cluster={c}
            clusterConfig={configByName.get(c.cluster_name)}
            onClick={() => setSelected(c)}
          />
        ))}
      </div>

      {selected && (
        <ClusterDialog
          cluster={selected}
          clusterConfigHint={configByName.get(selected.cluster_name)}
          onClose={() => setSelected(null)}
          onUpdated={(updated) => setSelected(updated)}
        />
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Card
// ---------------------------------------------------------------------------

function ClusterCard({
  cluster: c,
  clusterConfig,
  onClick,
}: {
  cluster: ClusterDisplayRow;
  clusterConfig?: ClusterConfigRecord;
  onClick: () => void;
}) {
  const overlay = clusterDbOverlay(c);
  const utilPct =
    c.max_running_queries > 0
      ? Math.round((c.running_queries / c.max_running_queries) * 100)
      : 0;

  const barColor = overlay
    ? overlay.unassigned
      ? "bg-slate-300"
      : "bg-amber-300"
    : !c.is_healthy
      ? "bg-slate-300"
      : utilPct > 80
        ? "bg-red-400"
        : utilPct > 50
          ? "bg-amber-400"
          : "bg-emerald-400";

  const borderClass = overlay
    ? overlay.unassigned
      ? "border-slate-200 hover:border-slate-300"
      : "border-amber-200 hover:border-amber-300"
    : !c.is_healthy
      ? "border-red-200 hover:border-red-300"
      : "border-slate-200 hover:border-indigo-200";

  const headerBg = overlay
    ? overlay.unassigned
      ? "from-slate-50 to-white"
      : "from-amber-50 to-white"
    : !c.is_healthy
      ? "from-red-50 to-white"
      : "from-slate-50 to-white";

  const titleClass = overlay
    ? overlay.unassigned
      ? "text-slate-800"
      : "text-amber-900"
    : c.is_healthy
      ? "text-slate-800"
      : "text-red-700";

  return (
    <button
      onClick={onClick}
      className={`text-left bg-white rounded-xl border overflow-hidden shadow-xs transition-all duration-150 w-full hover:shadow-sm ${borderClass}`}
    >
      <div className={`px-4 py-3 border-b border-slate-100 bg-gradient-to-r ${headerBg}`}>
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <p className={`text-sm font-bold truncate ${titleClass}`}>{c.cluster_name}</p>
            <p className="text-[11px] text-slate-400 mt-0.5 truncate">
              {overlay ? overlay.cardSubtitle : c.group_name}
            </p>
          </div>
          <div className="flex items-center gap-1.5 flex-shrink-0 mt-0.5">
            {overlay ? (
              <span
                className={`text-[9px] font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded-md border ${
                  overlay.unassigned
                    ? "text-slate-700 bg-slate-100 border-slate-200"
                    : "text-amber-900 bg-amber-100 border-amber-200"
                }`}
              >
                {overlay.badge}
              </span>
            ) : c.is_healthy ? (
              <CheckCircle2 size={14} className="text-emerald-400" />
            ) : (
              <AlertCircle size={14} className="text-red-400" />
            )}
          </div>
        </div>
        <div className="mt-2">
          <EngineBadge engine={c.engine_type} clusterConfig={clusterConfig} />
        </div>
      </div>

      <div className="px-4 py-3 space-y-3">
        <div className="grid grid-cols-2 gap-2 text-xs">
          <div>
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wide mb-0.5">
              Running
            </p>
            <p className="font-mono font-semibold text-slate-700">
              {c.configPending ? (
                <span className="text-slate-400 font-normal">—</span>
              ) : (
                <>
                  {c.running_queries}{" "}
                  <span
                    className="text-slate-400 font-normal"
                    title={clusterMaxCapTooltip(c) ?? undefined}
                  >
                    / {clusterMaxCapDenominator(c)}
                  </span>
                </>
              )}
            </p>
          </div>
          <div>
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wide mb-0.5">
              Queued
            </p>
            <p
              className={`font-mono font-semibold ${c.configPending ? "text-slate-400" : c.queued_queries > 0 ? "text-amber-600" : "text-slate-500"}`}
            >
              {c.configPending ? "—" : c.queued_queries}
            </p>
          </div>
        </div>

        <div className="space-y-1">
          <div className="flex items-center justify-between text-[10px]">
            <span className="text-slate-400 font-medium">Utilization</span>
            <span
              className={`font-semibold ${
                overlay
                  ? overlay.unassigned
                    ? "text-slate-500"
                    : "text-amber-600"
                  : utilPct > 80
                    ? "text-red-500"
                    : utilPct > 50
                      ? "text-amber-500"
                      : "text-emerald-500"
              }`}
            >
              {c.configPending ? "—" : `${utilPct}%`}
            </span>
          </div>
          <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${barColor}`}
              style={{ width: `${c.configPending ? 100 : Math.max(utilPct, 0)}%` }}
            />
          </div>
        </div>
      </div>
    </button>
  );
}

// ---------------------------------------------------------------------------
// Dialog
// ---------------------------------------------------------------------------

function ClusterDialog({
  cluster: c,
  clusterConfigHint,
  onClose,
  onUpdated,
}: {
  cluster: ClusterDisplayRow;
  /** From server merge; used for badge until GET config returns. */
  clusterConfigHint?: ClusterConfigRecord;
  onClose: () => void;
  onUpdated: (updated: ClusterDisplayRow) => void;
}) {
  const router = useRouter();
  const descriptor = findEngineDescriptor(c.engine_type);
  const overlay = clusterDbOverlay(c);

  const [persisted, setPersisted] = useState<ClusterConfigRecord | null>(null);
  const [persistStatus, setPersistStatus] = useState<"loading" | "ok" | "error">(
    "loading",
  );

  const [editing, setEditing] = useState(false);
  const [editEnabled, setEditEnabled] = useState(c.enabled);
  const [editMaxInput, setEditMaxInput] = useState("");
  const [editFlat, setEditFlat] = useState<Record<string, string>>({});
  const [editClusterName, setEditClusterName] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setPersistStatus("loading");
    setPersisted(null);
    getClusterConfig(c.cluster_name)
      .then((rec) => {
        if (!cancelled) {
          setPersisted(rec);
          setPersistStatus("ok");
        }
      })
      .catch(() => {
        if (!cancelled) setPersistStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [c.cluster_name]);

  const utilPct =
    c.max_running_queries > 0
      ? Math.round((c.running_queries / c.max_running_queries) * 100)
      : 0;

  const barColor = overlay
    ? overlay.unassigned
      ? "bg-slate-300"
      : "bg-amber-300"
    : !c.is_healthy
      ? "bg-slate-300"
      : utilPct > 80
        ? "bg-red-400"
        : utilPct > 50
          ? "bg-amber-400"
          : "bg-emerald-400";

  function startEdit() {
    setSaveError(null);
    if (persistStatus !== "ok" || !persisted) {
      setSaveError(
        "Could not load persisted cluster config. Ensure Postgres persistence is enabled and this cluster exists in the config database.",
      );
      return;
    }
    const d = findEngineDescriptor(persisted.engineKey);
    setEditEnabled(persisted.enabled);
    setEditMaxInput(
      persisted.maxRunningQueries != null ? String(persisted.maxRunningQueries) : "",
    );
    setEditFlat(
      persistedClusterConfigToFlat(
        persisted.config as Record<string, unknown>,
        d,
      ),
    );
    setEditClusterName(persisted.name);
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
    setSaveError(null);
    setEditClusterName("");
  }

  function closeOrDismissDeleteConfirm() {
    if (deleteConfirmOpen) {
      if (!deleting) {
        setDeleteConfirmOpen(false);
        setDeleteError(null);
      }
      return;
    }
    onClose();
  }

  useEffect(() => {
    if (!deleteConfirmOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !deleting) {
        setDeleteConfirmOpen(false);
        setDeleteError(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [deleteConfirmOpen, deleting]);

  async function executeDeleteCluster() {
    setDeleting(true);
    setDeleteError(null);
    try {
      await deleteClusterConfig(c.cluster_name);
      setDeleteConfirmOpen(false);
      router.refresh();
      onClose();
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeleting(false);
    }
  }

  async function save() {
    if (persistStatus !== "ok" || !persisted) {
      setSaveError("Persisted config is not loaded.");
      return;
    }

    const targetName = editClusterName.trim();
    if (!targetName) {
      setSaveError("Cluster name is required.");
      return;
    }
    if (targetName !== c.cluster_name && !CLUSTER_NAME_RE.test(targetName)) {
      setSaveError(
        "Cluster name must start with a letter and use only letters, numbers, underscores, and hyphens (max 63 chars).",
      );
      return;
    }

    const trimmed = editMaxInput.trim();
    let maxOverride: number | null = null;
    if (trimmed !== "") {
      const n = parseInt(trimmed, 10);
      if (!Number.isFinite(n) || n < 1 || String(n) !== trimmed) {
        setSaveError(
          "Max concurrent queries must be a positive integer, or leave empty for ∞ (group default).",
        );
        return;
      }
      maxOverride = n;
    }

    const specific = validateEngineSpecific(persisted.engineKey, editFlat);
    if (specific.length > 0) {
      setSaveError(specific.join(" "));
      return;
    }
    const validatePayload = buildValidateShape(editFlat);
    const schemaErrs = validateClusterConfig(
      targetName,
      persisted.engineKey,
      validatePayload,
      { skipImplementedCheck: true },
    );
    if (schemaErrs.length > 0) {
      setSaveError(schemaErrs.join(" "));
      return;
    }

    const driver =
      editFlat.driver ??
      String((persisted.config as Record<string, unknown>).driver ?? "");
    if (isSaasVariantDriver(driver)) {
      const variantErrs = validateVariantRows(
        variantsToRows(persisted.variants, driver),
        driver,
      );
      if (variantErrs.length > 0) {
        setSaveError(variantErrs.join(" "));
        return;
      }
    }

    setSaving(true);
    setSaveError(null);
    try {
      if (targetName !== c.cluster_name) {
        await renameClusterConfig(c.cluster_name, { newName: targetName });
      }
      const newRecord = await upsertClusterConfig(
        targetName,
        buildClusterUpsertFromForm(persisted, editFlat, {
          enabled: editEnabled,
          maxRunningQueriesInput: editMaxInput,
        }),
      );
      setPersisted(newRecord);

      if (!c.configPending) {
        let runtimeMax: number;
        if (maxOverride !== null) {
          runtimeMax = maxOverride;
        } else {
          const g = await getGroupConfig(c.group_name);
          runtimeMax = readGroupMaxRunningQueries(g) || c.max_running_queries;
        }

        const updated = await updateCluster(c.group_name, targetName, {
          enabled: editEnabled,
          max_running_queries: runtimeMax,
        });

        onUpdated({
          ...updated,
          configPending: false,
          persisted_max_running_queries: maxOverride,
        });
      } else {
        router.refresh();
        onUpdated({
          ...c,
          enabled: editEnabled,
          persisted_max_running_queries: maxOverride,
        });
      }
      setEditing(false);
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm"
        onClick={closeOrDismissDeleteConfirm}
      />

      <div className="relative z-10 w-full max-w-2xl max-h-[min(92vh,880px)] flex flex-col bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden">
        {/* Header */}
        <div
          className={`flex items-center justify-between px-6 py-5 border-b border-slate-100 flex-shrink-0 bg-gradient-to-r ${
            overlay
              ? overlay.unassigned
                ? "from-slate-50 to-white"
                : "from-amber-50 to-white"
              : !c.is_healthy
                ? "from-red-50 to-white"
                : "from-slate-50 to-white"
          }`}
        >
          <div className="flex items-center gap-3">
            <div
              className={`w-9 h-9 rounded-xl flex items-center justify-center ${
                overlay
                  ? overlay.unassigned
                    ? "bg-slate-100 border border-slate-200"
                    : "bg-amber-50 border border-amber-200"
                  : c.is_healthy
                    ? "bg-emerald-50 border border-emerald-200"
                    : "bg-red-50 border border-red-200"
              }`}
            >
              {overlay ? (
                <AlertCircle
                  size={18}
                  className={overlay.unassigned ? "text-slate-600" : "text-amber-600"}
                />
              ) : c.is_healthy ? (
                <CheckCircle2 size={18} className="text-emerald-500" />
              ) : (
                <AlertCircle size={18} className="text-red-500" />
              )}
            </div>
            <div>
              <p className="text-base font-bold text-slate-900">
                {c.cluster_name}
              </p>
              <p className="text-[11px] text-slate-400">
                {overlay ? overlay.cardSubtitle : c.group_name}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-1">
            {!editing && (
              <button
                type="button"
                disabled={persistStatus !== "ok"}
                onClick={startEdit}
                className="w-8 h-8 rounded-lg flex items-center justify-center text-slate-400 hover:text-indigo-600 hover:bg-indigo-50 transition-all disabled:opacity-40 disabled:pointer-events-none"
                title={
                  persistStatus === "loading"
                    ? "Loading config…"
                    : persistStatus === "error"
                      ? "Persisted config unavailable"
                      : "Edit cluster config (Postgres)"
                }
              >
                <Edit2 size={14} />
              </button>
            )}
            <button
              type="button"
              onClick={closeOrDismissDeleteConfirm}
              className="w-8 h-8 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all"
            >
              <X size={16} />
            </button>
          </div>
        </div>

        <div className="px-6 py-5 space-y-5 overflow-y-auto flex-1 min-h-0">
          {overlay?.unassigned && (
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-800">
              <p className="font-medium">Not in any cluster group</p>
              <p className="text-xs text-slate-600 mt-1 leading-relaxed">
                QueryFlux only loads clusters that appear in an <strong>enabled</strong> group&apos;s{" "}
                <code className="text-[11px] bg-white px-1 rounded border">members</code> list.
                Add this cluster to a group (API or YAML), then save — hot-reload will pick it up.
                Restarting alone does not help.
              </p>
              <Link
                href="/engines"
                className="inline-flex mt-2 text-xs font-semibold text-indigo-600 hover:text-indigo-700"
              >
                Open Groups overview →
              </Link>
            </div>
          )}
          {overlay && !overlay.unassigned && (
            <div className="rounded-xl border border-amber-200 bg-amber-50/80 px-4 py-3 text-sm text-amber-950">
              <p className="font-medium">Not in live proxy state</p>
              <p className="text-xs text-amber-900/90 mt-1 leading-relaxed">
                This cluster is in a group in Postgres but did not show up in live snapshots — check
                proxy logs (adapter build errors, disabled cluster, etc.) or wait for config reload.
              </p>
            </div>
          )}
          {/* Status + engine badges */}
          <div className="flex items-center gap-2 flex-wrap">
            {overlay ? (
              <span
                className={`flex items-center gap-1.5 text-xs font-semibold px-2.5 py-1 rounded-lg border ${
                  overlay.unassigned
                    ? "text-slate-700 bg-slate-100 border-slate-200"
                    : "text-amber-800 bg-amber-100 border-amber-200"
                }`}
              >
                <AlertCircle size={11} /> {overlay.badge}
              </span>
            ) : c.is_healthy ? (
              <span className="flex items-center gap-1.5 text-xs font-semibold text-emerald-600 bg-emerald-50 px-2.5 py-1 rounded-lg border border-emerald-200">
                <CheckCircle2 size={11} /> Healthy
              </span>
            ) : (
              <span className="flex items-center gap-1.5 text-xs font-semibold text-red-600 bg-red-50 px-2.5 py-1 rounded-lg border border-red-200">
                <AlertCircle size={11} /> Unhealthy
              </span>
            )}
            {!c.enabled && (
              <span className="text-xs font-semibold text-slate-500 bg-slate-100 px-2.5 py-1 rounded-lg border border-slate-200">
                Disabled
              </span>
            )}
            <EngineBadge
              engine={c.engine_type}
              clusterConfig={
                persistStatus === "ok" && persisted
                  ? persisted
                  : clusterConfigHint
              }
            />
            {descriptor && (
              <ConnectionTypeBadge type={descriptor.connectionType} />
            )}
          </div>

          {persistStatus === "loading" && (
            <p className="text-xs text-slate-500 flex items-center gap-2">
              <Loader2 size={14} className="animate-spin text-indigo-500" />
              Loading persisted configuration…
            </p>
          )}
          {persistStatus === "error" && (
            <p className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
              Could not load this cluster from the config database. Editing connection fields requires
              Postgres persistence and a row in <code className="font-mono">cluster_configs</code>.
            </p>
          )}

          {/* Config — view mode */}
          {!editing && (
            <EngineConfigView
              cluster={c}
              persisted={persistStatus === "ok" ? persisted : null}
            />
          )}

          {/* Config — edit mode */}
          {editing && persisted && (
            <EngineEditForm
              cluster={c}
              persisted={persisted}
              editClusterName={editClusterName}
              onChangeClusterName={setEditClusterName}
              editEnabled={editEnabled}
              editMaxInput={editMaxInput}
              editFlat={editFlat}
              onPatchFlat={(patch) => setEditFlat((prev) => ({ ...prev, ...patch }))}
              onToggleEnabled={() => setEditEnabled((v) => !v)}
              onChangeMaxInput={setEditMaxInput}
              saveError={saveError}
              saving={saving}
              onSave={() => void save()}
              onCancel={cancelEdit}
            />
          )}

          {/* Live utilization */}
          {!c.configPending && (
            <div>
              <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
                Live Utilization
              </p>
              <div className="grid grid-cols-3 gap-2 mb-3">
                <MiniStat label="Running" value={c.running_queries.toString()} />
                <MiniStat
                  label="Max"
                  value={
                    clusterUsesInheritedMaxCap(c) ? "∞" : c.max_running_queries.toString()
                  }
                  title={clusterMaxCapTooltip(c)}
                />
                <MiniStat
                  label="Queued"
                  value={c.queued_queries.toString()}
                  highlight={c.queued_queries > 0}
                />
              </div>
              <div className="space-y-1">
                <div className="flex justify-between text-[10px]">
                  <span className="text-slate-400">Utilization</span>
                  <span
                    className={`font-semibold ${
                      utilPct > 80
                        ? "text-red-500"
                        : utilPct > 50
                          ? "text-amber-500"
                          : "text-emerald-500"
                    }`}
                  >
                    {utilPct}%
                  </span>
                </div>
                <div className="h-2 bg-slate-200 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all duration-500 ${barColor}`}
                    style={{ width: `${Math.max(utilPct, 0)}%` }}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Actions */}
          <div className="border-t border-slate-100 pt-4 flex gap-2">
            {!c.configPending && (
              <Link
                href={`/queries?cluster_group=${encodeURIComponent(c.group_name)}`}
                className="flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium text-indigo-600 bg-indigo-50 hover:bg-indigo-100 border border-indigo-100 transition-colors"
              >
                <Layers size={13} />
                View queries
              </Link>
            )}
            {c.endpoint && (
              <a
                href={c.endpoint}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-xs font-medium text-slate-600 bg-slate-50 hover:bg-slate-100 border border-slate-200 transition-colors"
              >
                <ExternalLink size={13} />
                Open endpoint
              </a>
            )}
          </div>

          <div className="border-t border-slate-100 pt-4 space-y-2">
            <button
              type="button"
              disabled={editing || deleting}
              onClick={() => {
                setDeleteError(null);
                setDeleteConfirmOpen(true);
              }}
              className="w-full flex items-center justify-center gap-2 px-3 py-2.5 rounded-lg text-xs font-semibold text-red-700 bg-red-50 hover:bg-red-100 border border-red-200/80 transition-colors disabled:opacity-50 disabled:pointer-events-none"
            >
              <Trash2 size={14} />
              Delete cluster…
            </button>
            <p className="text-[10px] text-slate-500 text-center leading-relaxed px-1">
              Removes this cluster from <strong>all</strong> groups in Postgres, then deletes its
              config. Requires persistence; proxy config reloads automatically.
            </p>
          </div>
        </div>
      </div>

      {deleteConfirmOpen ? (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center p-4"
          role="presentation"
        >
          <button
            type="button"
            aria-label="Dismiss"
            disabled={deleting}
            className="absolute inset-0 bg-slate-900/55 backdrop-blur-[2px] disabled:cursor-wait"
            onClick={() => {
              if (!deleting) {
                setDeleteConfirmOpen(false);
                setDeleteError(null);
              }
            }}
          />
          <div
            className="relative w-full max-w-sm rounded-2xl bg-white shadow-2xl border border-red-200/90 overflow-hidden"
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-cluster-confirm-title"
          >
            <div className="px-5 py-4 border-b border-red-100 bg-gradient-to-r from-red-50 to-white">
              <div className="flex items-start gap-3">
                <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-red-100 border border-red-200 flex items-center justify-center">
                  <Trash2 size={18} className="text-red-700" />
                </div>
                <div className="min-w-0 pt-0.5">
                  <h2
                    id="delete-cluster-confirm-title"
                    className="text-sm font-bold text-red-950 tracking-tight"
                  >
                    Delete cluster?
                  </h2>
                  <p className="text-xs text-red-900/80 mt-1 leading-relaxed">
                    You are about to permanently delete{" "}
                    <span className="font-mono font-semibold text-red-950">{c.cluster_name}</span>.
                  </p>
                </div>
              </div>
            </div>
            <div className="px-5 py-4 space-y-3 text-sm text-slate-700">
              <p className="text-xs text-slate-600 leading-relaxed">
                This removes the cluster from <strong>every</strong> cluster group, deletes its
                stored config, and cannot be undone. The proxy reloads config afterward.
              </p>
              {deleteError ? (
                <p className="text-xs text-red-600 flex items-start gap-1.5 rounded-lg border border-red-200 bg-red-50 px-3 py-2">
                  <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
                  {deleteError}
                </p>
              ) : null}
            </div>
            <div className="flex gap-2 px-5 py-4 border-t border-slate-100 bg-slate-50/90">
              <button
                type="button"
                disabled={deleting}
                onClick={() => {
                  setDeleteConfirmOpen(false);
                  setDeleteError(null);
                }}
                className="flex-1 px-3 py-2.5 rounded-xl text-xs font-semibold text-slate-700 bg-white border border-slate-200 hover:bg-slate-50 transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={deleting}
                onClick={() => void executeDeleteCluster()}
                className="flex-1 flex items-center justify-center gap-2 px-3 py-2.5 rounded-xl text-xs font-semibold text-white bg-red-600 hover:bg-red-700 border border-red-700 transition-colors disabled:opacity-60"
              >
                {deleting ? (
                  <Loader2 size={14} className="animate-spin" />
                ) : (
                  <Trash2 size={14} />
                )}
                {deleting ? "Deleting…" : "Delete permanently"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Engine-aware config view
// ---------------------------------------------------------------------------

const PERSISTED_CONFIG_ROW_ORDER: Array<{ key: string; label: string }> = [
  { key: "endpoint", label: "Endpoint" },
  { key: "databasePath", label: "Database path" },
  { key: "driver", label: "ADBC driver" },
  { key: "uri", label: "Driver URI" },
  { key: "username", label: "Username" },
  { key: "password", label: "Password" },
  { key: "dbKwargs", label: "Driver options" },
  {
    key: "flightSqlClusterDialect",
    label: "Cluster SQL dialect (Flight SQL)",
  },
  { key: "poolSize", label: "Pool size" },
  { key: "maxResultBufferBytes", label: "Max result buffer (bytes)" },
  { key: "region", label: "AWS region" },
  { key: "s3OutputLocation", label: "S3 output location" },
  { key: "workgroup", label: "Workgroup" },
  { key: "catalog", label: "Catalog" },
  { key: "authType", label: "Auth type" },
  { key: "authUsername", label: "Auth username / key id / role ARN" },
  { key: "authPassword", label: "Password / secret key" },
  { key: "authToken", label: "Bearer / session / external id" },
  { key: "tlsInsecureSkipVerify", label: "TLS: skip verify" },
];

function formatPersistedConfigValue(key: string, raw: unknown): string {
  if (raw === undefined || raw === null) return "—";
  if (key === "authPassword" || key === "authToken" || key === "password") {
    const s = typeof raw === "string" ? raw : String(raw);
    return s ? "••••••••" : "—";
  }
  if (typeof raw === "boolean") return raw ? "Yes" : "No";
  const s = typeof raw === "string" ? raw : JSON.stringify(raw);
  return s || "—";
}

function EngineConfigView({
  cluster: c,
  persisted,
}: {
  cluster: ClusterDisplayRow;
  persisted: ClusterConfigRecord | null;
}) {
  const liveDescriptor = findEngineDescriptor(c.engine_type);
  const engineKey = persisted?.engineKey ?? c.engine_type;
  const descriptor = findEngineDescriptor(engineKey) ?? liveDescriptor;
  const connectionType = descriptor?.connectionType ?? "http";
  const cfg = persisted
    ? (persisted.config as Record<string, unknown>)
    : null;

  return (
    <div>
      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
        Configuration
      </p>
      <div className="bg-slate-50 rounded-xl border border-slate-100 divide-y divide-slate-100">
        <ConfigRow
          label="Group"
          value={
            c.configPending
              ? c.notInAnyGroup
                ? "— (not in any group's members)"
                : "— (in group but not live — see banner)"
              : c.group_name
          }
        />
        <ConfigRow
          label="Engine"
          value={descriptor?.displayName ?? engineKey}
        />
        {persisted && (
          <ConfigRow label="Engine key" value={persisted.engineKey} mono />
        )}

        {persisted && cfg ? (
          <>
            <ConfigRow
              label="Enabled (stored)"
              value={persisted.enabled ? "Yes" : "No"}
            />
            <ConfigRow
              label="Max concurrent (stored)"
              value={
                persisted.maxRunningQueries != null
                  ? String(persisted.maxRunningQueries)
                  : "∞ (inherit group)"
              }
              mono
            />
            {PERSISTED_CONFIG_ROW_ORDER.map(({ key, label }) => {
              let raw: unknown;
              if (key === "flightSqlClusterDialect") {
                raw = cfg.flightSqlClusterDialect ?? cfg.flightSqlEngine;
              } else {
                if (!(key in cfg)) return null;
                raw = cfg[key];
              }
              if (raw === undefined || raw === null || raw === "") return null;
              const isEndpoint = key === "endpoint";
              const ep =
                isEndpoint && typeof raw === "string"
                  ? raw
                  : undefined;
              return (
                <ConfigRow
                  key={key}
                  label={label}
                  value={formatPersistedConfigValue(key, raw)}
                  mono
                  link={
                    isEndpoint && connectionType === "http" ? ep : undefined
                  }
                />
              );
            })}
            {Object.keys(cfg).some((k) => !MANAGED_CONFIG_JSON_KEYS.has(k)) && (
              <div className="px-4 py-2.5">
                <p className="text-[11px] text-slate-400 font-medium mb-1">
                  Additional JSON fields
                </p>
                <pre className="text-[10px] font-mono text-slate-600 bg-white border border-slate-100 rounded-lg p-2 overflow-x-auto max-h-28">
                  {JSON.stringify(
                    Object.fromEntries(
                      Object.entries(cfg).filter(
                        ([k]) => !MANAGED_CONFIG_JSON_KEYS.has(k),
                      ),
                    ),
                    null,
                    2,
                  )}
                </pre>
              </div>
            )}
          </>
        ) : (
          <>
            {connectionType === "embedded" ? (
              <ConfigRow label="Database path" value="in-process" mono />
            ) : (
              <ConfigRow
                label={
                  connectionType === "mysqlWire"
                    ? "MySQL endpoint"
                    : connectionType === "driver"
                      ? "Driver URI"
                    : "HTTP endpoint"
                }
                value={c.endpoint ?? "—"}
                mono
                link={connectionType === "http" ? (c.endpoint ?? undefined) : undefined}
              />
            )}
            <ConfigRow
              label="Max concurrent queries"
              value={
                c.configPending && c.max_running_queries === 0
                  ? "— (from YAML / defaults after reload)"
                  : clusterUsesInheritedMaxCap(c)
                    ? `∞ (effective ${c.max_running_queries} from group)`
                    : c.max_running_queries.toString()
              }
              mono
            />
            <ConfigRow label="Enabled" value={c.enabled ? "Yes" : "No"} />
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Engine-aware edit form
// ---------------------------------------------------------------------------

function EngineEditForm({
  cluster: c,
  persisted,
  editClusterName,
  onChangeClusterName,
  editEnabled,
  editMaxInput,
  editFlat,
  onPatchFlat,
  onToggleEnabled,
  onChangeMaxInput,
  saveError,
  saving,
  onSave,
  onCancel,
}: {
  cluster: ClusterDisplayRow;
  persisted: ClusterConfigRecord;
  editClusterName: string;
  onChangeClusterName: (v: string) => void;
  editEnabled: boolean;
  editMaxInput: string;
  editFlat: Record<string, string>;
  onPatchFlat: (patch: Record<string, string>) => void;
  onToggleEnabled: () => void;
  onChangeMaxInput: (v: string) => void;
  saveError: string | null;
  saving: boolean;
  onSave: () => void;
  onCancel: () => void;
}) {
  const descriptor = findEngineDescriptor(persisted.engineKey);

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
          Edit configuration
        </p>
        {descriptor && (
          <span className="text-[10px] text-slate-400">{descriptor.displayName}</span>
        )}
      </div>

      <div className="bg-slate-50 rounded-xl border border-slate-100 divide-y divide-slate-100">
        <div className="flex flex-col px-4 py-3 gap-1.5">
          <label className="text-[11px] text-slate-600 font-medium" htmlFor="edit-cluster-name">
            Cluster name
          </label>
          <input
            id="edit-cluster-name"
            value={editClusterName}
            onChange={(e) => onChangeClusterName(e.target.value)}
            autoComplete="off"
            className="w-full text-sm font-mono bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          />
          <p className="text-[10px] text-slate-400">
            Renaming keeps the same stable id; group membership in Postgres is unchanged.
          </p>
        </div>
        <div className="flex items-center justify-between px-4 py-3">
          <div>
            <p className="text-[11px] text-slate-600 font-medium">Enabled</p>
            <p className="text-[10px] text-slate-400 mt-0.5">
              Disabled clusters receive no new queries
            </p>
          </div>
          <button
            type="button"
            onClick={onToggleEnabled}
            className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none flex-shrink-0 ${
              editEnabled ? "bg-emerald-500" : "bg-slate-300"
            }`}
          >
            <span
              className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${
                editEnabled ? "translate-x-4.5" : "translate-x-0.5"
              }`}
            />
          </button>
        </div>

        <div className="flex flex-col px-4 py-3 gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="min-w-0">
            <p className="text-[11px] text-slate-600 font-medium">
              Max concurrent queries
            </p>
            <p className="text-[10px] text-slate-400 mt-0.5">
              {c.configPending
                ? "Stored in Postgres. Empty = ∞ (inherit group limit when loaded)."
                : descriptor?.connectionType === "embedded"
                  ? "Empty = ∞ (use group default)."
                  : `Empty = ∞ (group default; effective now ${c.max_running_queries}).`}
            </p>
          </div>
          <input
            type="number"
            min={1}
            max={999999}
            value={editMaxInput}
            onChange={(e) => onChangeMaxInput(e.target.value)}
            placeholder="∞"
            className="w-full sm:w-28 text-right text-xs font-mono bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 flex-shrink-0"
          />
        </div>
      </div>

      <div className="mt-4 space-y-1">
        <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
          Connection
        </p>
        <p className="text-[10px] text-slate-500 mb-2">
          Saved to Postgres; the proxy reloads config and rebuilds adapters. Leave password or bearer
          token empty to keep the values already stored for this cluster.
        </p>
        {descriptor && persisted.engineKey ? (
          <EngineClusterConfig
            engineKey={persisted.engineKey}
            descriptor={descriptor}
            flat={editFlat}
            hiddenFieldKeys={
              persisted.engineKey === "adbc"
                ? (() => {
                    const hidden = new Set<string>();
                    const driverHidden = hiddenAdbcFieldKeysForDriver(editFlat.driver);
                    if (driverHidden) {
                      for (const k of driverHidden) hidden.add(k);
                    }
                    if (isAdbcPostgresqlDriver(editFlat)) {
                      hidden.add("username");
                      hidden.add("password");
                    }
                    return hidden.size ? hidden : undefined;
                  })()
                : undefined
            }
            onPatch={onPatchFlat}
          />
        ) : (
          <p className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
            Unknown engine key <code className="font-mono">{persisted.engineKey}</code> — add it to
            the Studio engine registry or edit via the Admin API.
          </p>
        )}
      </div>

      <VariantsSection
        persisted={persisted}
        baseClusterName={editClusterName}
        onPatchFlat={onPatchFlat}
      />

      {saveError && (
        <p className="mt-2 text-xs text-red-500 flex items-center gap-1">
          <AlertCircle size={11} /> {saveError}
        </p>
      )}

      <div className="flex gap-2 mt-3">
        <button
          type="button"
          onClick={onCancel}
          disabled={saving}
          className="flex-1 px-3 py-2 rounded-lg text-xs font-medium text-slate-600 bg-slate-100 hover:bg-slate-200 border border-slate-200 transition-colors disabled:opacity-50"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={onSave}
          disabled={saving || !descriptor}
          className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium text-white bg-indigo-600 hover:bg-indigo-700 transition-colors disabled:opacity-60"
        >
          {saving ? <Loader2 size={12} className="animate-spin" /> : null}
          {saving ? "Saving…" : "Save changes"}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Variants & custom queries section (shown on ADBC clusters)
// ---------------------------------------------------------------------------

function VariantsSection({
  persisted,
  baseClusterName,
  onPatchFlat: _onPatchFlat,
}: {
  persisted: ClusterConfigRecord;
  baseClusterName: string;
  onPatchFlat: (patch: Record<string, string>) => void;
}) {
  const isAdbc = persisted.engineKey === "adbc";
  const driver = ((persisted.config as Record<string, unknown>).driver as string) ?? "";
  const saasDriver = isSaasVariantDriver(driver);

  const [variantRows, setVariantRows] = useState(() =>
    saasDriver ? variantsToRows(persisted.variants, driver) : [],
  );
  const [variantsJson, setVariantsJson] = useState(() =>
    !saasDriver && persisted.variants && (persisted.variants as unknown[]).length > 0
      ? JSON.stringify(persisted.variants, null, 2)
      : "",
  );
  const [healthCheckQuery, setHealthCheckQuery] = useState(
    () => ((persisted.config as Record<string, unknown>).healthCheckQuery as string) ?? "",
  );
  const [reconcileQuery, setReconcileQuery] = useState(
    () => ((persisted.config as Record<string, unknown>).reconcileQuery as string) ?? "",
  );
  const [variantsError, setVariantsError] = useState<string | null>(null);
  const [variantFieldErrors, setVariantFieldErrors] = useState<string[]>([]);

  function syncSaasVariants(rows: typeof variantRows) {
    setVariantRows(rows);
    const errors = validateVariantRows(rows, driver);
    setVariantFieldErrors(errors);
    persisted.variants = rowsToVariants(rows, driver);
  }

  useEffect(() => {
    if (saasDriver) return;
    if (variantsJson.trim()) {
      try {
        const parsed = JSON.parse(variantsJson);
        if (!Array.isArray(parsed)) {
          setVariantsError("Variants must be a JSON array");
          return;
        }
        persisted.variants = parsed;
        setVariantsError(null);
      } catch {
        setVariantsError("Invalid JSON");
      }
    } else {
      persisted.variants = [];
      setVariantsError(null);
    }
  }, [variantsJson, persisted, saasDriver]);

  useEffect(() => {
    const cfg = persisted.config as Record<string, unknown>;
    if (healthCheckQuery.trim()) cfg.healthCheckQuery = healthCheckQuery.trim();
    else delete cfg.healthCheckQuery;
  }, [healthCheckQuery, persisted.config]);

  useEffect(() => {
    const cfg = persisted.config as Record<string, unknown>;
    if (reconcileQuery.trim()) cfg.reconcileQuery = reconcileQuery.trim();
    else delete cfg.reconcileQuery;
  }, [reconcileQuery, persisted.config]);

  if (!isAdbc) return null;

  return (
    <div className="mt-4 space-y-3">
      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
        {saasDriver ? "Warehouses & health" : "Variants & health"}
      </p>

      <div className="bg-slate-50 rounded-xl border border-slate-100 divide-y divide-slate-100">
        <div className="flex flex-col px-4 py-3 gap-1.5">
          {saasDriver ? (
            <AdbcSaasVariantsEditor
              driver={driver}
              baseClusterName={baseClusterName.trim() || persisted.name}
              rows={variantRows}
              onChange={syncSaasVariants}
              errors={variantFieldErrors}
            />
          ) : (
            <>
              <label className="text-[11px] text-slate-600 font-medium">
                Variants (JSON)
              </label>
              <textarea
                value={variantsJson}
                onChange={(e) => setVariantsJson(e.target.value)}
                placeholder={`[\n  { "name": "analytics", "overrides": { "warehouse": "ANALYTICS_WH" } }\n]`}
                rows={4}
                className="w-full text-xs font-mono bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 resize-y"
              />
              {variantsError && (
                <p className="text-[10px] text-red-500">{variantsError}</p>
              )}
              <p className="text-[10px] text-slate-400">
                Each variant expands into a separate cluster named{" "}
                <code className="font-mono">base::variant</code>.
              </p>
            </>
          )}
        </div>

        <AdbcHealthReconcileFields
          driver={driver}
          healthCheckQuery={healthCheckQuery}
          reconcileQuery={reconcileQuery}
          onHealthCheckQueryChange={setHealthCheckQuery}
          onReconcileQueryChange={setReconcileQuery}
        />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Connection type badge
// ---------------------------------------------------------------------------

function ConnectionTypeBadge({ type }: { type: ConnectionType }) {
  const map: Record<
    ConnectionType,
    { label: string; icon: React.ReactNode; className: string }
  > = {
    http: {
      label: "HTTP REST",
      icon: <Wifi size={10} />,
      className: "text-blue-600 bg-blue-50 border-blue-200",
    },
    mysqlWire: {
      label: "MySQL wire",
      icon: <Database size={10} />,
      className: "text-orange-600 bg-orange-50 border-orange-200",
    },
    embedded: {
      label: "In-process",
      icon: <Zap size={10} />,
      className: "text-violet-600 bg-violet-50 border-violet-200",
    },
    driver: {
      label: "ADBC driver",
      icon: <Database size={10} />,
      className: "text-indigo-600 bg-indigo-50 border-indigo-200",
    },
  };

  const { label, icon, className } = map[type];
  return (
    <span
      className={`flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border ${className}`}
    >
      {icon}
      {label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ConfigRow({
  label,
  value,
  mono,
  link,
}: {
  label: string;
  value: string;
  mono?: boolean;
  link?: string;
}) {
  return (
    <div className="flex items-start justify-between gap-3 px-4 py-2.5">
      <span className="text-[11px] text-slate-400 font-medium flex-shrink-0 pt-0.5">
        {label}
      </span>
      {link ? (
        <a
          href={link}
          target="_blank"
          rel="noopener noreferrer"
          className="text-[11px] font-mono text-indigo-600 hover:underline text-right truncate flex items-center gap-1"
        >
          {value}
          <ExternalLink size={10} className="flex-shrink-0" />
        </a>
      ) : (
        <span
          className={`text-[11px] text-right truncate ${mono ? "font-mono text-slate-700" : "text-slate-700"}`}
        >
          {value}
        </span>
      )}
    </div>
  );
}

function MiniStat({
  label,
  value,
  highlight,
  title,
}: {
  label: string;
  value: string;
  highlight?: boolean;
  title?: string;
}) {
  return (
    <div
      className="bg-slate-50 rounded-lg px-2.5 py-2 border border-slate-100 text-center"
      title={title}
    >
      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wide">
        {label}
      </p>
      <p
        className={`text-base font-bold mt-0.5 ${highlight ? "text-amber-600" : "text-slate-800"}`}
      >
        {value}
      </p>
    </div>
  );
}
