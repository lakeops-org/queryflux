"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  CATEGORY_ORDER,
  ENGINE_CATALOG,
  type EngineCategory,
  type EngineDef,
} from "@/components/engine-catalog";
import { EngineIcon } from "@/components/engine-icon";
import { AdbcHealthReconcileFields, AdbcSaasVariantsEditor, EngineClusterConfig } from "@/components/cluster-config";
import {
  isSaasVariantDriver,
  rowsToVariants,
  validateVariantRows,
  type VariantRow,
} from "@/lib/adbc-saas-variants";
import {
  findEngineDescriptor,
  isClusterOnboardingSelectable,
  validateClusterConfig,
} from "@/lib/engine-registry";
import {
  buildValidateShape,
  flatToPersistedConfig,
  isAdbcPostgresqlDriver,
  toUpsertBody,
  validateEngineSpecific,
} from "@/lib/cluster-persist-form";
import {
  hiddenAdbcFieldKeysForDriver,
} from "@/lib/adbc-driver-spec";
import { testClusterConfig, upsertClusterConfig } from "@/lib/api";
import type { TestClusterConfigResponse } from "@/lib/api";
import { AlertCircle, ArrowLeft, CheckCircle2, ChevronRight, Loader2, X } from "lucide-react";

type Props = {
  open: boolean;
  onClose: () => void;
};

type Step = 1 | 2;
type PickerEngine = EngineDef & {
  pickerKey: string;
  adbcDriver?: string;
};

const ADBC_DRIVER_VARIANTS: Array<{
  driver: string;
  name: string;
  category: EngineCategory;
  simpleIconSlug: string | null;
  hex: string;
}> = [
  // Official Arrow ADBC drivers (arrow.apache.org/adbc) + ADBC Driver Foundry (github.com/adbc-drivers).
  { driver: "trino", name: "Trino", category: "Lakehouse", simpleIconSlug: "siTrino", hex: "DD00A1" },
  { driver: "duckdb", name: "DuckDB", category: "Embedded", simpleIconSlug: "siDuckdb", hex: "FCC021" },
  { driver: "flightsql", name: "StarRocks", category: "Open Source OLAP", simpleIconSlug: null, hex: "A9334A" },
  { driver: "clickhouse", name: "ClickHouse", category: "Open Source OLAP", simpleIconSlug: "siClickhouse", hex: "FFCC01" },
  { driver: "mysql", name: "MySQL", category: "OLTP / General", simpleIconSlug: "siMysql", hex: "4479A1" },
  { driver: "postgresql", name: "PostgreSQL", category: "OLTP / General", simpleIconSlug: "siPostgresql", hex: "4169E1" },
  { driver: "sqlite", name: "SQLite", category: "Embedded", simpleIconSlug: "siSqlite", hex: "003B57" },
  { driver: "flightsql", name: "Flight SQL", category: "Other", simpleIconSlug: null, hex: "6366F1" },
  { driver: "snowflake", name: "Snowflake", category: "Cloud Warehouse", simpleIconSlug: "siSnowflake", hex: "29B5E8" },
  { driver: "bigquery", name: "BigQuery", category: "Cloud Warehouse", simpleIconSlug: null, hex: "4285F4" },
  { driver: "databricks", name: "Databricks", category: "Lakehouse", simpleIconSlug: "siDatabricks", hex: "FF3621" },
  { driver: "mssql", name: "SQL Server", category: "OLTP / General", simpleIconSlug: null, hex: "CC2927" },
  { driver: "redshift", name: "Redshift", category: "Cloud Warehouse", simpleIconSlug: null, hex: "8C4FFF" },
  { driver: "exasol", name: "Exasol", category: "Cloud Warehouse", simpleIconSlug: null, hex: "003A70" },
  { driver: "singlestore", name: "SingleStore", category: "Cloud Warehouse", simpleIconSlug: "siSinglestore", hex: "AA00FF" },
  { driver: "jdbc", name: "JDBC", category: "Other", simpleIconSlug: null, hex: "0EA5E9" },
];

function buildPickerEngines(): PickerEngine[] {
  const out: PickerEngine[] = [];
  for (const e of ENGINE_CATALOG) {
    if (e.engineKey === "adbc") {
      for (const v of ADBC_DRIVER_VARIANTS) {
        out.push({
          name: v.name,
          simpleIconSlug: v.simpleIconSlug,
          hex: v.hex,
          category: v.category,
          description: `Connect via ADBC driver: ${v.driver}`,
          engineKey: "adbc",
          supported: true,
          pickerKey: `adbc:${v.driver}:${v.name.toLowerCase().replace(/\s+/g, "-")}`,
          adbcDriver: v.driver,
        });
      }
      continue;
    }
    out.push({ ...e, pickerKey: `engine:${e.engineKey ?? e.name}` });
  }
  return out;
}

export function AddClusterDialog({ open, onClose }: Props) {
  const router = useRouter();
  const [step, setStep] = useState<Step>(1);
  const [selected, setSelected] = useState<PickerEngine | null>(null);
  const [clusterName, setClusterName] = useState("");
  const [flat, setFlat] = useState<Record<string, string>>({});
  const [clusterEnabled, setClusterEnabled] = useState(true);
  const [clusterMaxRunning, setClusterMaxRunning] = useState("");
  const [variantRows, setVariantRows] = useState<VariantRow[]>([]);
  const [healthCheckQuery, setHealthCheckQuery] = useState("");
  const [reconcileQuery, setReconcileQuery] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestClusterConfigResponse | null>(null);

  const descriptor = useMemo(
    () => (selected?.engineKey ? findEngineDescriptor(selected.engineKey) : undefined),
    [selected],
  );
  const pickerEngines = useMemo(() => buildPickerEngines(), []);

  const reset = useCallback(() => {
    setStep(1);
    setSelected(null);
    setClusterName("");
    setFlat({});
    setClusterEnabled(true);
    setClusterMaxRunning("");
    setVariantRows([]);
    setHealthCheckQuery("");
    setReconcileQuery("");
    setSaving(false);
    setSaveError(null);
    setTesting(false);
    setTestResult(null);
  }, []);

  useEffect(() => {
    if (!open) reset();
  }, [open, reset]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  function goNext() {
    if (!selected || !isClusterOnboardingSelectable(selected)) return;
    setSaveError(null);
    const d = findEngineDescriptor(selected.engineKey ?? "");
    const initial: Record<string, string> = {};
    if (d) {
      for (const f of d.configFields) {
        if (f.key === "auth.type" && d.supportedAuth.length === 1) {
          initial[f.key] = d.supportedAuth[0];
        } else {
          initial[f.key] = "";
        }
      }
    }
    if (selected.adbcDriver) {
      initial.driver = selected.adbcDriver;
    }
    setFlat(initial);
    setVariantRows([]);
    setHealthCheckQuery("");
    setReconcileQuery("");
    setStep(2);
  }

  function goBack() {
    setStep(1);
    setSaveError(null);
    setTestResult(null);
  }

  async function handleTest() {
    if (!selected?.engineKey) return;
    setTesting(true);
    setTestResult(null);
    setSaveError(null);
    try {
      const config = flatToPersistedConfig(flat);
      const result = await testClusterConfig(selected.engineKey, config);
      setTestResult(result);
    } catch (e) {
      setTestResult({ ok: false, message: e instanceof Error ? e.message : "Test failed" });
    } finally {
      setTesting(false);
    }
  }

  async function handleSave() {
    if (!selected?.engineKey || !descriptor) return;
    const name = clusterName.trim();
    if (!name) {
      setSaveError("Cluster name is required.");
      return;
    }
    const specific = validateEngineSpecific(selected.engineKey, flat);
    if (specific.length > 0) {
      setSaveError(specific.join(" "));
      return;
    }
    const validatePayload = buildValidateShape(flat);
    const errs = validateClusterConfig(name, selected.engineKey, validatePayload);
    if (errs.length > 0) {
      setSaveError(errs.join(" "));
      return;
    }
    const maxTrim = clusterMaxRunning.trim();
    if (maxTrim !== "") {
      const n = Number.parseInt(maxTrim, 10);
      if (!Number.isFinite(n) || n < 1 || String(n) !== maxTrim) {
        setSaveError("Max concurrent queries must be a positive integer.");
        return;
      }
    }
    const adbcDriver = selected.adbcDriver ?? flat.driver ?? "";
    if (isSaasVariantDriver(adbcDriver)) {
      const variantErrs = validateVariantRows(variantRows, adbcDriver);
      if (variantErrs.length > 0) {
        setSaveError(variantErrs.join(" "));
        return;
      }
    }
    setSaving(true);
    setSaveError(null);
    try {
      await upsertClusterConfig(
        name,
        toUpsertBody(selected.engineKey, flat, {
          enabled: clusterEnabled,
          maxRunningQueriesInput: clusterMaxRunning,
          variants: isSaasVariantDriver(adbcDriver)
            ? rowsToVariants(variantRows, adbcDriver)
            : undefined,
          healthCheckQuery,
          reconcileQuery,
        }),
      );
      reset();
      onClose();
      router.refresh();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : "Failed to save cluster");
    } finally {
      setSaving(false);
    }
  }

  if (!open) return null;

  const canProceedStep1 = !!selected && isClusterOnboardingSelectable(selected);
  const unsupported =
    selected &&
    (!selected.engineKey ||
      !descriptor ||
      !descriptor.implemented);

  const adbcDriver = selected?.adbcDriver ?? flat.driver ?? "";
  const saasAdbc = isSaasVariantDriver(adbcDriver);
  const isAdbc = selected?.engineKey === "adbc";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm"
        onClick={onClose}
        aria-hidden
      />

      <div
        role="dialog"
        aria-modal
        aria-labelledby="add-cluster-title"
        className={`relative w-full ${
          step === 1 ? "max-w-6xl" : isAdbc ? "max-w-2xl" : "max-w-lg"
        } max-h-[min(92vh,900px)] overflow-hidden flex flex-col bg-white rounded-2xl shadow-2xl border border-slate-200`}
      >
        {/* Header */}
        <div className="flex items-start justify-between px-6 py-4 border-b border-slate-100 flex-shrink-0">
          <div className="flex items-start gap-3 min-w-0">
            {step === 2 && (
              <button
                type="button"
                onClick={goBack}
                className="mt-0.5 w-8 h-8 rounded-lg flex items-center justify-center text-slate-500 hover:text-indigo-600 hover:bg-indigo-50 transition-colors flex-shrink-0"
                aria-label="Back"
              >
                <ArrowLeft size={18} />
              </button>
            )}
            <div>
              <p className="text-[10px] font-semibold text-indigo-600 uppercase tracking-widest">
                {step === 1 ? "Step 1 of 2" : "Step 2 of 2"}
              </p>
              <h2 id="add-cluster-title" className="text-base font-bold text-slate-900">
                {step === 1 ? "Choose cluster type" : "Configure cluster"}
              </h2>
              <p className="text-xs text-slate-500 mt-1">
                {step === 1
                  ? "Select an engine. Config is persisted to Postgres and the proxy reloads it automatically."
                  : "Enter a unique cluster name and connection details."}
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="w-8 h-8 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors flex-shrink-0"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto flex-1 min-h-0">
          {step === 1 ? (
            <div className="px-6 py-5 space-y-6">
              <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:gap-3">
                <h3 className="text-sm font-semibold text-slate-700">Engines</h3>
                <div className="hidden sm:block flex-1 h-px bg-slate-100" />
                <span className="text-xs text-slate-400">
                  {
                    pickerEngines.filter((e) => isClusterOnboardingSelectable(e))
                      .length
                  }{" "}
                  supported · {pickerEngines.length} total
                </span>
              </div>
              <p className="text-[11px] text-slate-500 -mt-2">
                Only engines marked as supported can be selected. Others show{" "}
                <span className="font-medium text-slate-600">Not supported yet</span>.
              </p>

              <div>
                <p className="text-[11px] font-semibold text-emerald-700 uppercase tracking-widest mb-3">
                  Supported
                </p>
                <div className="space-y-5">
                  {CATEGORY_ORDER.map((category) => {
                    const engines = pickerEngines.filter(
                      (e) => e.category === category && isClusterOnboardingSelectable(e),
                    );
                    if (engines.length === 0) return null;
                    return (
                      <div key={`supported-${category}`}>
                        <p className="text-[11px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
                          {category}
                        </p>
                        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
                          {engines.map((e) => {
                            const isSel = selected?.pickerKey === e.pickerKey;
                            return (
                              <button
                                key={e.pickerKey}
                                type="button"
                                onClick={() => setSelected(e)}
                                className={`rounded-xl border p-4 flex items-center gap-3 text-left transition-all duration-150 focus:outline-none ${
                                  isSel
                                    ? "border-indigo-500 bg-indigo-50/50 shadow-sm ring-1 ring-indigo-200 focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2"
                                    : "border-slate-200 bg-white hover:border-slate-300 hover:shadow-xs focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2"
                                }`}
                              >
                                <EngineIcon engine={e} size={36} />
                                <div className="min-w-0 flex-1">
                                  <div className="flex items-start justify-between gap-2">
                                    <p className="text-sm font-semibold leading-tight truncate text-slate-800">
                                      {e.name}
                                    </p>
                                    {e.adbcDriver && (
                                      <span className="flex-shrink-0 text-[9px] font-semibold uppercase tracking-wide text-indigo-800 bg-indigo-100 border border-indigo-200/80 px-1.5 py-0.5 rounded-md">
                                        ADBC
                                      </span>
                                    )}
                                  </div>
                                  <p className="text-[11px] text-slate-400 mt-0.5 leading-tight line-clamp-2">
                                    {e.description}
                                  </p>
                                </div>
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div>
                <p className="text-[11px] font-semibold text-amber-700 uppercase tracking-widest mb-3">
                  Not supported yet
                </p>
                <div className="space-y-5">
                  {CATEGORY_ORDER.map((category) => {
                    const engines = pickerEngines.filter(
                      (e) => e.category === category && !isClusterOnboardingSelectable(e),
                    );
                    if (engines.length === 0) return null;
                    return (
                      <div key={`unsupported-${category}`}>
                        <p className="text-[11px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
                          {category}
                        </p>
                        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
                          {engines.map((e) => (
                            <button
                              key={e.pickerKey}
                              type="button"
                              disabled
                              aria-disabled
                              title="Not supported yet — no QueryFlux adapter for this engine in Studio."
                              className="rounded-xl border p-4 flex items-center gap-3 text-left transition-all duration-150 focus:outline-none border-slate-100 bg-slate-50/80 cursor-not-allowed opacity-75 grayscale-[0.35]"
                            >
                              <EngineIcon engine={e} size={36} />
                              <div className="min-w-0 flex-1">
                                <div className="flex items-start justify-between gap-2">
                                  <p className="text-sm font-semibold leading-tight truncate text-slate-500">
                                    {e.name}
                                  </p>
                                  {e.adbcDriver && (
                                    <span className="flex-shrink-0 text-[9px] font-semibold uppercase tracking-wide text-indigo-800 bg-indigo-100 border border-indigo-200/80 px-1.5 py-0.5 rounded-md">
                                      ADBC
                                    </span>
                                  )}
                                  <span className="flex-shrink-0 text-[9px] font-semibold uppercase tracking-wide text-amber-800 bg-amber-100 border border-amber-200/80 px-1.5 py-0.5 rounded-md">
                                    Not supported yet
                                  </span>
                                </div>
                                <p className="text-[11px] text-slate-400 mt-0.5 leading-tight line-clamp-2">
                                  {e.description}
                                </p>
                              </div>
                            </button>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          ) : (
            <div className="px-6 py-5 space-y-5">
              {selected && (
                <div className="flex items-center gap-3 p-3 rounded-xl bg-slate-50 border border-slate-100">
                  <EngineIcon engine={selected} size={40} />
                  <div>
                    <p className="text-sm font-semibold text-slate-800">{selected.name}</p>
                    {selected.adbcDriver && (
                      <p className="text-[11px] text-indigo-700">ADBC driver: {selected.adbcDriver}</p>
                    )}
                    {selected.engineKey && (
                      <p className="text-[11px] font-mono text-indigo-600">{selected.engineKey}</p>
                    )}
                  </div>
                </div>
              )}

              {unsupported ? (
                <div className="flex gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                  <AlertCircle className="flex-shrink-0 mt-0.5" size={18} />
                  <div>
                    <p className="font-medium">Not available yet</p>
                    <p className="text-xs text-amber-800/90 mt-1">
                      {!selected?.engineKey
                        ? "This engine has no QueryFlux backend key yet."
                        : !descriptor
                          ? "Unknown engine in registry."
                          : "This engine is listed in the catalog but the adapter is not implemented. Use YAML or the API when support lands."}
                    </p>
                  </div>
                </div>
              ) : (
                <>
                  <div>
                    <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                      Cluster name <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={clusterName}
                      onChange={(e) => setClusterName(e.target.value)}
                      placeholder="e.g. trino-prod"
                      className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
                      autoComplete="off"
                    />
                    <p className="text-[10px] text-slate-400 mt-1">
                      Unique key in config; maps to <code className="font-mono">clusters.&lt;name&gt;</code>
                    </p>
                  </div>

                  <div className="rounded-xl border border-slate-100 bg-slate-50/70 p-4 space-y-4">
                    <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                      Routing
                    </p>
                    <div className="flex items-center justify-between gap-4">
                      <div>
                        <p className="text-sm font-medium text-slate-700">
                          Cluster enabled
                        </p>
                        <p className="text-[11px] text-slate-400 mt-0.5">
                          When disabled, the proxy skips this cluster when routing.
                        </p>
                      </div>
                      <button
                        type="button"
                        role="switch"
                        aria-checked={clusterEnabled}
                        aria-label="Cluster enabled"
                        onClick={() => setClusterEnabled((v) => !v)}
                        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 focus-visible:ring-offset-2 flex-shrink-0 ${
                          clusterEnabled ? "bg-emerald-500" : "bg-slate-300"
                        }`}
                      >
                        <span
                          className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${
                            clusterEnabled ? "translate-x-4.5" : "translate-x-0.5"
                          }`}
                        />
                      </button>
                    </div>
                    <div>
                      <label
                        htmlFor="add-cluster-max-running"
                        className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
                      >
                        Max concurrent queries
                      </label>
                      <input
                        id="add-cluster-max-running"
                        type="number"
                        min={1}
                        inputMode="numeric"
                        value={clusterMaxRunning}
                        onChange={(e) => setClusterMaxRunning(e.target.value)}
                        placeholder="∞ group default"
                        className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
                      />
                      <p className="text-[10px] text-slate-400 mt-1">
                        Leave empty for ∞ — no per-cluster cap; the cluster group&apos;s limit applies.
                      </p>
                    </div>
                  </div>

                  {selected && descriptor && selected.engineKey ? (
                    <div className="space-y-4">
                      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                        Connection
                      </p>
                      <EngineClusterConfig
                        engineKey={selected.engineKey}
                        descriptor={descriptor}
                        flat={flat}
                        readOnlyFieldKeys={
                          selected.adbcDriver
                            ? new Set<string>(
                                ["driver"],
                              )
                            : undefined
                        }
                        hiddenFieldKeys={
                          selected.engineKey === "adbc"
                            ? (() => {
                                const hidden = new Set<string>();
                                // Per-driver hiding (e.g. auth in URI).
                                const driverHidden = hiddenAdbcFieldKeysForDriver(
                                  selected.adbcDriver ?? flat.driver,
                                );
                                if (driverHidden) {
                                  for (const k of driverHidden) hidden.add(k);
                                }
                                // Back-compat explicit check.
                                if (
                                  selected.adbcDriver === "postgresql" ||
                                  isAdbcPostgresqlDriver(flat)
                                ) {
                                  hidden.add("username");
                                  hidden.add("password");
                                }
                                return hidden.size ? hidden : undefined;
                              })()
                            : undefined
                        }
                        onPatch={(patch) =>
                          setFlat((prev) => ({ ...prev, ...patch }))
                        }
                      />
                    </div>
                  ) : null}

                  {isAdbc ? (
                    <div className="space-y-2 pt-2 border-t border-slate-100">
                      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                        {saasAdbc ? "Warehouses & health" : "Health & reconcile"}
                      </p>
                      <div className="bg-slate-50 rounded-xl border border-slate-100 divide-y divide-slate-100">
                        {saasAdbc ? (
                          <div className="flex flex-col px-4 py-3 gap-1.5">
                            <AdbcSaasVariantsEditor
                              driver={adbcDriver}
                              baseClusterName={clusterName.trim()}
                              rows={variantRows}
                              onChange={setVariantRows}
                              errors={validateVariantRows(variantRows, adbcDriver)}
                            />
                          </div>
                        ) : null}
                        <AdbcHealthReconcileFields
                          driver={adbcDriver}
                          healthCheckQuery={healthCheckQuery}
                          reconcileQuery={reconcileQuery}
                          onHealthCheckQueryChange={setHealthCheckQuery}
                          onReconcileQueryChange={setReconcileQuery}
                        />
                      </div>
                    </div>
                  ) : null}
                </>
              )}

              {saveError && (
                <p className="text-xs text-red-600 flex items-start gap-1.5">
                  <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
                  {saveError}
                </p>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex flex-col gap-2 px-6 py-4 border-t border-slate-100 bg-slate-50/80 flex-shrink-0">
          {step === 2 && testResult && (
            <div
              className={`flex items-start gap-2 rounded-lg border px-3 py-2.5 text-xs ${
                testResult.ok
                  ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                  : "border-red-200 bg-red-50 text-red-700"
              }`}
            >
              {testResult.ok ? (
                <CheckCircle2 size={14} className="flex-shrink-0 mt-0.5" />
              ) : (
                <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
              )}
              {testResult.message}
            </div>
          )}
        <div className="flex items-center justify-end gap-2">
          {step === 1 ? (
            <>
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 rounded-lg text-sm font-medium text-slate-600 hover:bg-slate-100 transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={!canProceedStep1}
                onClick={goNext}
                className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Next
                <ChevronRight size={16} />
              </button>
            </>
          ) : (
            <>
              {!unsupported && (
                <>
                  <button
                    type="button"
                    onClick={handleTest}
                    disabled={saving || testing}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-slate-700 bg-white border border-slate-200 hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    {testing ? <Loader2 size={16} className="animate-spin" /> : null}
                    {testing ? "Testing…" : "Test connection"}
                  </button>
                  <button
                    type="button"
                    onClick={handleSave}
                    disabled={saving || testing || !clusterName.trim()}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    {saving ? <Loader2 size={16} className="animate-spin" /> : null}
                    {saving ? "Saving…" : "Save cluster"}
                  </button>
                </>
              )}
            </>
          )}
        </div>
        </div>
      </div>
    </div>
  );
}
