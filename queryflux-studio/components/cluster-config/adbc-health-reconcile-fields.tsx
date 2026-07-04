"use client";

import { isSaasVariantDriver } from "@/lib/adbc-saas-variants";
import {
  healthCheckQueryPlaceholder,
  reconcileQueryPlaceholder,
} from "@/lib/adbc-probe-defaults";

type Props = {
  driver: string;
  healthCheckQuery: string;
  reconcileQuery: string;
  onHealthCheckQueryChange: (value: string) => void;
  onReconcileQueryChange: (value: string) => void;
};

export function AdbcHealthReconcileFields({
  driver,
  healthCheckQuery,
  reconcileQuery,
  onHealthCheckQueryChange,
  onReconcileQueryChange,
}: Props) {
  const saasDriver = isSaasVariantDriver(driver);

  return (
    <>
      <div className="flex flex-col px-4 py-3 gap-1.5">
        <label className="text-[11px] text-slate-600 font-medium">Health check query</label>
        <input
          type="text"
          value={healthCheckQuery}
          onChange={(e) => onHealthCheckQueryChange(e.target.value)}
          placeholder={healthCheckQueryPlaceholder(driver, saasDriver)}
          className="w-full text-xs font-mono bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
        />
          <p className="text-[10px] text-slate-400">
            Optional override. Use <code className="font-mono">{"{{sub_resource}}"}</code> for the
            variant warehouse / project name. When left empty, QueryFlux applies built-in defaults
            per driver (same as the placeholder).
          {driver === "snowflake" &&
            " Default: SHOW WAREHOUSES via built-in introspection (no warehouse resume)."}
          {driver === "databricks" &&
            " Default: Databricks REST API (does not resume the warehouse)."}
          {driver === "bigquery" &&
            " Default: always healthy; reconcile uses JOBS_BY_PROJECT metadata."}
          {driver === "redshift" &&
            " Default: always healthy; reconcile uses stv_recents."}
        </p>
      </div>

      <div className="flex flex-col px-4 py-3 gap-1.5">
        <label className="text-[11px] text-slate-600 font-medium">Reconcile query</label>
        <input
          type="text"
          value={reconcileQuery}
          onChange={(e) => onReconcileQueryChange(e.target.value)}
          placeholder={reconcileQueryPlaceholder(driver, saasDriver)}
          className="w-full text-xs font-mono bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
        />
          <p className="text-[10px] text-slate-400">
            Optional override — must return a single integer (or Snowflake SHOW `running` column).
            When left empty, QueryFlux applies built-in defaults per driver (same as the placeholder).
        </p>
      </div>
    </>
  );
}
