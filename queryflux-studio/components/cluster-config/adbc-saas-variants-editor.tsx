"use client";

import { useMemo } from "react";
import { Plus, Trash2 } from "lucide-react";
import {
  expandedClusterNames,
  newVariantRow,
  saasVariantsSectionTitle,
  subResourceFieldSpec,
  type VariantRow,
} from "@/lib/adbc-saas-variants";

type Props = {
  driver: string;
  baseClusterName?: string;
  rows: VariantRow[];
  onChange: (rows: VariantRow[]) => void;
  errors?: string[];
};

export function AdbcSaasVariantsEditor({
  driver,
  baseClusterName,
  rows,
  onChange,
  errors = [],
}: Props) {
  const spec = subResourceFieldSpec(driver);
  const title = saasVariantsSectionTitle(driver);
  const expanded = useMemo(
    () => (baseClusterName ? expandedClusterNames(baseClusterName, rows) : []),
    [baseClusterName, rows],
  );

  function updateRow(id: string, patch: Partial<VariantRow>) {
    onChange(rows.map((r) => (r.id === id ? { ...r, ...patch } : r)));
  }

  function removeRow(id: string) {
    onChange(rows.filter((r) => r.id !== id));
  }

  function addRow() {
    onChange([...rows, newVariantRow()]);
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <label className="text-[11px] text-slate-600 font-medium">{title}</label>
        <button
          type="button"
          onClick={addRow}
          className="inline-flex items-center gap-1 text-[11px] font-semibold text-indigo-600 hover:text-indigo-800"
        >
          <Plus size={12} />
          Add
        </button>
      </div>

      {rows.length === 0 ? (
        <p className="text-[10px] text-slate-400 rounded-lg border border-dashed border-slate-200 bg-white px-3 py-3 text-center">
          No {title.toLowerCase()} configured — the base cluster connection is used as-is.
          Add entries to expose each as a separate runtime cluster (
          <code className="font-mono">base::name</code>).
        </p>
      ) : (
        <ul className="space-y-2">
          {rows.map((row, idx) => (
            <li
              key={row.id}
              className="grid grid-cols-1 sm:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_minmax(0,0.7fr)_auto] gap-2 items-start bg-white border border-slate-200 rounded-lg p-2.5"
            >
              <div className="flex flex-col gap-1">
                <span className="text-[10px] text-slate-400 font-medium">Variant name</span>
                <input
                  type="text"
                  value={row.name}
                  onChange={(e) => updateRow(row.id, { name: e.target.value })}
                  placeholder={idx === 0 ? "analytics" : "etl"}
                  className="w-full text-xs font-mono border border-slate-200 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                />
              </div>
              {spec ? (
                <div className="flex flex-col gap-1">
                  <span className="text-[10px] text-slate-400 font-medium">{spec.label}</span>
                  <input
                    type="text"
                    value={row.subResource}
                    onChange={(e) => updateRow(row.id, { subResource: e.target.value })}
                    placeholder={spec.placeholder}
                    className="w-full text-xs font-mono border border-slate-200 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                  />
                </div>
              ) : (
                <div className="flex flex-col gap-1 justify-center">
                  <span className="text-[10px] text-slate-400">
                    Uses base connection settings
                  </span>
                </div>
              )}
              <div className="flex flex-col gap-1">
                <span className="text-[10px] text-slate-400 font-medium">Max queries</span>
                <input
                  type="number"
                  min={1}
                  value={row.maxRunningQueries}
                  onChange={(e) => updateRow(row.id, { maxRunningQueries: e.target.value })}
                  placeholder="∞"
                  className="w-full text-xs font-mono border border-slate-200 rounded-lg px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                />
              </div>
              <button
                type="button"
                onClick={() => removeRow(row.id)}
                className="self-end sm:self-center p-1.5 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50"
                aria-label="Remove variant"
              >
                <Trash2 size={14} />
              </button>
            </li>
          ))}
        </ul>
      )}

      {errors.length > 0 && (
        <ul className="text-[10px] text-red-500 space-y-0.5">
          {errors.map((e) => (
            <li key={e}>{e}</li>
          ))}
        </ul>
      )}

      <p className="text-[10px] text-slate-400 leading-relaxed">
        Each row becomes an independent cluster for routing and capacity limits.
        {spec && (
          <>
            {" "}
            The {spec.label.toLowerCase()} override is mapped to the ADBC driver option
            automatically.
          </>
        )}
        {expanded.length > 0 && baseClusterName && (
          <>
            {" "}
            Add to group members:{" "}
            <code className="font-mono text-[10px]">{expanded.join(", ")}</code>
          </>
        )}
      </p>
    </div>
  );
}
