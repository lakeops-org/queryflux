"use client";

import { useCallback, useMemo, useState } from "react";
import type { ConfigField } from "@/lib/engine-registry";
import { Plus, Trash2 } from "lucide-react";

type KvRow = { key: string; value: string };

/** Studio-only: preserves empty key/value rows in the flat string (stripped before API). */
const META_ROW_COUNT = "__qf_kv_slots";

function parseKvRows(s: string): KvRow[] {
  const t = s.trim();
  if (!t) return [{ key: "", value: "" }];
  try {
    const p = JSON.parse(t) as unknown;
    if (!p || typeof p !== "object" || Array.isArray(p)) {
      return [{ key: "", value: "" }];
    }
    const raw = p as Record<string, unknown>;
    const slotRaw = raw[META_ROW_COUNT];
    const slotCount =
      typeof slotRaw === "string"
        ? Number.parseInt(slotRaw, 10)
        : typeof slotRaw === "number" && Number.isFinite(slotRaw)
          ? Math.trunc(slotRaw)
          : 0;

    const entries = Object.entries(raw).filter(([k]) => k !== META_ROW_COUNT && !k.startsWith("__qf_"));
    if (entries.length === 0 && slotCount < 1) {
      return [{ key: "", value: "" }];
    }

    const rows: KvRow[] = entries.map(([k, v]) => ({
      key: k,
      value: typeof v === "string" ? v : JSON.stringify(v),
    }));

    if (slotCount > rows.length) {
      while (rows.length < slotCount) {
        rows.push({ key: "", value: "" });
      }
    } else if (slotCount === 0 && rows.length === 0) {
      return [{ key: "", value: "" }];
    }

    return rows.length ? rows : [{ key: "", value: "" }];
  } catch {
    return [{ key: "", value: "" }];
  }
}

function rowsToJsonString(rows: KvRow[]): string {
  if (rows.length === 1 && !rows[0].key.trim() && !rows[0].value) {
    return "";
  }

  const o: Record<string, string> = {};
  for (const r of rows) {
    const k = r.key.trim();
    if (!k) continue;
    o[k] = r.value;
  }

  if (rows.length > 1) {
    o[META_ROW_COUNT] = String(rows.length);
  }

  return JSON.stringify(o, null, 2);
}

function inferInitialMode(v: string): "kv" | "json" {
  const t = v.trim();
  if (!t) return "kv";
  try {
    const p = JSON.parse(t) as unknown;
    if (p && typeof p === "object" && !Array.isArray(p)) {
      return "kv";
    }
  } catch {
    return "json";
  }
  return "json";
}

export function DbKwargsField({
  id,
  field,
  value,
  onChange,
  readOnly,
  driverHint,
  dbKwargsError,
}: {
  id: string;
  field: ConfigField;
  value: string;
  onChange: (v: string) => void;
  readOnly?: boolean;
  driverHint: string;
  dbKwargsError: string | null;
}) {
  const [userMode, setUserMode] = useState<"kv" | "json" | null>(null);
  const mode = userMode ?? (value.trim() ? inferInitialMode(value) : "kv");

  const rows = useMemo(() => parseKvRows(value), [value]);

  const commitRows = useCallback(
    (next: KvRow[]) => {
      onChange(rowsToJsonString(next));
    },
    [onChange],
  );

  const updateRow = useCallback(
    (index: number, patch: Partial<KvRow>) => {
      const next = rows.map((r, i) => (i === index ? { ...r, ...patch } : r));
      commitRows(next);
    },
    [rows, commitRows],
  );

  const addRow = useCallback(() => {
    commitRows([...rows, { key: "", value: "" }]);
  }, [rows, commitRows]);

  const removeRow = useCallback(
    (index: number) => {
      const next = rows.filter((_, i) => i !== index);
      commitRows(next.length ? next : [{ key: "", value: "" }]);
    },
    [rows, commitRows],
  );

  const switchToJson = useCallback(() => {
    setUserMode("json");
  }, []);

  const switchToKv = useCallback(() => {
    setUserMode("kv");
  }, []);

  return (
    <div>
      <div className="flex items-center justify-between gap-2 mb-2">
        <p className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide">
          {field.label}
        </p>
        <div className="flex rounded-lg border border-slate-200 p-0.5 bg-slate-50/80">
          <button
            type="button"
            disabled={readOnly}
            onClick={switchToKv}
            className={`px-2 py-1 text-[10px] font-semibold rounded-md transition-colors ${
              mode === "kv"
                ? "bg-white text-slate-800 shadow-sm"
                : "text-slate-500 hover:text-slate-700"
            }`}
          >
            Key / value
          </button>
          <button
            type="button"
            disabled={readOnly}
            onClick={switchToJson}
            className={`px-2 py-1 text-[10px] font-semibold rounded-md transition-colors ${
              mode === "json"
                ? "bg-white text-slate-800 shadow-sm"
                : "text-slate-500 hover:text-slate-700"
            }`}
          >
            Raw JSON
          </button>
        </div>
      </div>

      {mode === "kv" ? (
        <div className="space-y-2">
          {rows.map((row, index) => (
            <div key={index} className="flex gap-2 items-start">
              <input
                type="text"
                disabled={readOnly}
                value={row.key}
                onChange={(e) => updateRow(index, { key: e.target.value })}
                placeholder="Option name"
                className="flex-1 min-w-0 text-sm border border-slate-200 rounded-lg px-2.5 py-1.5 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
                autoComplete="off"
              />
              <input
                type="text"
                disabled={readOnly}
                value={row.value}
                onChange={(e) => updateRow(index, { value: e.target.value })}
                placeholder="Value"
                className="flex-1 min-w-0 text-sm border border-slate-200 rounded-lg px-2.5 py-1.5 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
                autoComplete="off"
              />
              <button
                type="button"
                disabled={readOnly || rows.length <= 1}
                onClick={() => removeRow(index)}
                className="flex-shrink-0 p-1.5 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50 disabled:opacity-30 disabled:pointer-events-none"
                aria-label="Remove row"
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
          <button
            type="button"
            disabled={readOnly}
            onClick={addRow}
            className="inline-flex items-center gap-1 text-[11px] font-medium text-indigo-600 hover:text-indigo-800 disabled:opacity-40"
          >
            <Plus size={14} />
            Add option
          </button>
        </div>
      ) : (
        <textarea
          id={id}
          value={value}
          disabled={readOnly}
          onChange={(e) => onChange(e.target.value)}
          onBlur={(e) => {
            const raw = e.target.value.trim();
            if (!raw) return;
            try {
              const parsed = JSON.parse(raw) as unknown;
              if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
              onChange(JSON.stringify(parsed, null, 2));
            } catch {
              // keep input; error line below
            }
          }}
          rows={5}
          spellCheck={false}
          placeholder={field.example}
          className={`w-full text-sm border rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400 ${
            dbKwargsError ? "border-amber-300 bg-amber-50/40" : "border-slate-200"
          }`}
        />
      )}

      <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
      <p className="text-[10px] text-slate-500 mt-1">{driverHint}</p>
      {dbKwargsError && <p className="text-[10px] text-amber-700 mt-1">{dbKwargsError}</p>}
    </div>
  );
}
