"use client";

import React, { useState } from "react";
import { putCatalogProviderConfig, testCatalogProviderConfig } from "@/lib/api";
import type { CatalogProviderConfig, StaticColumnDefDto, StaticTableSchemaDto } from "@/lib/api-types";
import { Field, SectionHeader, TextInput, SaveBar } from "@/components/studio-settings";
import { Database, Plus, Trash2 } from "lucide-react";

interface Props {
  initialConfig: CatalogProviderConfig | null;
}

type ProviderType = CatalogProviderConfig["type"];

const DEFAULTS: Record<ProviderType, CatalogProviderConfig> = {
  null: { type: "null" },
  static: { type: "static", schemas: [] },
  engineDelegate: { type: "engineDelegate", clusterGroup: "" },
  hiveMetastore: { type: "hiveMetastore", uri: "" },
  glue: { type: "glue", region: null },
  caching: {
    type: "caching",
    ttlSeconds: 300,
    maxEntries: 10000,
    delegate: { type: "null" },
  },
  fallback: {
    type: "fallback",
    primary: { type: "null" },
    secondary: { type: "null" },
  },
};

const TYPE_LABELS: Record<ProviderType, string> = {
  null: "None",
  static: "Static (literal schema)",
  engineDelegate: "Engine delegate",
  hiveMetastore: "Hive Metastore",
  glue: "AWS Glue",
  caching: "Caching (wraps another provider)",
  fallback: "Fallback (primary → secondary)",
};

// Declared in config, but the backend doesn't have a real implementation for
// these yet — they build successfully and degrade to a no-op provider rather
// than failing, per queryflux_catalog::build_catalog_provider.
const UNIMPLEMENTED = new Set<ProviderType>(["engineDelegate", "hiveMetastore", "glue"]);

// ---------------------------------------------------------------------------
// Static schema editor — one row per table, columns as a "name:TYPE" shorthand
// (comma-separated; nullable always defaults to true through this UI — edit
// the persisted JSON directly via the admin API if you need nullable: false).
// ---------------------------------------------------------------------------

function parseColumns(text: string): StaticColumnDefDto[] {
  return text
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((pair) => {
      const [name, dataType] = pair.split(":").map((s) => s.trim());
      return { name: name || pair, dataType: dataType || "VARCHAR", nullable: true };
    });
}

function columnsToText(columns: StaticColumnDefDto[]): string {
  return columns.map((c) => `${c.name}:${c.dataType}`).join(", ");
}

function StaticSchemasEditor({
  schemas,
  onChange,
}: {
  schemas: StaticTableSchemaDto[];
  onChange: (v: StaticTableSchemaDto[]) => void;
}) {
  const update = (idx: number, patch: Partial<StaticTableSchemaDto>) => {
    const next = schemas.slice();
    next[idx] = { ...next[idx], ...patch };
    onChange(next);
  };
  const remove = (idx: number) => onChange(schemas.filter((_, i) => i !== idx));
  const add = () =>
    onChange([...schemas, { catalog: "", database: "", table: "", columns: [] }]);

  return (
    <div className="space-y-3">
      {schemas.length === 0 && <p className="text-xs text-slate-400">No tables declared yet.</p>}
      {schemas.map((schema, idx) => (
        <div key={idx} className="border border-slate-200 rounded-lg p-3 space-y-2">
          <div className="flex items-start justify-between gap-2">
            <div className="grid grid-cols-3 gap-2 flex-1">
              <TextInput
                label="Catalog"
                value={schema.catalog}
                onChange={(v) => update(idx, { catalog: v })}
              />
              <TextInput
                label="Database"
                value={schema.database}
                onChange={(v) => update(idx, { database: v })}
              />
              <TextInput
                label="Table"
                value={schema.table}
                onChange={(v) => update(idx, { table: v })}
              />
            </div>
            <button
              type="button"
              onClick={() => remove(idx)}
              className="mt-5 text-slate-400 hover:text-red-600"
              title="Remove table"
            >
              <Trash2 size={14} />
            </button>
          </div>
          <TextInput
            label="Columns (name:TYPE, comma-separated)"
            value={columnsToText(schema.columns)}
            onChange={(v) => update(idx, { columns: parseColumns(v) })}
            placeholder="order_id:BIGINT, total:DECIMAL(10,2)"
          />
        </div>
      ))}
      <button
        type="button"
        onClick={add}
        className="flex items-center gap-1.5 text-xs font-semibold text-indigo-600 hover:text-indigo-700"
      >
        <Plus size={13} /> Add table
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Recursive CatalogProviderConfig editor — one dispatcher, indented on nesting
// (only `caching`/`fallback` recurse, wrapping another CatalogProviderConfig).
// ---------------------------------------------------------------------------

function CatalogProviderConfigEditor({
  value,
  onChange,
  depth = 0,
}: {
  value: CatalogProviderConfig;
  onChange: (v: CatalogProviderConfig) => void;
  depth?: number;
}) {
  return (
    <div className={depth > 0 ? "border-l-2 border-slate-200 pl-4" : undefined}>
      <Field label="Type">
        <select
          className="w-full max-w-xs px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
          value={value.type}
          onChange={(e) => onChange(DEFAULTS[e.target.value as ProviderType])}
        >
          {(Object.entries(TYPE_LABELS) as [ProviderType, string][]).map(([type, label]) => (
            <option key={type} value={type}>
              {label}
            </option>
          ))}
        </select>
      </Field>

      {UNIMPLEMENTED.has(value.type) && (
        <p className="text-xs text-amber-600 mt-1.5">
          Not implemented yet in this release — builds but degrades to a no-op (schema-aware
          translation falls back to dialect-only). Use &ldquo;Test connection&rdquo; below to
          confirm.
        </p>
      )}

      {value.type === "static" && (
        <div className="mt-3">
          <StaticSchemasEditor
            schemas={value.schemas}
            onChange={(schemas) => onChange({ ...value, schemas })}
          />
        </div>
      )}

      {value.type === "engineDelegate" && (
        <div className="mt-3">
          <TextInput
            label="Cluster group"
            value={value.clusterGroup}
            onChange={(clusterGroup) => onChange({ ...value, clusterGroup })}
            placeholder="trino-default"
          />
        </div>
      )}

      {value.type === "hiveMetastore" && (
        <div className="mt-3">
          <TextInput
            label="Metastore URI"
            value={value.uri}
            onChange={(uri) => onChange({ ...value, uri })}
            placeholder="thrift://localhost:9083"
          />
        </div>
      )}

      {value.type === "glue" && (
        <div className="mt-3">
          <TextInput
            label="AWS region (optional)"
            value={value.region ?? ""}
            onChange={(region) => onChange({ ...value, region: region || null })}
            placeholder="us-east-1"
          />
        </div>
      )}

      {value.type === "caching" && (
        <div className="mt-3 space-y-3">
          <div className="grid grid-cols-2 gap-4 max-w-sm">
            <TextInput
              label="TTL (seconds)"
              type="number"
              value={String(value.ttlSeconds)}
              onChange={(v) => onChange({ ...value, ttlSeconds: Number(v) || 0 })}
            />
            <TextInput
              label="Max entries"
              type="number"
              value={String(value.maxEntries)}
              onChange={(v) => onChange({ ...value, maxEntries: Number(v) || 0 })}
            />
          </div>
          <div>
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1.5">
              Wrapped provider
            </p>
            <CatalogProviderConfigEditor
              value={value.delegate}
              onChange={(delegate) => onChange({ ...value, delegate })}
              depth={depth + 1}
            />
          </div>
        </div>
      )}

      {value.type === "fallback" && (
        <div className="mt-3 space-y-4">
          <div>
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1.5">
              Primary
            </p>
            <CatalogProviderConfigEditor
              value={value.primary}
              onChange={(primary) => onChange({ ...value, primary })}
              depth={depth + 1}
            />
          </div>
          <div>
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1.5">
              Secondary (used when primary errors, or can&rsquo;t find a table)
            </p>
            <CatalogProviderConfigEditor
              value={value.secondary}
              onChange={(secondary) => onChange({ ...value, secondary })}
              depth={depth + 1}
            />
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function CatalogEditor({ initialConfig }: Props) {
  const [config, setConfig] = useState<CatalogProviderConfig>(initialConfig ?? { type: "null" });
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [testing, setTesting] = useState(false);
  const [testMsg, setTestMsg] = useState<{ text: string; ok: boolean } | null>(null);

  const save = async () => {
    setSaving(true);
    setSaveMsg(null);
    try {
      await putCatalogProviderConfig(config);
      setSaveMsg({ text: "Saved. The proxy reloads config automatically.", ok: true });
    } catch (e) {
      setSaveMsg({ text: String(e), ok: false });
    } finally {
      setSaving(false);
    }
  };

  const test = async () => {
    setTesting(true);
    setTestMsg(null);
    try {
      const result = await testCatalogProviderConfig(config);
      setTestMsg({ text: result.message, ok: result.ok });
    } catch (e) {
      setTestMsg({ text: String(e), ok: false });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="p-8 max-w-5xl space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Catalog</h1>
        <p className="text-sm text-slate-500 mt-1">
          Table/column metadata source for schema-aware SQL translation. Defaults to none
          (dialect-only translation) — configuring a provider here lets sqlglot qualify columns
          and types instead of just transpiling syntax.
        </p>
      </div>

      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Database size={15} />} title="Catalog provider" />
        <div className="p-6 space-y-5">
          <CatalogProviderConfigEditor value={config} onChange={setConfig} />

          <div className="space-y-3 pt-2">
            <SaveBar saving={saving} message={saveMsg} onSave={save} />
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={test}
                disabled={testing}
                className="px-4 py-2 rounded-lg border border-slate-200 text-slate-700 text-xs font-semibold hover:bg-slate-50 disabled:opacity-50 transition-colors"
              >
                {testing ? "Testing…" : "Test connection"}
              </button>
              {testMsg && (
                <span
                  className={`text-xs font-medium ${testMsg.ok ? "text-emerald-600" : "text-red-600"}`}
                >
                  {testMsg.text}
                </span>
              )}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
