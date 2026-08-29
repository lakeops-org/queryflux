"use client";

import React, { useState } from "react";
import { putCatalogProviderConfig, testCatalogProviderConfig } from "@/lib/api";
import type {
  CatalogCacheConfigDto,
  CatalogProviderConfig,
  GlueAuthConfig,
  IcebergRestAuthConfig,
} from "@/lib/api-types";
import { Field, SectionHeader, TextInput, SaveBar } from "@/components/studio-settings";
import { Database } from "lucide-react";

interface Props {
  initialConfig: CatalogProviderConfig | null;
}

type ProviderType = CatalogProviderConfig["type"];

const DEFAULT_CACHE: CatalogCacheConfigDto = { ttlSeconds: 300, maxEntries: 10000 };

// Every real provider here makes a network call per uncached lookup — cache
// is on by default so picking one doesn't quietly add that latency to every
// query. Fully editable/removable via the checkbox in its section.
const DEFAULTS: Record<ProviderType, CatalogProviderConfig> = {
  null: { type: "null" },
  hiveMetastore: { type: "hiveMetastore", uri: "", cache: DEFAULT_CACHE },
  glue: { type: "glue", region: null, auth: null, cache: DEFAULT_CACHE },
  icebergRest: {
    type: "icebergRest",
    uri: "",
    warehouse: null,
    catalogName: "",
    auth: null,
    cache: DEFAULT_CACHE,
  },
  fallback: {
    type: "fallback",
    primary: { type: "null" },
    secondary: { type: "null" },
  },
};

const TYPE_LABELS: Record<ProviderType, string> = {
  null: "None",
  hiveMetastore: "Hive Metastore",
  glue: "AWS Glue",
  icebergRest: "Iceberg REST Catalog",
  fallback: "Fallback (primary → secondary)",
};

// ---------------------------------------------------------------------------
// Cache field editor — shared by every network-calling provider's config.
// ---------------------------------------------------------------------------

function CacheFieldEditor({
  cache,
  onChange,
}: {
  cache: CatalogCacheConfigDto | null | undefined;
  onChange: (v: CatalogCacheConfigDto | null) => void;
}) {
  const enabled = cache != null;
  return (
    <div className="space-y-2">
      <label className="flex items-center gap-2 text-xs font-medium text-slate-700 cursor-pointer">
        <input
          type="checkbox"
          checked={enabled}
          onChange={(e) => onChange(e.target.checked ? DEFAULT_CACHE : null)}
        />
        Cache lookups
      </label>
      <p className="text-xs text-slate-400">
        Recommended — without this, every schema-aware translation attempt on a table QueryFlux
        hasn&rsquo;t seen recently makes a fresh network call. Table schemas change rarely, so a
        TTL of several minutes to hours is normally safe.
      </p>
      {enabled && cache && (
        <div className="grid grid-cols-2 gap-4 max-w-sm pt-1">
          <TextInput
            label="TTL (seconds)"
            type="number"
            value={String(cache.ttlSeconds)}
            onChange={(v) => onChange({ ...cache, ttlSeconds: Number(v) || 0 })}
          />
          <TextInput
            label="Max entries"
            type="number"
            value={String(cache.maxEntries)}
            onChange={(v) => onChange({ ...cache, maxEntries: Number(v) || 0 })}
          />
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Glue auth sub-form
// ---------------------------------------------------------------------------

const GLUE_AUTH_LABELS: Record<"none" | GlueAuthConfig["type"], string> = {
  none: "Default AWS credential chain",
  accessKey: "Static access key",
  roleArn: "Assume IAM role",
};

function GlueAuthEditor({
  auth,
  onChange,
}: {
  auth: GlueAuthConfig | null | undefined;
  onChange: (v: GlueAuthConfig | null) => void;
}) {
  const kind = auth?.type ?? "none";
  return (
    <div className="space-y-3">
      <Field label="Credentials">
        <select
          className="w-full max-w-xs px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
          value={kind}
          onChange={(e) => {
            const next = e.target.value as "none" | GlueAuthConfig["type"];
            if (next === "none") onChange(null);
            else if (next === "accessKey")
              onChange({ type: "accessKey", accessKeyId: "", secretAccessKey: "" });
            else onChange({ type: "roleArn", roleArn: "" });
          }}
        >
          {(Object.entries(GLUE_AUTH_LABELS) as [string, string][]).map(([k, label]) => (
            <option key={k} value={k}>
              {label}
            </option>
          ))}
        </select>
      </Field>

      {auth?.type === "accessKey" && (
        <div className="grid grid-cols-2 gap-4">
          <TextInput
            label="Access key ID"
            value={auth.accessKeyId}
            onChange={(v) => onChange({ ...auth, accessKeyId: v })}
          />
          <TextInput
            label="Secret access key"
            type="password"
            value={auth.secretAccessKey}
            onChange={(v) => onChange({ ...auth, secretAccessKey: v })}
          />
        </div>
      )}

      {auth?.type === "roleArn" && (
        <div className="grid grid-cols-2 gap-4">
          <TextInput
            label="Role ARN"
            value={auth.roleArn}
            onChange={(v) => onChange({ ...auth, roleArn: v })}
            placeholder="arn:aws:iam::123456789012:role/queryflux-glue-readonly"
          />
          <TextInput
            label="External ID (optional)"
            value={auth.externalId ?? ""}
            onChange={(v) => onChange({ ...auth, externalId: v || null })}
          />
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Iceberg REST auth sub-form
// ---------------------------------------------------------------------------

const ICEBERG_REST_AUTH_LABELS: Record<"none" | IcebergRestAuthConfig["type"], string> = {
  none: "None",
  oauth2ClientCredentials: "OAuth2 client credentials",
  bearerToken: "Bearer token",
};

function IcebergRestAuthEditor({
  auth,
  onChange,
}: {
  auth: IcebergRestAuthConfig | null | undefined;
  onChange: (v: IcebergRestAuthConfig | null) => void;
}) {
  const kind = auth?.type ?? "none";
  return (
    <div className="space-y-3">
      <Field label="Auth">
        <select
          className="w-full max-w-xs px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
          value={kind}
          onChange={(e) => {
            const next = e.target.value as "none" | IcebergRestAuthConfig["type"];
            if (next === "none") onChange(null);
            else if (next === "oauth2ClientCredentials")
              onChange({ type: "oauth2ClientCredentials", clientId: "", clientSecret: "" });
            else onChange({ type: "bearerToken", token: "" });
          }}
        >
          {(Object.entries(ICEBERG_REST_AUTH_LABELS) as [string, string][]).map(([k, label]) => (
            <option key={k} value={k}>
              {label}
            </option>
          ))}
        </select>
      </Field>

      {auth?.type === "oauth2ClientCredentials" && (
        <div className="grid grid-cols-2 gap-4">
          <TextInput
            label="Client ID"
            value={auth.clientId}
            onChange={(v) => onChange({ ...auth, clientId: v })}
          />
          <TextInput
            label="Client secret"
            type="password"
            value={auth.clientSecret}
            onChange={(v) => onChange({ ...auth, clientSecret: v })}
          />
        </div>
      )}

      {auth?.type === "bearerToken" && (
        <TextInput
          label="Token"
          type="password"
          value={auth.token}
          onChange={(v) => onChange({ ...auth, token: v })}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Recursive CatalogProviderConfig editor — one dispatcher, indented on nesting
// (only `fallback` recurses, wrapping two more CatalogProviderConfig values).
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

      {value.type === "hiveMetastore" && (
        <div className="mt-3 space-y-4">
          <TextInput
            label="Metastore URI"
            value={value.uri}
            onChange={(uri) => onChange({ ...value, uri })}
            placeholder="thrift://localhost:9083"
          />
          <CacheFieldEditor cache={value.cache} onChange={(cache) => onChange({ ...value, cache })} />
        </div>
      )}

      {value.type === "glue" && (
        <div className="mt-3 space-y-4">
          <TextInput
            label="AWS region (optional)"
            value={value.region ?? ""}
            onChange={(region) => onChange({ ...value, region: region || null })}
            placeholder="us-east-1"
          />
          <GlueAuthEditor auth={value.auth} onChange={(auth) => onChange({ ...value, auth })} />
          <CacheFieldEditor cache={value.cache} onChange={(cache) => onChange({ ...value, cache })} />
        </div>
      )}

      {value.type === "icebergRest" && (
        <div className="mt-3 space-y-4">
          <TextInput
            label="REST catalog URI"
            value={value.uri}
            onChange={(uri) => onChange({ ...value, uri })}
            placeholder="https://polaris.example.com/api/catalog"
          />
          <div className="grid grid-cols-2 gap-4">
            <TextInput
              label="Catalog name"
              value={value.catalogName}
              onChange={(catalogName) => onChange({ ...value, catalogName })}
              placeholder="prod"
            />
            <TextInput
              label="Warehouse (optional)"
              value={value.warehouse ?? ""}
              onChange={(warehouse) => onChange({ ...value, warehouse: warehouse || null })}
              placeholder="s3://my-bucket/warehouse"
            />
          </div>
          <IcebergRestAuthEditor auth={value.auth} onChange={(auth) => onChange({ ...value, auth })} />
          <CacheFieldEditor cache={value.cache} onChange={(cache) => onChange({ ...value, cache })} />
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
