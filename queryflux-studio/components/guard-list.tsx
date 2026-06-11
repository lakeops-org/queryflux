"use client";

import React, { useState } from "react";
import {
  ChevronDown,
  ChevronUp,
  Code2,
  Globe,
  Lock,
  Plus,
  ShieldCheck,
  Table2,
  Trash2,
} from "lucide-react";
import type { GuardSpecDto, UserScriptRecord } from "@/lib/api-types";

// ---------------------------------------------------------------------------
// Local UI model — exported so callers can hold guard state
// ---------------------------------------------------------------------------

export interface GuardRow {
  id: string;
  kind: "built_in" | "http_webhook" | "python_script";
  name: string; // "read_only" | "row_limit" | "require_predicate"
  maxRows: string;
  appliesTo: string; // comma-separated glob patterns
  url: string;
  timeoutMs: string;
  scriptId: string; // python_script: numeric script id stored as string, "" when unset
  scriptName: string; // python_script: display name resolved at load time
  inlineScript: string; // python_script: inline script body, "" when unset
  failBehavior: "deny" | "allow"; // http_webhook: behavior on unreachable
  retryCount: string; // http_webhook: retries after the first failed attempt
  headers: Array<{ key: string; value: string }>; // http_webhook: extra request headers
}

export function uid() {
  return Math.random().toString(36).slice(2);
}

export function dtoToRow(dto: GuardSpecDto, scripts?: UserScriptRecord[]): GuardRow {
  const scriptId = dto.script_id != null ? String(dto.script_id) : "";
  const scriptName = scriptId
    ? (scripts?.find((s) => s.id === dto.script_id)?.name ?? "")
    : "";
  return {
    id: uid(),
    kind: dto.kind,
    name: dto.name ?? "read_only",
    maxRows: dto.max_rows != null ? String(dto.max_rows) : "",
    appliesTo: dto.applies_to ? dto.applies_to.join(", ") : "",
    url: dto.url ?? "",
    timeoutMs: dto.timeout_ms != null ? String(dto.timeout_ms) : "",
    scriptId,
    scriptName,
    inlineScript: dto.script ?? "",
    failBehavior: dto.fail_behavior === "allow" ? "allow" : "deny",
    retryCount: dto.retry_count != null ? String(dto.retry_count) : "",
    headers: dto.headers
      ? Object.entries(dto.headers).map(([key, value]) => ({ key, value }))
      : [],
  };
}

export function rowToDto(row: GuardRow): GuardSpecDto {
  if (row.kind === "http_webhook") {
    const validHeaders = row.headers.filter((h) => h.key.trim());
    return {
      kind: "http_webhook",
      url: row.url,
      timeout_ms: row.timeoutMs.trim() ? Number(row.timeoutMs) : null,
      retry_count: row.retryCount.trim() ? Number(row.retryCount) : null,
      fail_behavior: row.failBehavior,
      headers: validHeaders.length > 0
        ? Object.fromEntries(validHeaders.map((h) => [h.key.trim(), h.value]))
        : null,
    };
  }
  if (row.kind === "python_script") {
    const dto: GuardSpecDto = {
      kind: "python_script",
      timeout_ms: row.timeoutMs.trim() ? Number(row.timeoutMs) : null,
    };
    if (row.scriptId) {
      dto.script_id = Number(row.scriptId);
    } else if (row.inlineScript.trim()) {
      dto.script = row.inlineScript;
    }
    return dto;
  }
  const dto: GuardSpecDto = { kind: "built_in", name: row.name };
  if (row.name === "row_limit" && row.maxRows.trim()) {
    dto.max_rows = Number(row.maxRows);
  }
  if (row.name === "require_predicate" && row.appliesTo.trim()) {
    dto.applies_to = row.appliesTo
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return dto;
}

// ---------------------------------------------------------------------------
// Guard name badge
// ---------------------------------------------------------------------------

const BUILT_IN_META: Record<
  string,
  { label: string; icon: React.ReactNode; color: string; bg: string; border: string }
> = {
  read_only: {
    label: "Read-only",
    icon: <Lock size={11} />,
    color: "text-rose-700",
    bg: "bg-rose-50",
    border: "border-rose-200",
  },
  row_limit: {
    label: "Row limit",
    icon: <Table2 size={11} />,
    color: "text-amber-700",
    bg: "bg-amber-50",
    border: "border-amber-200",
  },
  require_predicate: {
    label: "Require predicate",
    icon: <ShieldCheck size={11} />,
    color: "text-indigo-700",
    bg: "bg-indigo-50",
    border: "border-indigo-200",
  },
};

export function GuardNameBadge({ row }: { row: GuardRow }) {
  if (row.kind === "http_webhook") {
    return (
      <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap text-violet-700 bg-violet-50 border-violet-200">
        <Globe size={11} /> HTTP webhook
      </span>
    );
  }
  if (row.kind === "python_script") {
    return (
      <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap text-fuchsia-700 bg-fuchsia-50 border-fuchsia-200">
        <Code2 size={11} /> Python script
      </span>
    );
  }
  const meta = BUILT_IN_META[row.name] ?? {
    label: row.name,
    icon: <ShieldCheck size={11} />,
    color: "text-slate-600",
    bg: "bg-slate-50",
    border: "border-slate-200",
  };
  return (
    <span
      className={`inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap ${meta.color} ${meta.bg} ${meta.border}`}
    >
      {meta.icon} {meta.label}
    </span>
  );
}

export function guardParamsSummary(row: GuardRow): string {
  if (row.kind === "http_webhook") {
    const parts = [row.url];
    if (row.timeoutMs) parts.push(`timeout: ${row.timeoutMs}ms`);
    if (row.failBehavior === "allow") parts.push("fail-open");
    const hCount = row.headers.filter((h) => h.key.trim()).length;
    if (hCount > 0) parts.push(`${hCount} header${hCount > 1 ? "s" : ""}`);
    return parts.join("  ·  ");
  }
  if (row.kind === "python_script") {
    const label = row.scriptName || (row.scriptId ? `#${row.scriptId}` : "(no script selected)");
    const parts = [label];
    if (row.timeoutMs) parts.push(`timeout: ${row.timeoutMs}ms`);
    return parts.join("  ·  ");
  }
  switch (row.name) {
    case "row_limit":
      return row.maxRows ? `max rows: ${row.maxRows}` : "no limit set";
    case "require_predicate":
      return row.appliesTo ? `tables: ${row.appliesTo}` : "all tables";
    default:
      return "";
  }
}

// ---------------------------------------------------------------------------
// Add guard form
// ---------------------------------------------------------------------------

function blankGuardRow(kind: GuardRow["kind"], name = "read_only"): GuardRow {
  return {
    id: uid(),
    kind,
    name,
    maxRows: "",
    appliesTo: "",
    url: "",
    timeoutMs: "",
    scriptId: "",
    scriptName: "",
    inlineScript: "",
    failBehavior: "deny",
    retryCount: "",
    headers: [],
  };
}

export function AddGuardForm({
  onAdd,
  onCancel,
  guardScripts = [],
}: {
  onAdd: (row: GuardRow) => void;
  onCancel: () => void;
  guardScripts?: UserScriptRecord[];
}) {
  const [form, setForm] = useState<GuardRow>(blankGuardRow("built_in", "read_only"));

  const inputCls =
    "px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300";

  const isValid =
    form.kind === "http_webhook"
      ? form.url.trim() !== ""
      : form.kind === "python_script"
      ? form.scriptId.trim() !== "" || form.inlineScript.trim() !== ""
      : form.name !== "";

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-3 space-y-3">
      <div className="flex flex-wrap items-end gap-2.5">
        <div>
          <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            Type
          </label>
          <select
            className={inputCls}
            value={form.kind}
            onChange={(e) => {
              const kind = e.target.value as GuardRow["kind"];
              setForm(blankGuardRow(kind, kind === "built_in" ? "read_only" : ""));
            }}
          >
            <option value="built_in">Built-in</option>
            <option value="python_script">Python script</option>
            <option value="http_webhook">HTTP webhook</option>
          </select>
        </div>

        {form.kind === "built_in" && (
          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Guard
            </label>
            <select
              className={inputCls}
              value={form.name}
              onChange={(e) =>
                setForm((f) => ({ ...f, name: e.target.value, maxRows: "", appliesTo: "" }))
              }
            >
              <option value="read_only">read_only</option>
              <option value="row_limit">row_limit</option>
              <option value="require_predicate">require_predicate</option>
            </select>
          </div>
        )}

        {form.kind === "built_in" && form.name === "row_limit" && (
          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Max rows
            </label>
            <input
              type="number"
              className={`w-28 ${inputCls}`}
              placeholder="10000"
              value={form.maxRows}
              onChange={(e) => setForm((f) => ({ ...f, maxRows: e.target.value }))}
            />
          </div>
        )}

        {form.kind === "built_in" && form.name === "require_predicate" && (
          <div className="flex-1 min-w-[200px]">
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Table patterns{" "}
              <span className="normal-case font-normal text-slate-300">(comma-sep. globs)</span>
            </label>
            <input
              className={`w-full ${inputCls}`}
              placeholder="analytics.fct_*, reporting.*"
              value={form.appliesTo}
              onChange={(e) => setForm((f) => ({ ...f, appliesTo: e.target.value }))}
            />
          </div>
        )}

        {form.kind === "python_script" && (
          <div className="w-full space-y-2.5">
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Script
              </label>
              {guardScripts.length > 0 ? (
                <select
                  className={`w-full ${inputCls}`}
                  value={form.scriptId}
                  onChange={(e) => {
                    const id = e.target.value;
                    const scriptName = guardScripts.find((s) => String(s.id) === id)?.name ?? "";
                    setForm((f) => ({ ...f, scriptId: id, scriptName }));
                  }}
                >
                  <option value="">Select script…</option>
                  {guardScripts.map((s) => (
                    <option key={s.id} value={String(s.id)}>
                      {s.name}
                    </option>
                  ))}
                </select>
              ) : (
                <div className="text-xs text-slate-400 italic px-2.5 py-1.5 rounded-lg border border-slate-200 bg-slate-50">
                  No guard scripts yet — define them on the{" "}
                  <a href="/guardrails" className="text-indigo-600 hover:underline">Guardrails</a>{" "}
                  page.
                </div>
              )}
            </div>
            <div className="w-28">
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Timeout ms
              </label>
              <input
                type="number"
                className={`w-full ${inputCls}`}
                placeholder="200"
                value={form.timeoutMs}
                onChange={(e) => setForm((f) => ({ ...f, timeoutMs: e.target.value }))}
              />
            </div>
          </div>
        )}

        {form.kind === "http_webhook" && (
          <div className="w-full space-y-2.5">
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Endpoint URL
              </label>
              <input
                className={`w-full ${inputCls}`}
                placeholder="https://policy.internal/guard"
                value={form.url}
                onChange={(e) => setForm((f) => ({ ...f, url: e.target.value }))}
              />
            </div>
            <div className="flex gap-2.5">
              <div className="w-28">
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Timeout ms
                </label>
                <input
                  type="number"
                  className={`w-full ${inputCls}`}
                  placeholder="500"
                  value={form.timeoutMs}
                  onChange={(e) => setForm((f) => ({ ...f, timeoutMs: e.target.value }))}
                />
              </div>
              <div className="w-20">
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Retries
                </label>
                <input
                  type="number"
                  min={0}
                  className={`w-full ${inputCls}`}
                  placeholder="0"
                  value={form.retryCount}
                  onChange={(e) => setForm((f) => ({ ...f, retryCount: e.target.value }))}
                />
              </div>
              <div className="flex-1">
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  On failure
                </label>
                <select
                  className={`w-full ${inputCls}`}
                  value={form.failBehavior}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, failBehavior: e.target.value as "deny" | "allow" }))
                  }
                >
                  <option value="deny">deny (fail-closed)</option>
                  <option value="allow">allow (fail-open)</option>
                </select>
              </div>
            </div>
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Headers <span className="normal-case font-normal text-slate-300">(optional)</span>
              </label>
              <div className="space-y-1.5">
                {form.headers.map((h, i) => (
                  <div key={i} className="flex gap-1.5 items-center">
                    <input
                      className={`flex-1 ${inputCls}`}
                      placeholder="Header name"
                      value={h.key}
                      onChange={(e) =>
                        setForm((f) => {
                          const headers = [...f.headers];
                          headers[i] = { ...headers[i], key: e.target.value };
                          return { ...f, headers };
                        })
                      }
                    />
                    <input
                      className={`flex-1 ${inputCls}`}
                      placeholder="Value"
                      value={h.value}
                      onChange={(e) =>
                        setForm((f) => {
                          const headers = [...f.headers];
                          headers[i] = { ...headers[i], value: e.target.value };
                          return { ...f, headers };
                        })
                      }
                    />
                    <button
                      type="button"
                      onClick={() =>
                        setForm((f) => ({ ...f, headers: f.headers.filter((_, idx) => idx !== i) }))
                      }
                      className="p-1.5 rounded-lg text-slate-300 hover:text-red-500 hover:bg-red-50 transition-colors"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
                <button
                  type="button"
                  onClick={() =>
                    setForm((f) => ({ ...f, headers: [...f.headers, { key: "", value: "" }] }))
                  }
                  className="flex items-center gap-1 text-[11px] text-indigo-600 hover:text-indigo-700"
                >
                  <Plus size={11} /> Add header
                </button>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="flex items-center gap-2 pt-1">
        <button
          type="button"
          disabled={!isValid}
          onClick={() => isValid && onAdd(form)}
          className="px-3 py-1.5 rounded-lg bg-indigo-600 text-white text-xs font-semibold hover:bg-indigo-700 disabled:opacity-40 transition-colors"
        >
          Add guard
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-600 bg-white border border-slate-200 hover:bg-slate-50 transition-colors"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Guard row card
// ---------------------------------------------------------------------------

function GuardRowCard({
  row,
  index,
  total,
  onMoveUp,
  onMoveDown,
  onRemove,
}: {
  row: GuardRow;
  index: number;
  total: number;
  onMoveUp: () => void;
  onMoveDown: () => void;
  onRemove: () => void;
}) {
  const summary = guardParamsSummary(row);
  return (
    <div className="flex items-center gap-2.5 px-3 py-2.5 bg-white border-b border-slate-100 last:border-b-0 hover:bg-slate-50/70 transition-colors group">
      <div className="flex flex-col gap-0.5 shrink-0">
        <button
          type="button"
          disabled={index === 0}
          onClick={onMoveUp}
          className="p-0.5 rounded text-slate-300 hover:text-slate-600 hover:bg-slate-100 disabled:opacity-20 transition-colors"
        >
          <ChevronUp size={12} />
        </button>
        <button
          type="button"
          disabled={index === total - 1}
          onClick={onMoveDown}
          className="p-0.5 rounded text-slate-300 hover:text-slate-600 hover:bg-slate-100 disabled:opacity-20 transition-colors"
        >
          <ChevronDown size={12} />
        </button>
      </div>

      <span className="text-[10px] font-bold text-slate-400 tabular-nums min-w-[1.25rem] text-center">
        {index + 1}
      </span>

      <GuardNameBadge row={row} />

      {summary && (
        <span className="text-[11px] text-slate-400 font-mono truncate flex-1 min-w-0">
          {summary}
        </span>
      )}
      {!summary && <span className="flex-1" />}

      <button
        type="button"
        onClick={onRemove}
        className="p-1.5 rounded-lg text-slate-200 hover:text-red-500 hover:bg-red-50 transition-colors opacity-0 group-hover:opacity-100"
        title="Remove"
      >
        <Trash2 size={11} />
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Guard list — main reusable component
// ---------------------------------------------------------------------------

export function GuardList({
  rows,
  onChange,
  emptyText = "No guards — all queries pass through.",
  guardScripts = [],
}: {
  rows: GuardRow[];
  onChange: (next: GuardRow[]) => void;
  emptyText?: string;
  guardScripts?: UserScriptRecord[];
}) {
  const [adding, setAdding] = useState(false);

  function move(index: number, delta: -1 | 1) {
    const j = index + delta;
    if (j < 0 || j >= rows.length) return;
    const next = [...rows];
    [next[index], next[j]] = [next[j], next[index]];
    onChange(next);
  }

  return (
    <div className="space-y-2.5">
      {rows.length === 0 && !adding && (
        <p className="text-xs text-slate-400 italic">{emptyText}</p>
      )}

      {rows.length > 0 && (
        <div className="rounded-lg border border-slate-200 overflow-hidden">
          {rows.map((row, i) => (
            <GuardRowCard
              key={row.id}
              row={row}
              index={i}
              total={rows.length}
              onMoveUp={() => move(i, -1)}
              onMoveDown={() => move(i, 1)}
              onRemove={() => onChange(rows.filter((_, idx) => idx !== i))}
            />
          ))}
        </div>
      )}

      {adding ? (
        <AddGuardForm
          guardScripts={guardScripts}
          onAdd={(row) => {
            onChange([...rows, row]);
            setAdding(false);
          }}
          onCancel={() => setAdding(false)}
        />
      ) : (
        <button
          type="button"
          onClick={() => setAdding(true)}
          className="flex items-center gap-1.5 text-xs font-medium text-indigo-600 hover:text-indigo-700 border border-indigo-200 rounded-lg px-3 py-1.5 hover:bg-indigo-50 transition-colors"
        >
          <Plus size={12} /> Add guard
        </button>
      )}
    </div>
  );
}
