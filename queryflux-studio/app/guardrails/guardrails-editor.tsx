"use client";

import React, { useState } from "react";
import { Code2, Pencil, Plus, ShieldCheck, Trash2 } from "lucide-react";
import {
  createUserScript,
  deleteUserScript,
  getGuardrailsConfig,
  listUserScripts,
  putGuardrailsConfig,
  updateUserScript,
} from "@/lib/api";
import type { GuardrailsConfig, UserScriptRecord } from "@/lib/api-types";
import { SectionHeader, SaveBar } from "@/components/studio-settings";
import { dtoToRow, GuardList, rowToDto, type GuardRow } from "@/components/guard-list";
import { GUARD_SCRIPT_TEMPLATE } from "@/lib/script-templates";
import CodeMirror from "@uiw/react-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";

export interface GuardrailsEditorProps {
  initialConfig: GuardrailsConfig | null;
  initialScripts: UserScriptRecord[];
}

// ---------------------------------------------------------------------------
// Inline guard script editor (create / edit)
// ---------------------------------------------------------------------------

function GuardScriptForm({
  initial,
  onSave,
  onCancel,
}: {
  initial?: UserScriptRecord;
  onSave: (s: UserScriptRecord) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState(initial?.name ?? "");
  const [description, setDescription] = useState(initial?.description ?? "");
  const [body, setBody] = useState(initial?.body ?? GUARD_SCRIPT_TEMPLATE);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function save() {
    if (!name.trim()) { setError("Name is required."); return; }
    setSaving(true);
    setError(null);
    try {
      const payload = { name: name.trim(), description: description.trim(), kind: "guard", body };
      const saved = initial
        ? await updateUserScript(initial.id, payload)
        : await createUserScript(payload);
      onSave(saved);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  }

  const inputCls =
    "w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300";

  return (
    <div className="rounded-lg border border-indigo-200 bg-indigo-50/20 p-4 space-y-3">
      <div className="flex gap-3">
        <div className="flex-1">
          <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            Name
          </label>
          <input
            className={inputCls}
            placeholder="cost-policy"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </div>
        <div className="flex-1">
          <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            Description <span className="normal-case font-normal text-slate-300">(optional)</span>
          </label>
          <input
            className={inputCls}
            placeholder="Blocks mutations on production tables"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
        </div>
      </div>
      <div>
        <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
          Python body
        </label>
        <CodeMirror
          value={body}
          onChange={setBody}
          extensions={[python()]}
          theme={oneDark}
          basicSetup={{ lineNumbers: true, foldGutter: false }}
          className="rounded-lg overflow-hidden text-xs"
          style={{ fontSize: "12px" }}
        />
      </div>
      {error && <p className="text-xs text-red-600">{error}</p>}
      <div className="flex gap-2">
        <button
          type="button"
          disabled={saving}
          onClick={save}
          className="px-3 py-1.5 rounded-lg bg-indigo-600 text-white text-xs font-semibold hover:bg-indigo-700 disabled:opacity-50"
        >
          {saving ? "Saving…" : initial ? "Update script" : "Create script"}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-500 hover:bg-slate-100"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main editor
// ---------------------------------------------------------------------------

export function GuardrailsEditor({ initialConfig, initialScripts }: GuardrailsEditorProps) {
  const [global, setGlobal] = useState<GuardRow[]>(() =>
    (initialConfig?.global ?? []).map((dto) => dtoToRow(dto, initialScripts)),
  );
  const [guardScripts, setGuardScripts] = useState<UserScriptRecord[]>(initialScripts);
  const [editingScript, setEditingScript] = useState<UserScriptRecord | null | "new">(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<{ text: string; ok: boolean } | null>(null);

  // Re-fetch scripts after create/edit/delete so the list stays fresh.
  async function refreshScripts() {
    const rows = await listUserScripts("guard").catch(() => []);
    setGuardScripts(rows);
  }

  async function handleDeleteScript(id: number) {
    if (!confirm("Delete this guard script? Guards referencing it will need to be updated.")) return;
    try {
      await deleteUserScript(id);
      await refreshScripts();
    } catch (e) {
      alert(e instanceof Error ? e.message : "Delete failed");
    }
  }

  async function save() {
    setSaving(true);
    setMsg(null);
    try {
      const current = await getGuardrailsConfig().catch(() => ({ global: [], groups: {} }));
      await putGuardrailsConfig({ global: global.map(rowToDto), groups: current.groups });
      setMsg({ text: "Saved. Guards apply to new queries immediately.", ok: true });
    } catch (e) {
      setMsg({ text: String(e), ok: false });
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="p-8 max-w-4xl space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Guardrails</h1>
        <p className="text-sm text-slate-500 mt-1">
          Guards inspect the final SQL before it reaches the engine. Configure the global chain
          first, then add Python scripts to the library and attach them globally or per cluster group.
        </p>
      </div>

      {/* Global guards */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<ShieldCheck size={15} />} title="Global guards" />
        <div className="p-6 space-y-4">
          <p className="text-[11px] text-slate-500 leading-relaxed">
            These guards run for <strong>every query</strong>, regardless of cluster group. Supports
            built-in guards, Python scripts (selected from the library below), and HTTP webhooks.
            Per-group guards are configured in the cluster group editor.
          </p>
          <GuardList rows={global} onChange={setGlobal} guardScripts={guardScripts} />
        </div>
      </section>

      {/* Guard scripts library */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Code2 size={15} />} title="Guard scripts" />
        <div className="p-6 space-y-4">
          <p className="text-[11px] text-slate-500 leading-relaxed">
            Reusable Python scripts. Each defines{" "}
            <code className="text-[10px] bg-slate-100 px-1 rounded">
              def check(ctx: dict) -&gt; dict
            </code>{" "}
            and returns <code className="text-[10px] bg-slate-100 px-1 rounded">
              {`{"action": "allow"|"warn"|"deny", ...}`}
            </code>. Attach them above in the global chain or in the cluster group editor.
          </p>

          {guardScripts.length > 0 && (
            <div className="rounded-lg border border-slate-200 overflow-hidden">
              {guardScripts.map((s) => (
                <div
                  key={s.id}
                  className="flex items-center gap-3 px-4 py-2.5 border-b border-slate-100 last:border-b-0 hover:bg-slate-50/70 group"
                >
                  <Code2 size={13} className="text-fuchsia-500 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <span className="text-sm font-mono font-medium text-slate-800">{s.name}</span>
                    {s.description && (
                      <span className="ml-2 text-xs text-slate-400 truncate">{s.description}</span>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={() => setEditingScript(s)}
                    className="p-1.5 rounded-lg text-slate-300 hover:text-indigo-600 hover:bg-indigo-50 opacity-0 group-hover:opacity-100 transition-colors"
                    title="Edit"
                  >
                    <Pencil size={13} />
                  </button>
                  <button
                    type="button"
                    onClick={() => handleDeleteScript(s.id)}
                    className="p-1.5 rounded-lg text-slate-300 hover:text-red-500 hover:bg-red-50 opacity-0 group-hover:opacity-100 transition-colors"
                    title="Delete"
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              ))}
            </div>
          )}

          {editingScript === "new" && (
            <GuardScriptForm
              onSave={async () => {
                await refreshScripts();
                setEditingScript(null);
              }}
              onCancel={() => setEditingScript(null)}
            />
          )}
          {editingScript && editingScript !== "new" && (
            <GuardScriptForm
              initial={editingScript}
              onSave={async () => {
                await refreshScripts();
                setEditingScript(null);
              }}
              onCancel={() => setEditingScript(null)}
            />
          )}

          {editingScript === null && (
            <button
              type="button"
              onClick={() => setEditingScript("new")}
              className="flex items-center gap-1.5 text-xs font-medium text-indigo-600 hover:text-indigo-700 border border-indigo-200 rounded-lg px-3 py-1.5 hover:bg-indigo-50 transition-colors"
            >
              <Plus size={12} /> New guard script
            </button>
          )}
        </div>
      </section>

      <section className="bg-white rounded-xl border border-slate-200 shadow-xs p-6">
        <SaveBar saving={saving} message={msg} onSave={save} label="Save guardrails" />
      </section>
    </div>
  );
}
