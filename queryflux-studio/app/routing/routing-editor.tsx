"use client";

import React, { useState } from "react";
import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import Link from "next/link";
import { putRoutingConfig } from "@/lib/api";
import type { RoutingConfigDto, CompoundConditionEntry } from "@/lib/api-types";
import {
  chainItemsToRouters,
  type ChainItem,
  routersToChainItems,
} from "@/lib/routing-chain";
import { ROUTING_SCRIPT_TEMPLATE } from "@/lib/script-templates";
import { PythonRoutingScriptDialog } from "@/components/python-routing-script-dialog";
import {
  ArrowRight,
  ChevronDown,
  ChevronUp,
  Code2,
  GripVertical,
  Globe,
  Layers,
  Network,
  Pencil,
  Plus,
  Route,
  Tag,
  Trash2,
  User,
  XCircle,
} from "lucide-react";
import { Field, SectionHeader, SaveBar } from "@/components/studio-settings";

export interface ClusterGroupOption {
  id: number;
  name: string;
}

// ---------------------------------------------------------------------------
// Flat routing rule model
// ---------------------------------------------------------------------------

type FlatRuleType = "header" | "user" | "tag" | "regex" | "protocol";

/** One saved multi-condition (AND/OR) router row — edited only via dialog. */
interface CompoundRuleBlock {
  id: string;
  combine: "all" | "any";
  conditions: CompoundCondRow[];
  targetGroupId: number | null;
}

interface CompoundCondRow {
  id: string;
  condType: "protocol" | "header" | "user" | "tag" | "regex";
  protocol: string;
  headerName: string;
  headerValue: string;
  username: string;
  tagKey: string;
  tagValue: string;
  regex: string;
}

interface FlatRule {
  id: string;
  ruleType: FlatRuleType;
  // header
  headerName: string;
  headerValue: string;
  // user
  username: string;
  // tag — key is required, value is optional (empty = key-only / any value)
  tagKey: string;
  tagValue: string;
  // regex
  regex: string;
  /** SQL regex action: route (default) or deny. */
  regexAction: "route" | "deny";
  /** Client-visible message when regexAction is deny. */
  denyError: string;
  // protocol
  protocol: string;
  // shared — cluster_group_configs.id
  targetGroupId: number | null;
}

const PROTOCOLS = [
  { value: "trinoHttp", label: "Trino HTTP" },
  { value: "postgresWire", label: "Postgres wire" },
  { value: "mysqlWire", label: "MySQL wire" },
  { value: "clickhouseHttp", label: "ClickHouse HTTP" },
  { value: "flightSql", label: "Flight SQL" },
];

function uid() {
  return Math.random().toString(36).slice(2);
}

function blankCompoundCond(): CompoundCondRow {
  return {
    id: uid(),
    condType: "protocol",
    protocol: "trinoHttp",
    headerName: "",
    headerValue: "",
    username: "",
    tagKey: "",
    tagValue: "",
    regex: "",
  };
}

function blankRule(ruleType: FlatRuleType): FlatRule {
  return {
    id: uid(),
    ruleType,
    headerName: "",
    headerValue: "",
    username: "",
    tagKey: "",
    tagValue: "",
    regex: "",
    regexAction: "route",
    denyError: "",
    protocol: "trinoHttp",
    targetGroupId: null,
  };
}

function blankCompoundRuleBlock(): CompoundRuleBlock {
  return {
    id: uid(),
    combine: "all",
    conditions: [blankCompoundCond()],
    targetGroupId: null,
  };
}

function compoundRuleBlockValid(b: CompoundRuleBlock): boolean {
  return (
    b.targetGroupId != null &&
    b.conditions.length > 0 &&
    b.conditions.every(compoundConditionValid)
  );
}

function parseCompoundConditionFromApi(c: CompoundConditionEntry): CompoundCondRow {
  const row = blankCompoundCond();
  row.id = uid();
  switch (c.type) {
    case "protocol":
      row.condType = "protocol";
      row.protocol = c.protocol ?? "trinoHttp";
      break;
    case "header":
      row.condType = "header";
      row.headerName = c.headerName ?? "";
      row.headerValue = c.headerValue ?? "";
      break;
    case "user":
      row.condType = "user";
      row.username = c.username ?? "";
      break;
    case "clientTag":
      row.condType = "tag";
      row.tagKey = c.tag ?? "";
      row.tagValue = "";
      break;
    case "queryRegex":
      row.condType = "regex";
      row.regex = c.regex ?? "";
      break;
    default:
      break;
  }
  return row;
}

function compoundConditionValid(c: CompoundCondRow): boolean {
  switch (c.condType) {
    case "protocol":
      return c.protocol !== "";
    case "header":
      return c.headerName.trim() !== "" && c.headerValue.trim() !== "";
    case "user":
      return c.username.trim() !== "";
    case "tag":
      return c.tagKey.trim() !== "";
    case "regex":
      return c.regex.trim() !== "";
    default:
      return false;
  }
}

function compoundConditionToApi(c: CompoundCondRow): CompoundConditionEntry {
  switch (c.condType) {
    case "protocol":
      return { type: "protocol", protocol: c.protocol };
    case "header":
      return { type: "header", headerName: c.headerName, headerValue: c.headerValue };
    case "user":
      return { type: "user", username: c.username };
    case "tag":
      return { type: "clientTag", tag: c.tagKey };
    case "regex":
      return { type: "queryRegex", regex: c.regex };
  }
}

function flatRuleToChainItem(rule: FlatRule): ChainItem {
  const id = uid();
  switch (rule.ruleType) {
    case "header":
      return {
        id,
        kind: "header",
        headerName: rule.headerName,
        headerValue: rule.headerValue,
        targetGroupId: rule.targetGroupId,
      };
    case "user":
      return { id, kind: "user", username: rule.username, targetGroupId: rule.targetGroupId };
    case "tag":
      return { id, kind: "tag", tagKey: rule.tagKey, tagValue: rule.tagValue, targetGroupId: rule.targetGroupId };
    case "regex":
      return {
        id,
        kind: "regex",
        regex: rule.regex,
        action: rule.regexAction,
        error: rule.denyError,
        targetGroupId: rule.regexAction === "deny" ? null : rule.targetGroupId,
      };
    case "protocol":
      return {
        id,
        kind: "protocol",
        protocol: rule.protocol,
        targetGroupId: rule.targetGroupId,
      };
  }
}

function chainCompoundToBlock(item: Extract<ChainItem, { kind: "compound" }>): CompoundRuleBlock {
  const conditions =
    item.conditions.length > 0
      ? item.conditions.map(parseCompoundConditionFromApi)
      : [blankCompoundCond()];
  return {
    id: item.id,
    combine: item.combine,
    targetGroupId: item.targetGroupId,
    conditions,
  };
}

function compoundBlockToChainItem(block: CompoundRuleBlock): Extract<ChainItem, { kind: "compound" }> {
  return {
    id: block.id,
    kind: "compound",
    combine: block.combine,
    targetGroupId: block.targetGroupId,
    conditions: block.conditions.map(compoundConditionToApi),
  };
}

function chainItemConditionSummary(item: ChainItem, protocols: typeof PROTOCOLS): string {
  switch (item.kind) {
    case "header":
      return `${item.headerName} = "${item.headerValue}"`;
    case "user":
      return item.username;
    case "tag":
      return item.tagValue ? `${item.tagKey}:${item.tagValue}` : item.tagKey;
    case "regex":
      return item.regex;
    case "protocol":
      return protocols.find((p) => p.value === item.protocol)?.label ?? item.protocol;
    case "compound":
      return compoundBlockSummary(chainCompoundToBlock(item));
    case "pythonScript":
      return "Python script router — use Edit to view or change code";
    case "passthrough":
      return `Raw: ${item.router.type}`;
  }
}

// ---------------------------------------------------------------------------
// Rule type badge
// ---------------------------------------------------------------------------

const RULE_TYPE_META: Record<
  FlatRuleType,
  { label: string; icon: React.ReactNode; color: string; bg: string; border: string }
> = {
  header: {
    label: "Header",
    icon: <Globe size={11} />,
    color: "text-indigo-700",
    bg: "bg-indigo-50",
    border: "border-indigo-200",
  },
  user: {
    label: "User",
    icon: <User size={11} />,
    color: "text-emerald-700",
    bg: "bg-emerald-50",
    border: "border-emerald-200",
  },
  tag: {
    label: "Tag",
    icon: <Tag size={11} />,
    color: "text-amber-700",
    bg: "bg-amber-50",
    border: "border-amber-200",
  },
  regex: {
    label: "SQL Regex",
    icon: <Code2 size={11} />,
    color: "text-violet-700",
    bg: "bg-violet-50",
    border: "border-violet-200",
  },
  protocol: {
    label: "Protocol",
    icon: <Network size={11} />,
    color: "text-sky-700",
    bg: "bg-sky-50",
    border: "border-sky-200",
  },
};

function RuleTypeBadge({ type }: { type: FlatRuleType }) {
  const m = RULE_TYPE_META[type];
  return (
    <span
      className={`inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap ${m.color} ${m.bg} ${m.border}`}
    >
      {m.icon} {m.label}
    </span>
  );
}

function ChainItemTypeBadge({ item }: { item: ChainItem }) {
  switch (item.kind) {
    case "compound":
      return <MultiConditionBadge />;
    case "pythonScript":
      return (
        <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap text-fuchsia-700 bg-fuchsia-50 border-fuchsia-200">
          <Code2 size={11} /> Python script
        </span>
      );
    case "passthrough":
      return (
        <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap text-slate-600 bg-slate-100 border-slate-200">
          {item.router.type}
        </span>
      );
    default:
      return <RuleTypeBadge type={item.kind} />;
  }
}

function MultiConditionBadge() {
  const m = {
    label: "Multi-condition",
    icon: <Layers size={11} />,
    color: "text-rose-700",
    bg: "bg-rose-50",
    border: "border-rose-200",
  };
  return (
    <span
      className={`inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-md border whitespace-nowrap ${m.color} ${m.bg} ${m.border}`}
    >
      {m.icon} {m.label}
    </span>
  );
}

function compoundBlockSummary(block: CompoundRuleBlock): string {
  const n = block.conditions.length;
  const mode = block.combine === "any" ? "ANY" : "ALL";
  return `${mode} · ${n} condition${n === 1 ? "" : "s"}`;
}

function compoundCondSummary(c: CompoundCondRow): string {
  switch (c.condType) {
    case "protocol":
      return PROTOCOLS.find((p) => p.value === c.protocol)?.label ?? c.protocol;
    case "header":
      return `${c.headerName}="${c.headerValue}"`;
    case "user":
      return `user:${c.username}`;
    case "tag":
      return c.tagValue ? `tag:${c.tagKey}:${c.tagValue}` : `tag:${c.tagKey}`;
    case "regex":
      return c.regex.length > 48 ? `${c.regex.slice(0, 48)}…` : c.regex;
  }
}

// ---------------------------------------------------------------------------
// Multi-condition rule: condition rows (AND/OR lives in dialog footer)
// ---------------------------------------------------------------------------

function CompoundConditionsEditor({
  conditions,
  onChange,
}: {
  conditions: CompoundCondRow[];
  onChange: (next: CompoundCondRow[]) => void;
}) {
  const setConds = onChange;
  const updateCond = (id: string, patch: Partial<CompoundCondRow>) => {
    setConds(conditions.map((c) => (c.id === id ? { ...c, ...patch } : c)));
  };

  const inputCls =
    "px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300";

  return (
    <div className="space-y-2">
      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
        Conditions
      </p>
      {conditions.map((c) => (
          <div
            key={c.id}
            className="flex flex-wrap items-end gap-2 p-3 rounded-lg border border-slate-200 bg-slate-50/50"
          >
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Type
              </label>
              <select
                className={inputCls}
                value={c.condType}
                onChange={(e) => {
                  const t = e.target.value as CompoundCondRow["condType"];
                  updateCond(c.id, {
                    condType: t,
                    protocol: "trinoHttp",
                    headerName: "",
                    headerValue: "",
                    username: "",
                    tagKey: "",
                    tagValue: "",
                    regex: "",
                  });
                }}
              >
                <option value="protocol">Protocol</option>
                <option value="header">Header</option>
                <option value="user">User</option>
                <option value="tag">Tag</option>
                <option value="regex">SQL regex</option>
              </select>
            </div>

            {c.condType === "protocol" && (
              <div>
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Protocol
                </label>
                <select
                  className={inputCls}
                  value={c.protocol}
                  onChange={(e) => updateCond(c.id, { protocol: e.target.value })}
                >
                  {PROTOCOLS.map((p) => (
                    <option key={p.value} value={p.value}>
                      {p.label}
                    </option>
                  ))}
                </select>
              </div>
            )}

            {c.condType === "header" && (
              <>
                <div>
                  <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                    Header
                  </label>
                  <input
                    className={inputCls}
                    placeholder="X-Env"
                    value={c.headerName}
                    onChange={(e) => updateCond(c.id, { headerName: e.target.value })}
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                    Value
                  </label>
                  <input
                    className={inputCls}
                    placeholder="prod"
                    value={c.headerValue}
                    onChange={(e) => updateCond(c.id, { headerValue: e.target.value })}
                  />
                </div>
              </>
            )}

            {c.condType === "user" && (
              <div>
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Username
                </label>
                <input
                  className={inputCls}
                  placeholder="alice"
                  value={c.username}
                  onChange={(e) => updateCond(c.id, { username: e.target.value })}
                />
              </div>
            )}

            {c.condType === "tag" && (
              <>
                <div>
                  <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                    Tag key
                  </label>
                  <input
                    className={inputCls}
                    placeholder="team"
                    value={c.tagKey}
                    onChange={(e) => updateCond(c.id, { tagKey: e.target.value })}
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                    Value <span className="normal-case font-normal text-slate-300">(optional)</span>
                  </label>
                  <input
                    className={inputCls}
                    placeholder="eng"
                    value={c.tagValue}
                    onChange={(e) => updateCond(c.id, { tagValue: e.target.value })}
                  />
                </div>
              </>
            )}

            {c.condType === "regex" && (
              <div className="flex-1 min-w-[200px]">
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Regex
                </label>
                <input
                  className={`${inputCls} w-full`}
                  placeholder="(?i)^SELECT"
                  value={c.regex}
                  onChange={(e) => updateCond(c.id, { regex: e.target.value })}
                />
              </div>
            )}

            <button
              type="button"
              onClick={() => setConds(conditions.filter((x) => x.id !== c.id))}
              disabled={conditions.length <= 1}
              className="p-1.5 rounded-lg text-slate-300 hover:text-red-500 hover:bg-red-50 disabled:opacity-30 transition-colors mb-0.5"
              title="Remove condition"
            >
              <Trash2 size={12} />
            </button>
          </div>
        ))}
      <button
        type="button"
        onClick={() => setConds([...conditions, blankCompoundCond()])}
        className="flex items-center gap-1 text-[11px] font-medium text-indigo-600 hover:text-indigo-700"
      >
        <Plus size={12} /> Add condition
      </button>
    </div>
  );
}

function CompoundRuleDialog({
  open,
  mode,
  initial,
  groups,
  onClose,
  onSave,
}: {
  open: boolean;
  mode: "create" | "edit";
  initial: CompoundRuleBlock | null;
  groups: ClusterGroupOption[];
  onClose: () => void;
  onSave: (block: CompoundRuleBlock) => void;
}) {
  const [draft, setDraft] = useState<CompoundRuleBlock>(() => blankCompoundRuleBlock());

  React.useEffect(() => {
    if (!open) return;
    if (initial) {
      setDraft({
        id: initial.id,
        combine: initial.combine,
        targetGroupId: initial.targetGroupId,
        conditions: initial.conditions.map((c) => ({ ...c })),
      });
    } else {
      setDraft(blankCompoundRuleBlock());
    }
    // Intentionally only when the dialog opens — avoids resetting while editing if the table
    // updates the same row (e.g. group dropdown).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  const valid = compoundRuleBlockValid(draft);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/40 backdrop-blur-[2px]"
      role="dialog"
      aria-modal="true"
      aria-labelledby="compound-rule-dialog-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="bg-white rounded-xl border border-slate-200 shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between gap-3">
          <h2
            id="compound-rule-dialog-title"
            className="text-sm font-bold text-slate-900"
          >
            {mode === "create" ? "New multi-condition route" : "Edit multi-condition route"}
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:bg-slate-100 hover:text-slate-600"
            aria-label="Close"
          >
            <XCircle size={16} />
          </button>
        </div>

        <div className="p-5 space-y-5">
          <Field label="Route to group">
            <GroupSelectById
              valueId={draft.targetGroupId}
              onChangeId={(id) => setDraft((d) => ({ ...d, targetGroupId: id }))}
              groups={groups}
            />
          </Field>

          <CompoundConditionsEditor
            conditions={draft.conditions}
            onChange={(next) => setDraft((d) => ({ ...d, conditions: next }))}
          />

          <div className="rounded-lg border border-slate-200 bg-slate-50/80 p-4 space-y-2">
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
              Apply to all conditions
            </label>
            <select
              className="w-full max-w-[220px] px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
              value={draft.combine}
              onChange={(e) =>
                setDraft((d) => ({
                  ...d,
                  combine: e.target.value === "any" ? "any" : "all",
                }))
              }
            >
              <option value="all">All match (AND)</option>
              <option value="any">Any match (OR)</option>
            </select>
            <p className="text-[11px] text-slate-500">
              <span className="font-medium text-slate-600">All</span> — every row must match.
              <span className="font-medium text-slate-600"> Any</span> — at least one row must
              match.
            </p>
          </div>
        </div>

        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2 bg-slate-50/50 rounded-b-xl">
          <button
            type="button"
            onClick={onClose}
            className="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-600 hover:bg-slate-200/80 transition-colors"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={!valid}
            onClick={() => {
              onSave(draft);
              onClose();
            }}
            className="px-4 py-1.5 rounded-lg bg-indigo-600 text-white text-xs font-semibold hover:bg-indigo-700 disabled:opacity-40 transition-colors"
          >
            {mode === "create" ? "Add route" : "Save changes"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// New-rule inline form
// ---------------------------------------------------------------------------

function GroupSelectById({
  valueId,
  onChangeId,
  groups,
}: {
  valueId: number | null;
  onChangeId: (id: number | null) => void;
  groups: ClusterGroupOption[];
}) {
  if (groups.length === 0) {
    return (
      <input
        type="number"
        className="min-w-[180px] px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
        placeholder="group id"
        value={valueId ?? ""}
        onChange={(e) => {
          const v = e.target.value;
          onChangeId(v === "" ? null : Number(v));
        }}
      />
    );
  }
  return (
    <select
      className="min-w-[180px] px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
      value={valueId === null ? "" : String(valueId)}
      onChange={(e) => {
        const v = e.target.value;
        onChangeId(v === "" ? null : Number(v));
      }}
    >
      <option value="">Select group…</option>
      {groups.map((g) => (
        <option key={g.id} value={String(g.id)}>
          {g.name}
        </option>
      ))}
    </select>
  );
}

function NewRuleForm({
  onAdd,
  onCancel,
  groups,
}: {
  onAdd: (r: FlatRule) => void;
  onCancel: () => void;
  groups: ClusterGroupOption[];
}) {
  const [form, setForm] = useState<FlatRule>(blankRule("header"));

  const isValid =
    (() => {
      switch (form.ruleType) {
        case "header":
          return (
            form.targetGroupId != null &&
            form.headerName.trim() !== "" &&
            form.headerValue.trim() !== ""
          );
        case "user":
          return form.targetGroupId != null && form.username.trim() !== "";
        case "tag":
          return form.targetGroupId != null && form.tagKey.trim() !== "";
        case "regex":
          if (form.regex.trim() === "") return false;
          try {
            new RegExp(form.regex);
          } catch {
            return false;
          }
          if (form.regexAction === "deny") return true;
          return form.targetGroupId != null;
        case "protocol":
          return form.targetGroupId != null && form.protocol !== "";
      }
    })();

  const input = (
    placeholder: string,
    value: string,
    onChange: (v: string) => void,
    extraClass = "",
    ariaLabel?: string,
  ) => (
    <input
      className={`px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 ${extraClass}`}
      placeholder={placeholder}
      value={value}
      aria-label={ariaLabel}
      onChange={(e) => onChange(e.target.value)}
    />
  );

  return (
    <div className="rounded-lg border border-dashed border-indigo-300 bg-indigo-50/30 p-4 space-y-4">
      <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
        New routing rule
      </p>

      <div className="flex flex-wrap items-end gap-3">
        {/* Rule type */}
        <div className="min-w-[130px]">
          <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            Rule type
          </label>
          <select
            className="w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            value={form.ruleType}
            onChange={(e) => setForm(blankRule(e.target.value as FlatRuleType))}
          >
            <option value="header">Header</option>
            <option value="user">User</option>
            <option value="protocol">Protocol</option>
            <option value="regex">SQL Regex</option>
            <option value="tag">Tag</option>
          </select>
        </div>

        {/* Condition — type-specific */}
        {form.ruleType === "header" && (
          <>
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Header name
              </label>
              {input("X-Custom-Header", form.headerName, (v) =>
                setForm((f) => ({ ...f, headerName: v })),
              )}
            </div>
            <div className="pb-1.5 text-slate-400 text-xs font-medium">equals</div>
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Header value
              </label>
              {input("analytics", form.headerValue, (v) =>
                setForm((f) => ({ ...f, headerValue: v })),
              )}
            </div>
          </>
        )}

        {form.ruleType === "user" && (
          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Username
            </label>
            {input("alice", form.username, (v) => setForm((f) => ({ ...f, username: v })))}
          </div>
        )}

        {form.ruleType === "tag" && (
          <>
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Tag key
              </label>
              {input("team", form.tagKey, (v) => setForm((f) => ({ ...f, tagKey: v })))}
            </div>
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Value <span className="normal-case font-normal text-slate-300">(optional)</span>
              </label>
              {input("eng", form.tagValue, (v) => setForm((f) => ({ ...f, tagValue: v })))}
            </div>
          </>
        )}

        {form.ruleType === "regex" && (
          <div className="flex-1 min-w-[220px] space-y-3">
            <div>
              <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                Regex pattern
              </label>
              {input("^SELECT.*fact_", form.regex, (v) => setForm((f) => ({ ...f, regex: v })), "w-full")}
            </div>
            <div className="flex flex-wrap gap-3 items-end">
              <div>
                <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                  Action
                </label>
                <select
                  className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                  value={form.regexAction}
                  onChange={(e) =>
                    setForm((f) => ({
                      ...f,
                      regexAction: e.target.value as "route" | "deny",
                      targetGroupId: e.target.value === "deny" ? null : f.targetGroupId,
                    }))
                  }
                >
                  <option value="route">Route</option>
                  <option value="deny">Deny</option>
                </select>
              </div>
              {form.regexAction === "deny" && (
                <div className="flex-1 min-w-[180px]">
                  <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
                    Error message
                  </label>
                  {input(
                    "Query denied by routing rule",
                    form.denyError,
                    (v) => setForm((f) => ({ ...f, denyError: v })),
                    "w-full",
                    "Denial message",
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {form.ruleType === "protocol" && (
          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Protocol
            </label>
            <select
              className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
              value={form.protocol}
              onChange={(e) => setForm((f) => ({ ...f, protocol: e.target.value }))}
            >
              {PROTOCOLS.filter((p) => p.value !== "flightSql").map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
        )}

        {/* Arrow */}
        <div className="pb-2 text-slate-300">
          <ArrowRight size={14} />
        </div>

        {/* Target group / deny */}
        <div>
          <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            {form.ruleType === "regex" && form.regexAction === "deny" ? "Outcome" : "Route to group"}
          </label>
          {form.ruleType === "regex" && form.regexAction === "deny" ? (
            <span className="inline-flex items-center px-2.5 py-1.5 text-xs font-semibold rounded-lg bg-amber-50 text-amber-800 ring-1 ring-amber-200">
              Deny query
            </span>
          ) : (
            <GroupSelectById
              valueId={form.targetGroupId}
              onChangeId={(id) => setForm((f) => ({ ...f, targetGroupId: id }))}
              groups={groups}
            />
          )}
        </div>
      </div>

      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={() => {
            if (isValid) onAdd(form);
          }}
          disabled={!isValid}
          className="px-3 py-1.5 rounded-lg bg-indigo-600 text-white text-xs font-semibold hover:bg-indigo-700 disabled:opacity-40 transition-colors"
        >
          Add rule
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-500 hover:bg-slate-100 transition-colors"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sortable routing chain row (dnd-kit)
// ---------------------------------------------------------------------------

function SortableChainRow({
  item,
  index,
  total,
  groups,
  onMoveUp,
  onMoveDown,
  onRemove,
  onEditCompound,
  onEditPython,
  onSetTargetGroup,
  onSetDenyError,
}: {
  item: ChainItem;
  index: number;
  total: number;
  groups: ClusterGroupOption[];
  onMoveUp: () => void;
  onMoveDown: () => void;
  onRemove: () => void;
  onEditCompound: () => void;
  onEditPython: () => void;
  onSetTargetGroup: (id: number | null) => void;
  onSetDenyError: (error: string) => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    setActivatorNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: item.id });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    position: "relative",
    zIndex: isDragging ? 2 : undefined,
  };

  return (
    <tr
      ref={setNodeRef}
      style={style}
      className={`align-top ${isDragging ? "opacity-60 shadow-sm ring-1 ring-indigo-200" : "bg-white hover:bg-slate-50"}`}
    >
      <td className="px-2 py-2.5">
        <div className="flex items-center gap-0.5">
          <button
            type="button"
            ref={setActivatorNodeRef}
            className="p-0.5 rounded text-slate-400 hover:bg-slate-200 hover:text-slate-600 cursor-grab active:cursor-grabbing touch-none shrink-0"
            title="Drag to reorder evaluation order"
            aria-label="Drag to reorder rule"
            {...attributes}
            {...listeners}
          >
            <GripVertical size={15} aria-hidden />
          </button>
          <span
            className="tabular-nums min-w-[1.25rem] text-center text-[11px] font-bold text-slate-500"
            title="Evaluation step (1 = first)"
          >
            {index + 1}
          </span>
          <div className="flex flex-col gap-0.5 shrink-0">
            <button
              type="button"
              disabled={index === 0}
              onClick={onMoveUp}
              className="p-0.5 rounded text-slate-400 hover:bg-slate-100 hover:text-slate-700 disabled:opacity-25"
              title="Move earlier — evaluated sooner"
            >
              <ChevronUp size={14} />
            </button>
            <button
              type="button"
              disabled={index === total - 1}
              onClick={onMoveDown}
              className="p-0.5 rounded text-slate-400 hover:bg-slate-100 hover:text-slate-700 disabled:opacity-25"
              title="Move later — evaluated after the next rule"
            >
              <ChevronDown size={14} />
            </button>
          </div>
        </div>
      </td>
      <td className="px-2 py-2.5">
        <ChainItemTypeBadge item={item} />
      </td>
      <td className="px-4 py-2.5 text-slate-600 font-mono">
        {item.kind === "compound" ? (
          <div className="space-y-1">
            <span className="text-slate-500">{compoundBlockSummary(chainCompoundToBlock(item))}</span>
            <ul className="text-[10px] text-slate-400 font-normal list-disc pl-3.5 space-y-0.5">
              {chainCompoundToBlock(item).conditions.map((c) => (
                <li key={c.id}>{compoundCondSummary(c)}</li>
              ))}
            </ul>
          </div>
        ) : (
          chainItemConditionSummary(item, PROTOCOLS)
        )}
      </td>
      <td className="px-2 py-2.5 text-slate-300">
        <ArrowRight size={12} />
      </td>
      <td className="px-4 py-2.5">
        {item.kind === "pythonScript" || item.kind === "passthrough" ? (
          <span className="text-slate-400 text-[11px]">
            {item.kind === "pythonScript" ? "— (group from script)" : "— (see raw config)"}
          </span>
        ) : item.kind === "regex" && item.action === "deny" ? (
          <div className="space-y-1">
            <span className="inline-flex items-center px-2 py-0.5 text-[11px] font-semibold rounded-md bg-amber-50 text-amber-800 ring-1 ring-amber-200">
              Deny
            </span>
            <input
              aria-label="Denial message"
              className="w-full max-w-[220px] px-2 py-1 text-[11px] rounded-md border border-slate-200 bg-white text-slate-700 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              placeholder="Query denied by routing rule"
              value={item.error}
              onChange={(e) => onSetDenyError(e.target.value)}
            />
          </div>
        ) : (
          <GroupSelectById
            valueId={item.targetGroupId}
            onChangeId={onSetTargetGroup}
            groups={groups}
          />
        )}
      </td>
      <td className="px-2 py-2.5 text-right">
        <div className="flex items-center justify-end gap-0.5">
          {item.kind === "compound" && (
            <button
              type="button"
              onClick={onEditCompound}
              className="p-1.5 rounded-lg text-slate-300 hover:text-indigo-600 hover:bg-indigo-50 transition-colors"
              title="Edit"
            >
              <Pencil size={12} />
            </button>
          )}
          {item.kind === "pythonScript" && (
            <button
              type="button"
              onClick={onEditPython}
              className="p-1.5 rounded-lg text-slate-300 hover:text-indigo-600 hover:bg-indigo-50 transition-colors"
              title="Edit Python script"
            >
              <Pencil size={12} />
            </button>
          )}
          <button
            type="button"
            onClick={onRemove}
            className="p-1.5 rounded-lg text-slate-300 hover:text-red-500 hover:bg-red-50 transition-colors"
            title="Remove"
          >
            <Trash2 size={12} />
          </button>
        </div>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Main editor
// ---------------------------------------------------------------------------

interface RoutingEditorProps {
  initialRouting: RoutingConfigDto | null;
  groups: ClusterGroupOption[];
}

export function RoutingEditor({ initialRouting, groups }: RoutingEditorProps) {
  const groupByName = React.useMemo(
    () => new Map(groups.map((g) => [g.name, g.id] as const)),
    [groups],
  );

  const initialFallbackId = React.useMemo((): number | null => {
    const r = initialRouting;
    if (!r) return null;
    if (typeof r.routingFallbackGroupId === "number") return r.routingFallbackGroupId;
    const n = r.routingFallback ?? r.routing_fallback ?? "";
    if (n && groupByName.has(n)) return groupByName.get(n)!;
    return null;
  }, [initialRouting, groupByName]);

  const [routingFallbackId, setRoutingFallbackId] = useState<number | null>(initialFallbackId);
  React.useEffect(() => {
    setRoutingFallbackId(initialFallbackId);
  }, [initialFallbackId]);

  const [chainItems, setChainItems] = useState<ChainItem[]>(() =>
    routersToChainItems(initialRouting?.routers ?? [], groupByName),
  );

  React.useEffect(() => {
    setChainItems(routersToChainItems(initialRouting?.routers ?? [], groupByName));
  }, [initialRouting, groupByName]);

  const [addingRule, setAddingRule] = useState(false);
  const [routingSaving, setRoutingSaving] = useState(false);
  const [routingMsg, setRoutingMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [compoundDialogOpen, setCompoundDialogOpen] = useState(false);
  const [compoundDialogEditId, setCompoundDialogEditId] = useState<string | null>(null);

  const [pythonDialogOpen, setPythonDialogOpen] = useState(false);
  const [pythonDialogEditId, setPythonDialogEditId] = useState<string | null>(null);
  const [pythonDialogInitialScript, setPythonDialogInitialScript] = useState("");

  function closePythonDialog() {
    setPythonDialogOpen(false);
    setPythonDialogEditId(null);
    setPythonDialogInitialScript("");
  }

  function openPythonDialogCreate() {
    setPythonDialogEditId(null);
    setPythonDialogInitialScript(ROUTING_SCRIPT_TEMPLATE);
    setPythonDialogOpen(true);
  }

  function openPythonDialogEdit(id: string, script: string) {
    setPythonDialogEditId(id);
    setPythonDialogInitialScript(script);
    setPythonDialogOpen(true);
  }

  const compoundDialogMode: "create" | "edit" =
    compoundDialogEditId !== null &&
    chainItems.some((x) => x.kind === "compound" && x.id === compoundDialogEditId)
      ? "edit"
      : "create";

  const compoundDialogInitial: CompoundRuleBlock | null =
    compoundDialogMode === "edit" && compoundDialogEditId
      ? (() => {
          const row = chainItems.find(
            (x): x is Extract<ChainItem, { kind: "compound" }> =>
              x.kind === "compound" && x.id === compoundDialogEditId,
          );
          return row ? chainCompoundToBlock(row) : null;
        })()
      : null;

  function moveChainItem(index: number, delta: -1 | 1) {
    setChainItems((items) => {
      const j = index + delta;
      if (j < 0 || j >= items.length) return items;
      const next = [...items];
      [next[index], next[j]] = [next[j], next[index]];
      return next;
    });
  }

  const routingDragSensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 6 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  function handleRoutingDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    setChainItems((items) => {
      const oldIndex = items.findIndex((x) => x.id === active.id);
      const newIndex = items.findIndex((x) => x.id === over.id);
      if (oldIndex < 0 || newIndex < 0) return items;
      return arrayMove(items, oldIndex, newIndex);
    });
  }

  function setChainItemTargetGroup(id: string, targetGroupId: number | null) {
    setChainItems((items) =>
      items.map((x) => {
        if (x.id !== id) return x;
        switch (x.kind) {
          case "header":
          case "user":
          case "tag":
          case "regex":
          case "protocol":
          case "compound":
            return { ...x, targetGroupId };
          default:
            return x;
        }
      }),
    );
  }

  function setChainItemDenyError(id: string, error: string) {
    setChainItems((items) =>
      items.map((x) => (x.id === id && x.kind === "regex" ? { ...x, error } : x)),
    );
  }

  const saveRoutingConfig = async () => {
    setRoutingSaving(true);
    setRoutingMsg(null);
    try {
      for (const item of chainItems) {
        if (item.kind === "regex" && item.regex.trim() !== "") {
          try {
            new RegExp(item.regex);
          } catch {
            throw new Error(`Invalid regex pattern: ${item.regex}`);
          }
        }
      }
      await putRoutingConfig({
        routingFallbackGroupId: routingFallbackId,
        routingFallback: "",
        routers: chainItemsToRouters(chainItems),
      });
      setRoutingMsg({ text: "Saved. The proxy reloads routing automatically.", ok: true });
    } catch (e) {
      setRoutingMsg({ text: String(e), ok: false });
    } finally {
      setRoutingSaving(false);
    }
  };

  const hasRules = chainItems.length > 0 || addingRule;

  return (
    <div className="p-8 max-w-5xl space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Routing</h1>
        <p className="text-sm text-slate-500 mt-1">
          Request routing rules and fallback group. Rules are evaluated <strong>top to bottom</strong>; the{" "}
          <strong>first</strong> rule that selects a group or denies the query wins. Reorder by dragging the grip in the first
          column (or use ↑ ↓).{" "}
          <Link href="/security" className="text-indigo-600 hover:text-indigo-700 font-medium">
            Security &amp; auth
          </Link>{" "}
          · Python routing lives in the chain as <strong>Python script</strong> steps. Translation fixups stay on
          the{" "}
          <Link href="/scripts" className="text-indigo-600 hover:text-indigo-700 font-medium">
            Scripts
          </Link>{" "}
          page.
        </p>
      </div>

      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Route size={15} />} title="Routing rules" />
        <div className="p-6 space-y-5">
          <div>
            <Field label="Routing fallback group">
              <GroupSelectById
                valueId={routingFallbackId}
                onChangeId={setRoutingFallbackId}
                groups={groups}
              />
            </Field>
            <p className="text-[11px] text-slate-400 mt-1">
              Used when no routing rule matches.
            </p>
          </div>

          {!hasRules ? (
            <p className="text-sm text-slate-400 italic">
              No routing rules — all requests use the fallback group.
            </p>
          ) : (
            <div className="space-y-2">
              {chainItems.length > 0 && (
                <p className="text-[11px] text-slate-500 leading-relaxed">
                  <span className="font-semibold text-slate-600">Evaluation order:</span> step{" "}
                  <code className="text-slate-600 bg-slate-100 px-1 rounded">1</code> runs first. Each rule either
                  picks a group or passes; the first pick is used and the rest of the chain is skipped. Drag the
                  grip handle to reorder, or use ↑ ↓.
                </p>
              )}
              <div className="rounded-lg border border-slate-200 overflow-hidden">
                <DndContext
                  sensors={routingDragSensors}
                  collisionDetection={closestCenter}
                  onDragEnd={handleRoutingDragEnd}
                >
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-slate-50 border-b border-slate-200">
                        <th className="text-left px-2 py-2 w-[5.75rem] text-[10px] font-semibold text-slate-400 uppercase">
                          Order
                        </th>
                        <th className="text-left px-2 py-2 text-[10px] font-semibold text-slate-400 uppercase w-[128px]">
                          Type
                        </th>
                        <th className="text-left px-4 py-2 text-[10px] font-semibold text-slate-400 uppercase">
                          Condition
                        </th>
                        <th className="px-2 py-2 w-6" />
                        <th className="text-left px-4 py-2 text-[10px] font-semibold text-slate-400 uppercase w-[200px]">
                          Route to group
                        </th>
                        <th className="px-2 py-2 w-[72px] text-right text-[10px] font-semibold text-slate-400 uppercase">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      <SortableContext
                        items={chainItems.map((c) => c.id)}
                        strategy={verticalListSortingStrategy}
                      >
                        {chainItems.map((item, i) => (
                          <SortableChainRow
                            key={item.id}
                            item={item}
                            index={i}
                            total={chainItems.length}
                            groups={groups}
                            onMoveUp={() => moveChainItem(i, -1)}
                            onMoveDown={() => moveChainItem(i, 1)}
                            onRemove={() =>
                              setChainItems((rows) => rows.filter((r) => r.id !== item.id))
                            }
                            onEditCompound={() => {
                              setCompoundDialogEditId(item.id);
                              setCompoundDialogOpen(true);
                            }}
                            onEditPython={() => {
                              if (item.kind !== "pythonScript") return;
                              openPythonDialogEdit(item.id, item.script);
                            }}
                            onSetTargetGroup={(id) => setChainItemTargetGroup(item.id, id)}
                            onSetDenyError={(error) => setChainItemDenyError(item.id, error)}
                          />
                        ))}
                      </SortableContext>
                    </tbody>
                  </table>
                </DndContext>
              </div>
            </div>
          )}

          <CompoundRuleDialog
            open={compoundDialogOpen}
            mode={compoundDialogMode}
            initial={compoundDialogInitial}
            groups={groups}
            onClose={() => {
              setCompoundDialogOpen(false);
              setCompoundDialogEditId(null);
            }}
            onSave={(block) => {
              const row = compoundBlockToChainItem(block);
              if (compoundDialogMode === "edit") {
                setChainItems((items) => items.map((x) => (x.id === block.id ? row : x)));
              } else {
                setChainItems((items) => [...items, row]);
              }
            }}
          />

          {pythonDialogOpen && (
            <PythonRoutingScriptDialog
              key={`${pythonDialogEditId ?? "new"}:${pythonDialogInitialScript}`}
              open={pythonDialogOpen}
              onOpenChange={(o) => {
                if (!o) closePythonDialog();
                else setPythonDialogOpen(true);
              }}
              editChainItemId={pythonDialogEditId}
              initialScript={pythonDialogInitialScript}
              onCommit={(script, editId) => {
                setChainItems((items) => {
                  if (editId) {
                    return items.map((x) =>
                      x.id === editId && x.kind === "pythonScript" ? { ...x, script } : x,
                    );
                  }
                  return [
                    ...items,
                    { id: uid(), kind: "pythonScript", script, script_file: null },
                  ];
                });
              }}
            />
          )}

          {addingRule ? (
            <NewRuleForm
              onAdd={(rule) => {
                setChainItems((items) => [...items, flatRuleToChainItem(rule)]);
                setAddingRule(false);
              }}
              onCancel={() => setAddingRule(false)}
              groups={groups}
            />
          ) : (
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => setAddingRule(true)}
                className="flex items-center gap-1.5 text-xs font-medium text-indigo-600 hover:text-indigo-700 border border-indigo-200 rounded-lg px-3 py-2 hover:bg-indigo-50 transition-colors"
              >
                <Plus size={12} /> Add rule
              </button>
              <button
                type="button"
                onClick={() => {
                  setCompoundDialogEditId(null);
                  setCompoundDialogOpen(true);
                }}
                className="flex items-center gap-1.5 text-xs font-medium text-rose-700 hover:text-rose-800 border border-rose-200 rounded-lg px-3 py-2 hover:bg-rose-50 transition-colors"
              >
                <Layers size={12} /> Multi-condition route…
              </button>
              <button
                type="button"
                onClick={openPythonDialogCreate}
                className="flex items-center gap-1.5 text-xs font-medium text-fuchsia-800 hover:text-fuchsia-900 border border-fuchsia-200 rounded-lg px-3 py-2 hover:bg-fuchsia-50 transition-colors"
              >
                <Code2 size={12} /> Python script router
              </button>
            </div>
          )}

          {chainItems.length > 0 && (
            <p className="text-[11px] text-slate-400">
              Rules run <strong>top to bottom</strong> as configured. The first step that returns a cluster
              group wins. Python script steps use <code>def route(query, ctx)</code> and return a group name
              or <code>None</code>.
            </p>
          )}

          <SaveBar
            saving={routingSaving}
            message={routingMsg}
            onSave={saveRoutingConfig}
            label="Save routing rules"
          />
        </div>
      </section>

      {!initialRouting && (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center">
          <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
            <Route size={18} className="text-slate-400" />
          </div>
          <p className="text-sm font-medium text-slate-500">Could not load routing config</p>
          <p className="text-xs text-slate-400 mt-1">Ensure the proxy admin API is reachable</p>
        </div>
      )}
    </div>
  );
}
