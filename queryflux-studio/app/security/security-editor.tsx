"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { getAuthStatus, putSecurityConfig } from "@/lib/api";
import type { SecurityConfigDto, UpsertSecurityConfig, GroupAuthzDto } from "@/lib/api-types";
import { Field, SectionHeader, TextInput, SaveBar } from "@/components/studio-settings";
import {
  CheckCircle2,
  Key,
  Lock,
  Plus,
  Shield,
  Trash2,
  User,
  Users,
  XCircle,
} from "lucide-react";

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface Props {
  initialSecurity: SecurityConfigDto | null;
}

// ---------------------------------------------------------------------------
// Static users editor
// ---------------------------------------------------------------------------

interface StaticUserEntry {
  username: string;
  password: string;
  groups: string;
  roles: string;
}

function StaticUsersEditor({
  value,
  onChange,
}: {
  value: Record<string, { password: string; groups?: string[]; roles?: string[] }>;
  onChange: (v: Record<string, { password: string; groups?: string[]; roles?: string[] }>) => void;
}) {
  const [newUser, setNewUser] = useState<StaticUserEntry>({
    username: "",
    password: "",
    groups: "",
    roles: "",
  });

  const users: StaticUserEntry[] = Object.entries(value).map(([username, u]) => ({
    username,
    password: u.password,
    groups: (u.groups ?? []).join(", "),
    roles: (u.roles ?? []).join(", "),
  }));

  const commitNew = () => {
    if (!newUser.username.trim()) return;
    const next = { ...value };
    next[newUser.username.trim()] = {
      password: newUser.password,
      groups: newUser.groups ? newUser.groups.split(",").map((s) => s.trim()).filter(Boolean) : [],
      roles: newUser.roles ? newUser.roles.split(",").map((s) => s.trim()).filter(Boolean) : [],
    };
    onChange(next);
    setNewUser({ username: "", password: "", groups: "", roles: "" });
  };

  const removeUser = (username: string) => {
    const next = { ...value };
    delete next[username];
    onChange(next);
  };

  const updateUser = (username: string, field: keyof StaticUserEntry, val: string) => {
    const next = { ...value };
    const existing = next[username] ?? { password: "" };
    if (field === "password") {
      next[username] = { ...existing, password: val };
    } else if (field === "groups") {
      next[username] = {
        ...existing,
        groups: val.split(",").map((s) => s.trim()).filter(Boolean),
      };
    } else if (field === "roles") {
      next[username] = {
        ...existing,
        roles: val.split(",").map((s) => s.trim()).filter(Boolean),
      };
    }
    onChange(next);
  };

  return (
    <div className="space-y-1">
      <div className="grid grid-cols-[1fr_1fr_1fr_1fr_auto] gap-2 text-[10px] font-semibold text-slate-400 uppercase tracking-widest px-1 mb-1">
        <span>Username</span>
        <span>Password</span>
        <span>Groups (comma-sep)</span>
        <span>Roles (comma-sep)</span>
        <span />
      </div>
      {users.map((u) => (
        <div key={u.username} className="grid grid-cols-[1fr_1fr_1fr_1fr_auto] gap-2 items-center">
          <input
            className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-slate-50 font-mono focus:outline-none"
            value={u.username}
            readOnly
          />
          <input
            type="password"
            className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            placeholder="(leave blank to keep)"
            onChange={(e) => updateUser(u.username, "password", e.target.value)}
          />
          <input
            className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
            defaultValue={u.groups}
            onBlur={(e) => updateUser(u.username, "groups", e.target.value)}
          />
          <input
            className="px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
            defaultValue={u.roles}
            onBlur={(e) => updateUser(u.username, "roles", e.target.value)}
          />
          <button
            type="button"
            onClick={() => removeUser(u.username)}
            className="p-1.5 rounded-lg text-slate-400 hover:text-red-500 hover:bg-red-50 transition-colors"
          >
            <Trash2 size={12} />
          </button>
        </div>
      ))}
      {/* Add row */}
      <div className="grid grid-cols-[1fr_1fr_1fr_1fr_auto] gap-2 items-center mt-1">
        <input
          className="px-2.5 py-1.5 text-xs rounded-lg border border-dashed border-slate-300 bg-slate-50 text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
          placeholder="username"
          value={newUser.username}
          onChange={(e) => setNewUser((p) => ({ ...p, username: e.target.value }))}
        />
        <input
          type="password"
          className="px-2.5 py-1.5 text-xs rounded-lg border border-dashed border-slate-300 bg-slate-50 focus:outline-none focus:ring-2 focus:ring-indigo-300"
          placeholder="password"
          value={newUser.password}
          onChange={(e) => setNewUser((p) => ({ ...p, password: e.target.value }))}
        />
        <input
          className="px-2.5 py-1.5 text-xs rounded-lg border border-dashed border-slate-300 bg-slate-50 text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
          placeholder="group1, group2"
          value={newUser.groups}
          onChange={(e) => setNewUser((p) => ({ ...p, groups: e.target.value }))}
        />
        <input
          className="px-2.5 py-1.5 text-xs rounded-lg border border-dashed border-slate-300 bg-slate-50 text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
          placeholder="role1, role2"
          value={newUser.roles}
          onChange={(e) => setNewUser((p) => ({ ...p, roles: e.target.value }))}
        />
        <button
          type="button"
          onClick={commitNew}
          className="p-1.5 rounded-lg text-indigo-600 hover:bg-indigo-50 border border-indigo-200 transition-colors"
        >
          <Plus size={12} />
        </button>
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// Main editor component
// ---------------------------------------------------------------------------

export function SecurityEditor({ initialSecurity }: Props) {
  // ── Security form state ─────────────────────────────────────────────────

  const initSecurityForm = (): UpsertSecurityConfig => ({
    auth_provider: initialSecurity?.auth_provider ?? "none",
    auth_required: initialSecurity?.auth_required ?? false,
    oidc: initialSecurity?.oidc
      ? {
          issuer: initialSecurity.oidc.issuer,
          jwks_uri: initialSecurity.oidc.jwks_uri,
          audience: initialSecurity.oidc.audience,
          groups_claim: initialSecurity.oidc.groups_claim,
          roles_claim: initialSecurity.oidc.roles_claim,
        }
      : null,
    ldap: initialSecurity?.ldap
      ? {
          url: initialSecurity.ldap.url,
          bind_dn: initialSecurity.ldap.bind_dn,
          bind_password: null, // never pre-fill secrets
          user_search_base: initialSecurity.ldap.user_search_base,
          user_search_filter: initialSecurity.ldap.user_search_filter,
          user_dn_template: initialSecurity.ldap.user_dn_template,
          group_search_base: initialSecurity.ldap.group_search_base,
          group_name_attribute: initialSecurity.ldap.group_name_attribute,
        }
      : null,
    static_users: null, // passwords never pre-filled
    authorization_provider: initialSecurity?.authorization_provider ?? "none",
    openfga: initialSecurity?.openfga
      ? {
          url: initialSecurity.openfga.url,
          store_id: initialSecurity.openfga.store_id,
          credentials: null, // never pre-fill secrets
        }
      : null,
  });

  const [securityForm, setSecurityForm] = useState<UpsertSecurityConfig>(initSecurityForm);
  const [securitySaving, setSecuritySaving] = useState(false);
  const [securityMsg, setSecurityMsg] = useState<{ text: string; ok: boolean } | null>(null);

  // ── Admin password status (read-only; password changes are not available in Studio) ──

  const [dbOverride, setDbOverride] = useState<boolean | null>(null);

  useEffect(() => {
    getAuthStatus()
      .then((s) => setDbOverride(s.db_override))
      .catch(() => setDbOverride(null));
  }, []);

  const saveSecurityConfig = async () => {
    setSecuritySaving(true);
    setSecurityMsg(null);
    try {
      await putSecurityConfig(securityForm);
      setSecurityMsg({ text: "Saved. The proxy reloads config automatically.", ok: true });
    } catch (e) {
      setSecurityMsg({ text: String(e), ok: false });
    } finally {
      setSecuritySaving(false);
    }
  };

  // ── Group authz (read-only display from initial data) ───────────────────

  const groupAuthz: Record<string, GroupAuthzDto> =
    initialSecurity?.group_authorization ?? {};
  const hasGroupAuthz = Object.keys(groupAuthz).length > 0;

  // ── Render ──────────────────────────────────────────────────────────────

  return (
    <div className="p-8 max-w-5xl space-y-8">

      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Security</h1>
        <p className="text-sm text-slate-500 mt-1">
          Authentication and authorization.{" "}
          <Link href="/routing" className="text-indigo-600 hover:text-indigo-700 font-medium">
            Routing rules
          </Link>
        </p>
      </div>

      {/* ── Admin API Password ───────────────────────────────────────────── */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Key size={15} />} title="Admin API Password" />
        <div className="p-6">
          <p className="text-xs text-slate-600">
            {dbOverride === true
              ? "A password is stored in the database for the admin API (not managed from Studio)."
              : dbOverride === false
                ? "Bootstrap credentials from YAML or environment may be in use. Configure the admin password on the server."
                : "Status unavailable."}
          </p>
        </div>
      </section>

      {/* ── Authentication ───────────────────────────────────────────────── */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Shield size={15} />} title="Authentication" />
        <div className="p-6 space-y-5">
          <div className="grid grid-cols-2 gap-4">
            <Field label="Provider">
              <select
                className="w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                value={securityForm.auth_provider}
                onChange={(e) =>
                  setSecurityForm((f) => ({ ...f, auth_provider: e.target.value }))
                }
              >
                <option value="none">None (network trust)</option>
                <option value="static">Static user list</option>
                <option value="oidc">OIDC / JWT</option>
                <option value="ldap">LDAP</option>
              </select>
            </Field>
            <Field label="Require authentication">
              <div className="flex items-center gap-3 mt-0.5">
                <button
                  type="button"
                  onClick={() =>
                    setSecurityForm((f) => ({ ...f, auth_required: !f.auth_required }))
                  }
                  className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                    securityForm.auth_required ? "bg-indigo-600" : "bg-slate-200"
                  }`}
                >
                  <span
                    className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow-sm transition-transform ${
                      securityForm.auth_required ? "translate-x-4" : "translate-x-0.5"
                    }`}
                  />
                </button>
                <span className="text-xs text-slate-500">
                  {securityForm.auth_required ? (
                    <span className="flex items-center gap-1 text-emerald-600 font-medium">
                      <CheckCircle2 size={12} /> Required
                    </span>
                  ) : (
                    <span className="flex items-center gap-1 text-slate-400">
                      <XCircle size={12} /> Anonymous allowed
                    </span>
                  )}
                </span>
              </div>
            </Field>
          </div>

          {/* None */}
          {securityForm.auth_provider === "none" && (
            <p className="text-xs text-slate-400 italic">
              Identity derived from session username (network trust only).
            </p>
          )}

          {/* Static users */}
          {securityForm.auth_provider === "static" && (
            <div className="space-y-3">
              <p className="text-xs text-slate-500">
                Define users with passwords. Passwords are stored in the database.
                Leave password blank for existing users to keep the current value.
              </p>
              <StaticUsersEditor
                value={securityForm.static_users ?? {}}
                onChange={(v) => setSecurityForm((f) => ({ ...f, static_users: v }))}
              />
            </div>
          )}

          {/* OIDC */}
          {securityForm.auth_provider === "oidc" && (
            <div className="rounded-lg border border-indigo-100 bg-indigo-50 p-4 space-y-3">
              <p className="text-xs font-semibold text-indigo-600 uppercase tracking-widest">
                OIDC
              </p>
              <div className="grid grid-cols-2 gap-3">
                <TextInput
                  label="Issuer"
                  value={securityForm.oidc?.issuer ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      oidc: { ...(f.oidc ?? { issuer: "", jwks_uri: "", groups_claim: "" }), issuer: v },
                    }))
                  }
                  placeholder="https://accounts.example.com"
                />
                <TextInput
                  label="JWKS URI"
                  value={securityForm.oidc?.jwks_uri ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      oidc: { ...(f.oidc ?? { issuer: "", jwks_uri: "", groups_claim: "" }), jwks_uri: v },
                    }))
                  }
                  placeholder="https://accounts.example.com/.well-known/jwks.json"
                />
                <TextInput
                  label="Audience (optional)"
                  value={securityForm.oidc?.audience ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      oidc: { ...(f.oidc ?? { issuer: "", jwks_uri: "", groups_claim: "" }), audience: v || null },
                    }))
                  }
                />
                <TextInput
                  label="Groups claim"
                  value={securityForm.oidc?.groups_claim ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      oidc: { ...(f.oidc ?? { issuer: "", jwks_uri: "", groups_claim: "" }), groups_claim: v },
                    }))
                  }
                  placeholder="groups"
                />
                <TextInput
                  label="Roles claim (optional)"
                  value={securityForm.oidc?.roles_claim ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      oidc: { ...(f.oidc ?? { issuer: "", jwks_uri: "", groups_claim: "" }), roles_claim: v || null },
                    }))
                  }
                />
              </div>
            </div>
          )}

          {/* LDAP */}
          {securityForm.auth_provider === "ldap" && (
            <div className="rounded-lg border border-violet-100 bg-violet-50 p-4 space-y-3">
              <p className="text-xs font-semibold text-violet-600 uppercase tracking-widest">
                LDAP
              </p>
              <div className="grid grid-cols-2 gap-3">
                <TextInput
                  label="URL"
                  value={securityForm.ldap?.url ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: { ...(f.ldap ?? { url: "", user_search_base: "" }), url: v },
                    }))
                  }
                  placeholder="ldap://ldap.example.com:389"
                />
                <TextInput
                  label="Bind DN"
                  value={securityForm.ldap?.bind_dn ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: { ...(f.ldap ?? { url: "", user_search_base: "" }), bind_dn: v },
                    }))
                  }
                  placeholder="cn=reader,dc=example,dc=com"
                />
                <TextInput
                  label="Bind password"
                  type="password"
                  value={securityForm.ldap?.bind_password ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: {
                        ...(f.ldap ?? { url: "", user_search_base: "" }),
                        bind_password: v || null,
                      },
                    }))
                  }
                  placeholder="(leave blank to keep current)"
                />
                <TextInput
                  label="User search base"
                  value={securityForm.ldap?.user_search_base ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: { ...(f.ldap ?? { url: "", user_search_base: "" }), user_search_base: v },
                    }))
                  }
                  placeholder="ou=users,dc=example,dc=com"
                />
                <TextInput
                  label="User search filter"
                  value={securityForm.ldap?.user_search_filter ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: {
                        ...(f.ldap ?? { url: "", user_search_base: "" }),
                        user_search_filter: v,
                      },
                    }))
                  }
                  placeholder="(uid={username})"
                />
                <TextInput
                  label="User DN template (optional)"
                  value={securityForm.ldap?.user_dn_template ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: {
                        ...(f.ldap ?? { url: "", user_search_base: "" }),
                        user_dn_template: v || null,
                      },
                    }))
                  }
                />
                <TextInput
                  label="Group search base (optional)"
                  value={securityForm.ldap?.group_search_base ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: {
                        ...(f.ldap ?? { url: "", user_search_base: "" }),
                        group_search_base: v || null,
                      },
                    }))
                  }
                />
                <TextInput
                  label="Group name attribute"
                  value={securityForm.ldap?.group_name_attribute ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      ldap: {
                        ...(f.ldap ?? { url: "", user_search_base: "" }),
                        group_name_attribute: v,
                      },
                    }))
                  }
                  placeholder="cn"
                />
              </div>
            </div>
          )}

          <SaveBar
            saving={securitySaving}
            message={securityMsg}
            onSave={saveSecurityConfig}
            label="Save authentication"
          />
        </div>
      </section>

      {/* ── Authorization ────────────────────────────────────────────────── */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
        <SectionHeader icon={<Lock size={15} />} title="Authorization" />
        <div className="p-6 space-y-5">
          <Field label="Provider">
            <select
              className="w-full max-w-xs px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
              value={securityForm.authorization_provider}
              onChange={(e) =>
                setSecurityForm((f) => ({ ...f, authorization_provider: e.target.value }))
              }
            >
              <option value="none">None (simple allow-lists per group)</option>
              <option value="openfga">OpenFGA</option>
            </select>
          </Field>

          {/* None — show read-only group allow lists */}
          {securityForm.authorization_provider === "none" && (
            <>
              {hasGroupAuthz ? (
                <div className="space-y-2">
                  <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                    Group access policies (edit via group config)
                  </p>
                  <div className="divide-y divide-slate-100 rounded-lg border border-slate-200 overflow-hidden">
                    {Object.entries(groupAuthz).map(([group, authz]) => (
                      <div
                        key={group}
                        className="flex items-start gap-6 px-4 py-3 bg-white hover:bg-slate-50"
                      >
                        <div className="min-w-[140px]">
                          <span className="text-xs font-semibold text-slate-700 bg-slate-100 px-2 py-0.5 rounded">
                            {group}
                          </span>
                        </div>
                        <div className="flex-1 flex flex-wrap gap-2">
                          {authz.allow_groups.map((g) => (
                            <span
                              key={`g:${g}`}
                              className="inline-flex items-center gap-1 text-[11px] font-medium text-indigo-700 bg-indigo-50 border border-indigo-100 px-2 py-0.5 rounded-md"
                            >
                              <Users size={10} /> {g}
                            </span>
                          ))}
                          {authz.allow_users.map((u) => (
                            <span
                              key={`u:${u}`}
                              className="inline-flex items-center gap-1 text-[11px] font-medium text-emerald-700 bg-emerald-50 border border-emerald-100 px-2 py-0.5 rounded-md"
                            >
                              <User size={10} /> {u}
                            </span>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <p className="text-sm text-slate-400 italic">
                  No per-group restrictions — all authenticated users may access all groups.
                  Configure allow-lists via group configs.
                </p>
              )}
            </>
          )}

          {/* OpenFGA */}
          {securityForm.authorization_provider === "openfga" && (
            <div className="rounded-lg border border-amber-100 bg-amber-50 p-4 space-y-3">
              <p className="text-xs font-semibold text-amber-600 uppercase tracking-widest">
                OpenFGA
              </p>
              <div className="grid grid-cols-2 gap-3">
                <TextInput
                  label="URL"
                  value={securityForm.openfga?.url ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      openfga: { ...(f.openfga ?? { url: "", store_id: "" }), url: v },
                    }))
                  }
                  placeholder="http://openfga:8080"
                />
                <TextInput
                  label="Store ID"
                  value={securityForm.openfga?.store_id ?? ""}
                  onChange={(v) =>
                    setSecurityForm((f) => ({
                      ...f,
                      openfga: { ...(f.openfga ?? { url: "", store_id: "" }), store_id: v },
                    }))
                  }
                  placeholder="01H..."
                />
                <Field label="Credentials method">
                  <select
                    className="w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                    value={securityForm.openfga?.credentials?.method ?? "none"}
                    onChange={(e) => {
                      const method = e.target.value;
                      setSecurityForm((f) => ({
                        ...f,
                        openfga: {
                          ...(f.openfga ?? { url: "", store_id: "" }),
                          credentials:
                            method === "none" ? null : { method },
                        },
                      }));
                    }}
                  >
                    <option value="none">None</option>
                    <option value="api_key">API key</option>
                    <option value="client_credentials">Client credentials</option>
                  </select>
                </Field>
                {securityForm.openfga?.credentials?.method === "api_key" && (
                  <TextInput
                    label="API key"
                    type="password"
                    value={securityForm.openfga?.credentials?.api_key ?? ""}
                    onChange={(v) =>
                      setSecurityForm((f) => ({
                        ...f,
                        openfga: {
                          ...(f.openfga ?? { url: "", store_id: "" }),
                          credentials: {
                            ...(f.openfga?.credentials ?? { method: "api_key" }),
                            api_key: v,
                          },
                        },
                      }))
                    }
                    placeholder="(leave blank to keep)"
                  />
                )}
                {securityForm.openfga?.credentials?.method === "client_credentials" && (
                  <>
                    <TextInput
                      label="Client ID"
                      value={securityForm.openfga?.credentials?.client_id ?? ""}
                      onChange={(v) =>
                        setSecurityForm((f) => ({
                          ...f,
                          openfga: {
                            ...(f.openfga ?? { url: "", store_id: "" }),
                            credentials: {
                              ...(f.openfga?.credentials ?? { method: "client_credentials" }),
                              client_id: v,
                            },
                          },
                        }))
                      }
                    />
                    <TextInput
                      label="Client secret"
                      type="password"
                      value={securityForm.openfga?.credentials?.client_secret ?? ""}
                      onChange={(v) =>
                        setSecurityForm((f) => ({
                          ...f,
                          openfga: {
                            ...(f.openfga ?? { url: "", store_id: "" }),
                            credentials: {
                              ...(f.openfga?.credentials ?? { method: "client_credentials" }),
                              client_secret: v,
                            },
                          },
                        }))
                      }
                      placeholder="(leave blank to keep)"
                    />
                    <TextInput
                      label="Token endpoint"
                      value={securityForm.openfga?.credentials?.token_endpoint ?? ""}
                      onChange={(v) =>
                        setSecurityForm((f) => ({
                          ...f,
                          openfga: {
                            ...(f.openfga ?? { url: "", store_id: "" }),
                            credentials: {
                              ...(f.openfga?.credentials ?? { method: "client_credentials" }),
                              token_endpoint: v,
                            },
                          },
                        }))
                      }
                      placeholder="https://auth.example.com/oauth/token"
                    />
                  </>
                )}
              </div>
            </div>
          )}

          <SaveBar
            saving={securitySaving}
            message={securityMsg}
            onSave={saveSecurityConfig}
            label="Save authorization"
          />
        </div>
      </section>

      {/* Fallback when config is unavailable */}
      {!initialSecurity && (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center">
          <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
            <Shield size={18} className="text-slate-400" />
          </div>
          <p className="text-sm font-medium text-slate-500">Could not load config</p>
          <p className="text-xs text-slate-400 mt-1">Ensure the proxy admin API is reachable</p>
        </div>
      )}
    </div>
  );
}
