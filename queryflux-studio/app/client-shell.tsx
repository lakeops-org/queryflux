"use client";

import React, { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import {
  Bot,
  BookOpen,
  FileCode,
  LayoutDashboard,
  List,
  Layers,
  Radio,
  Route,
  Server,
  Shield,
  ShieldCheck,
  Zap,
} from "lucide-react";
import { LoginDialog } from "@/components/login-dialog";
import { clearCredentials, fetchSessionStatus } from "@/lib/auth";

const nav = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/clusters", label: "Clusters", icon: Server },
  { href: "/engines", label: "Groups", icon: Layers },
  { href: "/scripts", label: "Scripts", icon: FileCode },
  { href: "/queries", label: "Query History", icon: List },
  { href: "/agents", label: "Agents", icon: Bot },
  { href: "/security", label: "Security", icon: Shield },
  { href: "/guardrails", label: "Guardrails", icon: ShieldCheck },
  { href: "/protocols", label: "Protocols", icon: Radio },
  { href: "/routing", label: "Routing", icon: Route },
  { href: "/swagger", label: "API Reference", icon: BookOpen },
];

export function ClientShell({ children }: { children: React.ReactNode }) {
  const [authenticated, setAuthenticated] = useState(false);
  const [checked, setChecked] = useState(false);

  // On mount, check HttpOnly session via server (no credentials in JS).
  useEffect(() => {
    fetchSessionStatus()
      .then((s) => setAuthenticated(s.authenticated))
      .catch(() => setAuthenticated(false))
      .finally(() => setChecked(true));
  }, []);

  /** Full navigation to `/` so server components reload with the HttpOnly session cookie. */
  const handleLoginSuccess = useCallback(() => {
    window.location.assign("/");
  }, []);

  const handleLogout = useCallback(() => {
    void clearCredentials().then(() => setAuthenticated(false));
  }, []);

  // If any API call fires a qf:unauthorized event, force re-login.
  useEffect(() => {
    function onUnauthorized() {
      void clearCredentials().then(() => setAuthenticated(false));
    }
    window.addEventListener("qf:unauthorized", onUnauthorized);
    return () => window.removeEventListener("qf:unauthorized", onUnauthorized);
  }, []);

  // Suppress the flash-of-login-screen on first load.
  if (!checked) return null;

  return (
    <div className="flex min-h-0 w-full flex-1 flex-row">
      {!authenticated && <LoginDialog onSuccess={handleLoginSuccess} />}

      {/* Sidebar */}
      <aside className="w-60 flex-shrink-0 bg-white border-r border-slate-200 flex flex-col shadow-sm">
        {/* Brand */}
        <div className="flex items-center gap-3 px-5 py-5 border-b border-slate-100">
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-sm flex-shrink-0">
            <Zap size={16} className="text-white" />
          </div>
          <div>
            <span className="font-bold text-sm text-slate-900 tracking-tight leading-none block">
              QueryFlux
            </span>
            <span className="text-[11px] text-slate-400 font-medium tracking-wide">Studio</span>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-0.5">
          <p className="px-3 py-1 text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
            Menu
          </p>
          {nav.map(({ href, label, icon: Icon }) => (
            <Link
              key={href}
              href={href}
              className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-600 hover:bg-indigo-50 hover:text-indigo-700 transition-all duration-150 group"
            >
              <Icon size={16} className="text-slate-400 group-hover:text-indigo-500 transition-colors" />
              <span className="font-medium">{label}</span>
            </Link>
          ))}
        </nav>

        {/* Footer */}
        <div className="px-5 py-4 border-t border-slate-100">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-emerald-400"></div>
              <span className="text-xs text-slate-400 font-medium">v0.1.0 · Connected</span>
            </div>
            {authenticated && (
              <button
                onClick={handleLogout}
                className="text-[10px] text-slate-400 hover:text-slate-700 transition-colors font-medium"
              >
                Sign out
              </button>
            )}
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 min-h-0 overflow-auto bg-slate-50">{children}</main>
    </div>
  );
}
