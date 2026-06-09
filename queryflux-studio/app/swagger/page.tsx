"use client";

import dynamic from "next/dynamic";

// swagger-ui-react uses browser APIs — load it client-side only.
const SwaggerUI = dynamic(() => import("swagger-ui-react"), { ssr: false });

export default function SwaggerPage() {
  return (
    <div className="flex flex-col min-h-full bg-white">
      <div className="px-8 py-6 border-b border-slate-100">
        <h1 className="text-xl font-bold text-slate-900">API Reference</h1>
        <p className="text-sm text-slate-500 mt-1">
          Interactive documentation for the QueryFlux admin API.
        </p>
      </div>
      <div className="flex-1 px-4 py-4 swagger-wrapper">
        <SwaggerUI url="/api/admin-proxy/openapi.json" />
      </div>
    </div>
  );
}
