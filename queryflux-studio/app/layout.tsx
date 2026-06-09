import React from "react";
import { Geist, Geist_Mono } from "next/font/google";
import type { Metadata } from "next";
import { ClientShell } from "./client-shell";
import "./globals.css";
import "swagger-ui-react/swagger-ui.css";

export const metadata: Metadata = {
  title: "QueryFlux Studio",
  description: "QueryFlux management console",
};

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${geistSans.variable} ${geistMono.variable} h-full`}>
      <body className="h-full flex min-h-0 bg-slate-50 text-slate-900 antialiased">
        <ClientShell>{children}</ClientShell>
      </body>
    </html>
  );
}
