#!/usr/bin/env python3
"""Compare proxy-overhead benchmark runs: base vs head (see scripts/bench/proxy-ab.sh).

Reads every ``base-*.json`` / ``head-*.json`` in a directory (queryflux-bench output:
``[{"name", "value", "unit"}, ...]``), takes the per-metric median across rounds, and prints
a Markdown report.

Regression rule — tuned so identical binaries do not alarm on a busy machine:
  * Only *Via QueryFlux p50* is gated. p95 / p99 / mean are shown but hinge on a handful of
    slow requests, and one noisy round swings them by 50%+ even with identical binaries.
  * Rounds are paired (base-N and head-N ran back to back, see proxy-ab.sh). The *Direct*
    rows run identical mock code on both sides, so their median drift is pure runner noise.
  * A metric is flagged when its median per-round change exceeds ``--threshold`` PLUS that
    noise, AND head was slower in all but at most one round — a consistent shift, not one
    bad round. On a quiet runner noise is ~1–3%, so ~12% shifts get caught; on a noisy one
    the bar rises instead of raising false alarms (CodSpeed covers small, precise changes).

Never fails the build: writes ``regressed=true|false`` to $GITHUB_OUTPUT when set, so the
workflow decides what to do with it (we only warn).

Usage: compare.py <results-dir> [--threshold 10] [--title "..."] [--head-only]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

# The only metric that decides "regressed" (see module docstring).
GATED = ("Via QueryFlux p50 (ms)",)
# Rows shown in the report, in order (per engine).
SHOWN = (
    "Via QueryFlux p50 (ms)",
    "Via QueryFlux p95 (ms)",
    "Via QueryFlux p99 (ms)",
    "Via QueryFlux mean (ms)",
    "Overhead p50 (ms)",
    "Overhead mean (ms)",
    "Direct p50 (ms)",
)


def load_rounds(results_dir: str, side: str) -> dict[str, dict[str, float]]:
    """{round id: {metric name: value}} from ``<side>-<round>.json`` files."""
    rounds = {}
    for path in glob.glob(os.path.join(results_dir, f"{side}-*.json")):
        rid = os.path.basename(path)[len(side) + 1 : -len(".json")]
        if not rid.isdigit():
            continue  # e.g. head-median.json written by --json-out
        with open(path) as f:
            rounds[rid] = {e["name"]: float(e["value"]) for e in json.load(f)}
    return rounds


def medians(rounds: dict[str, dict[str, float]]) -> dict[str, float]:
    samples: dict[str, list[float]] = defaultdict(list)
    for metrics in rounds.values():
        for name, value in metrics.items():
            samples[name].append(value)
    return {name: statistics.median(vals) for name, vals in samples.items()}


def split(name: str) -> tuple[str, str]:
    engine, _, metric = name.partition(" — ")
    return engine, metric


def pct(base: float, head: float) -> float:
    return 0.0 if base == 0 else (head - base) / base * 100.0


def fmt_ms(v: float) -> str:
    return f"{v * 1000:.0f} µs" if abs(v) < 1 else f"{v:.2f} ms"


def report_ab(base_rounds, head_rounds, threshold: float, title: str):
    base, head = medians(base_rounds), medians(head_rounds)
    paired = sorted(set(base_rounds) & set(head_rounds), key=int)
    engines = sorted({split(n)[0] for n in head})
    lines = [f"### {title}", ""]
    regressed = []
    for engine in engines:
        direct = f"{engine} — Direct p50 (ms)"
        noise = abs(pct(base.get(direct, 0), head.get(direct, 0)))
        lines += [
            f"**{engine}** · runner noise ≈ {noise:.1f}% (Direct p50 drift; QueryFlux not involved)",
            "",
            "| Metric | base | head | change | rounds slower | |",
            "| --- | ---: | ---: | ---: | :---: | :---: |",
        ]
        for metric in SHOWN:
            name = f"{engine} — {metric}"
            if name not in base or name not in head:
                continue
            per_round = [
                pct(base_rounds[r][name], head_rounds[r][name])
                for r in paired
                if name in base_rounds[r] and name in head_rounds[r]
            ]
            change = statistics.median(per_round) if per_round else pct(base[name], head[name])
            slower = sum(c > 0 for c in per_round)
            consistent = per_round and slower >= len(per_round) - 1
            faster_consistent = per_round and (len(per_round) - slower) >= len(per_round) - 1
            mark = ""
            bar = threshold + noise
            if metric in GATED and change > bar and consistent:
                mark = "🔴"
                regressed.append(f"{engine} {metric.replace(' (ms)', '')} {change:+.1f}%")
            elif metric in GATED and change < -bar and faster_consistent:
                mark = "🟢"
            label = metric.replace(" (ms)", "")
            lines.append(
                f"| {label} | {fmt_ms(base[name])} | {fmt_ms(head[name])} | {change:+.1f}% "
                f"| {slower}/{len(per_round)} | {mark} |"
            )
        lines.append("")
    lines.append(
        f"<sub>Medians of {len(paired)} paired rounds. 🔴/🟢 only on *Via QueryFlux p50*: median "
        f"change beyond ±({threshold:g}% + runner noise) with head slower/faster in all but ≤1 "
        "round.</sub>"
    )
    if regressed:
        lines.insert(
            2,
            f"> [!WARNING]\n> Possible proxy-overhead regression: " + "; ".join(regressed) + "\n",
        )
    else:
        lines.insert(2, f"No consistent proxy-overhead change beyond {threshold:g}% + runner noise.\n")
    return "\n".join(lines), bool(regressed)


def report_head(head: dict[str, float], title: str):
    engines = sorted({split(n)[0] for n in head})
    lines = [f"### {title}", "", "| Engine | Direct p50 | Via QueryFlux p50 | p95 | p99 | Overhead p50 |"]
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for e in engines:
        def g(m: str) -> str:
            v = head.get(f"{e} — {m} (ms)")
            return "—" if v is None else fmt_ms(v)

        lines.append(
            f"| {e} | {g('Direct p50')} | {g('Via QueryFlux p50')} | {g('Via QueryFlux p95')} "
            f"| {g('Via QueryFlux p99')} | {g('Overhead p50')} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir")
    ap.add_argument("--threshold", type=float, default=10.0)
    ap.add_argument("--title", default="Proxy overhead (queryflux-bench, mock backends)")
    ap.add_argument("--head-only", action="store_true", help="single-side summary, no comparison")
    ap.add_argument("--json-out", help="write head medians as a queryflux-bench-shaped JSON array")
    args = ap.parse_args()

    head_rounds = load_rounds(args.results_dir, "head")
    head = medians(head_rounds)
    if not head:
        print(f"no head-*.json in {args.results_dir}", file=sys.stderr)
        return 1

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(
                [
                    {"name": n, "value": round(v, 4), "unit": "%" if n.endswith("(%)") else "ms"}
                    for n, v in head.items()
                ],
                f,
                indent=2,
            )

    regressed = False
    if args.head_only:
        print(report_head(head, args.title))
    else:
        base_rounds = load_rounds(args.results_dir, "base")
        if base_rounds:
            text, regressed = report_ab(base_rounds, head_rounds, args.threshold, args.title)
            print(text)
        else:
            # proxy-ab.sh drops base results when the base binary can't run the head harness.
            print(report_head(head, args.title))
            print("_No base comparison: the base build could not run this PR's benchmark harness._")

    if gh_out := os.environ.get("GITHUB_OUTPUT"):
        with open(gh_out, "a") as f:
            f.write(f"regressed={'true' if regressed else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
