#!/usr/bin/env python3
"""Record a main-branch proxy benchmark run into the `benchmark-data` branch checkout, and
check it for a jump against recent main history.

Writes into <data-dir>/proxy/:
  latest.json   — {"commit", "date", "runner", "metrics": {name: value}} for the newest run
  history.json  — list of the same objects, oldest first, capped at --keep entries

The website build (deploy-website.yml) copies both into website/src/data/benchmarks/.

Jump check — PRs only get the A/B benchmark on request, so main is the safety net. Every
main run lands on a different shared runner, so one run is compared against the last
--window main runs rather than the previous one. *Via QueryFlux p50* jumped when it is BOTH
at least --jump-pct above the window median AND above every run in the window: a noisy
runner rarely beats ten earlier ones by that margin, while a real step change does. Needs
--min-history earlier runs. When it fires, a Markdown note goes to --report-out and
`jump=true` to $GITHUB_OUTPUT (the workflow only warns).

Usage: publish.py <head-median.json> <data-dir> --commit SHA [--report-out FILE]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics

GATED_SUFFIX = " — Via QueryFlux p50 (ms)"


def find_jumps(history, metrics, window, min_history, jump_pct):
    """[(engine, value, median, max)] for gated metrics that jumped vs the last `window` runs."""
    recent = history[-window:]
    if len(recent) < min_history:
        return []
    jumps = []
    for name, value in metrics.items():
        if not name.endswith(GATED_SUFFIX):
            continue
        past = [h["metrics"][name] for h in recent if name in h.get("metrics", {})]
        if len(past) < min_history:
            continue
        med, top = statistics.median(past), max(past)
        if value >= med * (1 + jump_pct / 100) and value > top:
            jumps.append((name[: -len(GATED_SUFFIX)], value, med, top))
    return jumps


def jump_report(jumps, commit, window, jump_pct) -> str:
    rows = "\n".join(
        f"| {engine} | {med * 1000:.0f} µs | {top * 1000:.0f} µs | **{value * 1000:.0f} µs** "
        f"| {(value / med - 1) * 100:+.1f}% |"
        for engine, value, med, top in jumps
    )
    return f"""<!-- queryflux-proxy-bench-jump -->
### ⚠️ Proxy overhead jumped after this merge

The proxy benchmark on `main` at `{commit[:12]}` is at least {jump_pct:g}% above the median of
the last {window} `main` runs **and** higher than all of them (*Via QueryFlux p50*):

| Engine | median (last {window}) | max (last {window}) | this merge | vs median |
| --- | ---: | ---: | ---: | ---: |
{rows}

This may be runner noise — if the next merges come back down, ignore it. If it sticks, re-run
the A/B on a follow-up PR with the `benchmark` label to confirm. History:
https://queryflux.dev/benchmarks
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("median_json", help="compare.py --json-out output")
    ap.add_argument("data_dir")
    ap.add_argument("--commit", required=True)
    ap.add_argument("--date", default=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))
    ap.add_argument("--keep", type=int, default=200)
    ap.add_argument("--runner", default="GitHub Actions ubuntu-latest")
    ap.add_argument("--window", type=int, default=10)
    ap.add_argument("--min-history", type=int, default=5)
    ap.add_argument("--jump-pct", type=float, default=15.0)
    ap.add_argument("--report-out", help="write a Markdown note here when a jump is found")
    args = ap.parse_args()

    with open(args.median_json) as f:
        metrics = {e["name"]: e["value"] for e in json.load(f)}
    entry = {"commit": args.commit, "date": args.date, "runner": args.runner, "metrics": metrics}

    out = os.path.join(args.data_dir, "proxy")
    os.makedirs(out, exist_ok=True)
    history_path = os.path.join(out, "history.json")
    history = []
    if os.path.exists(history_path):
        with open(history_path) as f:
            history = json.load(f)
    # Re-runs of the same commit replace its entry instead of duplicating it.
    history = [h for h in history if h.get("commit") != args.commit]

    # Only compare like with like: runs from the same kind of runner.
    comparable = [h for h in history if h.get("runner") == args.runner]
    jumps = find_jumps(comparable, metrics, args.window, args.min_history, args.jump_pct)
    if jumps and args.report_out:
        with open(args.report_out, "w") as f:
            f.write(jump_report(jumps, args.commit, args.window, args.jump_pct))
    if gh_out := os.environ.get("GITHUB_OUTPUT"):
        with open(gh_out, "a") as f:
            f.write(f"jump={'true' if jumps else 'false'}\n")
    for engine, value, med, _ in jumps:
        print(f"jump: {engine} via-QueryFlux p50 {value:.4f} ms vs median {med:.4f} ms")

    history.append(entry)
    history = history[-args.keep :]

    with open(history_path, "w") as f:
        json.dump(history, f, indent=1, ensure_ascii=False)
        f.write("\n")
    with open(os.path.join(out, "latest.json"), "w") as f:
        json.dump(entry, f, indent=2, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()
