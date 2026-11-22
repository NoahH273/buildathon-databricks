#!/usr/bin/env python3
"""Summarize nested event timings preserved by the unchanged shared harness.

Usage: python3 rust-pipeline/scripts/summarize_event_profiles.py RESULTS_JSON...
Only successful profiling runs enter these tables; ordinary measurements are excluded.
"""
import json
from pathlib import Path
import statistics
import sys

def summarize(path):
    payload = json.loads(path.read_text())
    print(f"# Detailed event timings: {payload['metadata']['name']}\n")
    print(f"Source: `{path}`\n")
    profiles = [r for r in payload["runs"] if r["kind"] == "profile" and r["status"] == "passed"]
    for dataset in sorted({r["dataset"] for r in profiles}):
        phases = dict.fromkeys(stage["name"] for run in profiles if run["dataset"] == dataset
                               for stage in run["checkpoints"]["stages"]
                               if stage["metrics"].get("detailed_event_profile"))
        if not phases:
            raise ValueError(f"{dataset}: no detailed profile metrics")
        for phase in phases:
            details = [stage["metrics"] for run in profiles if run["dataset"] == dataset
                       for stage in run["checkpoints"]["stages"] if stage["name"] == phase
                       and stage["metrics"].get("detailed_event_profile")]
            if not details:
                raise ValueError(f"{dataset}/{phase}: no detailed profile metrics")
            wall = statistics.median(d["phase_seconds"] for d in details)
            timed = statistics.median(d["timed_operations_seconds"] for d in details)
            residual = statistics.median(d["unattributed_seconds"] for d in details)
            print(f"## {dataset}: {phase}\n")
            print(f"Profiles: {len(details)}. Median phase: **{wall:.6f} s**; "
                  f"timed operations: {timed:.6f} s; residual: {residual:.6f} s.\n")
            print("| Subphase | Median seconds | Share of phase | Median calls |")
            print("| --- | ---: | ---: | ---: |")
            names = {s["name"] for d in details for s in d["subphases"]}
            rows = []
            for name in names:
                matching = [s for d in details for s in d["subphases"] if s["name"] == name]
                seconds = statistics.median(s["seconds"] for s in matching)
                calls = statistics.median(s["calls"] for s in matching)
                rows.append((name, seconds, calls))
            for name, seconds, calls in sorted(rows, key=lambda r: (-r[1], r[0])):
                print(f"| {name} | {seconds:.6f} | {100*seconds/wall:.1f}% | {calls:,.0f} |")
            print(f"| Untimed control flow / timer bookkeeping | {residual:.6f} | "
                  f"{100*residual/wall:.1f}% | — |\n")
    print("Operation timings include clock-read cost. Residual also includes uninstrumented "
          "control flow, so it is not a pure overhead estimate. Medians are computed "
          "independently and need not add exactly. These are diagnostic measurements; "
          "ordinary benchmark samples disable detailed timers.\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for argument in sys.argv[1:]:
        summarize(Path(argument))
