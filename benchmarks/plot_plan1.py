#!/usr/bin/env python3
"""
plot_plan1_by_nodes.py — one bar per node count for the Plan 1 scaling test.

Reads the harness output JSON from the out/ directory (the external results
folder for Plan 1) and draws a bar per node-count step: bar height is delivered
throughput (stored msg/s), colour encodes message loss, and each bar is labelled
with its loss % and p95 latency. A dashed line marks the single-instance ceiling
and a green/red shading separates the healthy region from the saturated one.

Usage
-----
  # default: read out/plan1_results.json, write out/plan1_by_nodes.png
  python3 plot_plan1_by_nodes.py

  # read a specific result file instead (resolved inside OUT if no path given)
  python3 plot_plan1_by_nodes.py plan1_full.json

Environment
-----------
  OUT       results/output directory (default: ./out)
  CEILING   ceiling line in msg/s (default: auto = max stored rate across rows)

The result file is the single JSON written by plan1_throughput.js. One ramp run
writes ALL its steps as rows in that one file, so a full 200..800000 run is a
single source — no merging needed:
  { "config": {...}, "rows": [ { "nodes", "storedRateMsgS", "lossPct",
                                 "latP95ms", ... }, ... ] }
Only these four fields per row are used here.
"""

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import colormaps
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize

OUT = os.environ.get("OUT", "out")


def load_rows(path):
    """Load rows from a single result JSON file, sorted ascending by node count."""
    full = path if os.path.isabs(path) or os.path.dirname(path) else os.path.join(OUT, path)
    with open(full) as fh:
        data = json.load(fh)
    rows = data.get("rows", [])
    return sorted(rows, key=lambda r: int(r["nodes"]))


def fmt_lat(ms):
    """Human-readable latency: ms under a second, whole seconds above."""
    return f"{ms:.0f} ms" if ms < 1000 else f"{ms / 1000:.0f} s"


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "plan1_results.json"
    rows = load_rows(path)
    if not rows:
        sys.exit(f"No rows found in {path}.")

    nodes = [int(r["nodes"]) for r in rows]
    stored = [float(r["storedRateMsgS"]) for r in rows]
    loss = [float(r["lossPct"]) for r in rows]
    p95 = [float(r["latP95ms"]) for r in rows]

    ceiling = float(os.environ.get("CEILING", max(stored)))
    # first index where loss climbs above 5% marks the healthy/saturated split
    knee_i = next((i for i, l in enumerate(loss) if l > 5), len(nodes))

    x = list(range(len(nodes)))
    cmap = colormaps["RdYlGn_r"]
    norm = Normalize(0, 100)
    colors = [cmap(norm(max(0, l))) for l in loss]  # clamp negative loss to 0

    fig, ax = plt.subplots(figsize=(11, 5.6), dpi=130)
    ax.bar(x, stored, color=colors, edgecolor="#333", linewidth=0.6, width=0.72)

    top = max(stored) * 1.25
    for xi, s, l, p in zip(x, stored, loss, p95):
        ax.text(xi, s + top * 0.015,
                f"{l:.0f}% loss\np95 {fmt_lat(p)}",
                ha="center", va="bottom", fontsize=8.5,
                color=("#0a7d1a" if l < 5 else "#7a0f0f"), fontweight="bold")

    # ceiling line
    ax.axhline(ceiling, ls="--", color="#1f77b4", alpha=0.6)
    ax.text(x[0] - 0.4, ceiling + top * 0.015,
            f"single-instance ceiling ≈ {ceiling:.0f} msg/s",
            color="#1f77b4", fontsize=9)

    # healthy / saturated shading (only when both regions exist)
    if 0 < knee_i < len(nodes):
        ax.axvspan(-0.5, knee_i - 0.5, color="#2ca02c", alpha=0.06)
        ax.text((knee_i - 1) / 2, top * 0.92, "healthy\n(low loss)",
                ha="center", color="#2ca02c", fontsize=9)
        ax.axvspan(knee_i - 0.5, len(nodes) - 0.5, color="#d62728", alpha=0.05)
        ax.text((knee_i + len(nodes) - 1) / 2, top * 0.92,
                "saturated (rising loss + latency)",
                ha="center", color="#a11111", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels([f"{n:,}" for n in nodes], rotation=30, ha="right")
    ax.set_xlabel("Sensor nodes (scaling-test step)")
    ax.set_ylabel("Delivered throughput — stored msg/s")
    ax.set_ylim(0, top)
    ax.set_title("Plan 1 — delivered throughput and health at each node count")

    sm = ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    fig.colorbar(sm, ax=ax, pad=0.01).set_label("Message loss %")

    fig.tight_layout()
    outfile = os.path.join(OUT, "plan1_by_nodes.png")
    fig.savefig(outfile)
    print(f"wrote {outfile}  ({len(nodes)} node-count steps)")


if __name__ == "__main__":
    main()
