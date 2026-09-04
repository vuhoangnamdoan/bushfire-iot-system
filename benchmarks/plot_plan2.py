import csv
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")


def read_csv(name):
    path = os.path.join(OUT, name)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def main():
    path = os.path.join(OUT, "plan2_results.json")
    if not os.path.exists(path):
        sys.exit(f"missing {path} — run: node benchmarks/plan2_burst.js")
    with open(path) as f:
        summary = json.load(f)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib not installed — run: pip install matplotlib")

    timings = read_csv("plan2_detect_timings.csv")
    lags = read_csv("plan2_alert_lags.csv")

    # 1) Detection response-time distribution
    total = [float(t["totalMs"]) for t in timings if t.get("totalMs")]
    query = [float(t["queryMs"]) for t in timings if t.get("queryMs")]
    if total:
        plt.figure(figsize=(7, 4.5))
        plt.hist(total, bins=20, alpha=0.7, label="total detect ms")
        if query:
            plt.hist(query, bins=20, alpha=0.7, label="mongo read ms")
        plt.xlabel("Milliseconds")
        plt.ylabel("Count of /api/detect calls")
        plt.title("Plan 2 — Detection response time during burst")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUT, "plan2_detect_latency.png"), dpi=140)
        plt.close()

    # 2) Alert lag per alert
    if lags:
        vals = [float(l["lagSec"]) for l in lags if l.get("lagSec")]
        labels = [l["region"] for l in lags]
        if vals:
            plt.figure(figsize=(7, 4.5))
            plt.bar(range(len(vals)), vals, tick_label=labels)
            plt.ylabel("Ignition -> alert dispatched (s)")
            plt.title("Plan 2 — Alert lag by region")
            plt.xticks(rotation=45, ha="right")
            plt.grid(True, axis="y", alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(OUT, "plan2_alert_lag.png"), dpi=140)
            plt.close()

    # 3) Cooldown effect: detect-triggered vs alerts written
    plt.figure(figsize=(6, 4.5))
    bars = {
        "detect calls": summary.get("detectCalls", 0),
        "triggered": summary.get("detectTriggered", 0),
        "alerts written": summary.get("alertsWritten", 0),
    }
    plt.bar(list(bars.keys()), list(bars.values()))
    plt.ylabel("Count during burst")
    plt.title("Plan 2 — Cooldown suppresses alert storm")
    for i, v in enumerate(bars.values()):
        plt.text(i, v, str(v), ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "plan2_cooldown.png"), dpi=140)
    plt.close()

    print(f"Charts + table written to {OUT}/")


if __name__ == "__main__":
    main()
