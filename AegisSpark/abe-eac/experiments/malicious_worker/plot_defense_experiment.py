#!/usr/bin/env python3
"""
Paper figures for AegisSpark malicious-worker defense experiment.

Figure 1 — Grouped bars + line: interception latency & rejection rate for
            scenarios A / B / C.
Figure 2 — Line chart: throughput vs malicious request percentage
            (AegisSpark vs Centralized ACL).

Usage:
  python3 plot_defense_experiment.py
  python3 plot_defense_experiment.py --data-dir ../../../result/malicious-worker
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


SCENARIO_LABELS = {
    "A_ticket_tamper": "A: Ticket\nTamper",
    "B_forged_sig_exe": "B: Forged\nsig_exe",
    "C_replay_or_attr": "C: Replay /\nIllegal SK_u",
}


def _style() -> None:
    sns.set_theme(style="whitegrid", context="paper", font_scale=1.15)
    plt.rcParams.update(
        {
            "figure.dpi": 140,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "axes.titlesize": 12,
            "axes.labelsize": 11,
            "legend.fontsize": 9,
            "font.family": "DejaVu Sans",
        }
    )


def plot_scenario_defense(df: pd.DataFrame, out_path: Path) -> None:
    """Fig.1: rejection rate (bars) + interception latency (line) for A/B/C."""
    order = ["A_ticket_tamper", "B_forged_sig_exe", "C_replay_or_attr"]
    df = df[df["scenario"].isin(order)].copy()
    df["scenario"] = pd.Categorical(df["scenario"], categories=order, ordered=True)
    df = df.sort_values("scenario")
    df["label"] = df["scenario"].astype(str).map(SCENARIO_LABELS)

    x = np.arange(len(df))
    width = 0.55

    fig, ax1 = plt.subplots(figsize=(7.2, 4.2))
    color_bar = "#2C5F2D"
    color_line = "#97BC62"

    bars = ax1.bar(
        x,
        df["rejection_rate_pct"].astype(float),
        width=width,
        color=color_bar,
        edgecolor="black",
        linewidth=0.6,
        label="Rejection rate (%)",
        zorder=2,
    )
    ax1.set_ylabel("Attack rejection rate (%)")
    ax1.set_ylim(0, 105)
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["label"])
    ax1.set_xlabel("Attack scenario")

    ax2 = ax1.twinx()
    lat_ms = df["latency_us_p50"].astype(float) / 1000.0
    ax2.plot(
        x,
        lat_ms,
        color=color_line,
        marker="o",
        markersize=8,
        linewidth=2.2,
        label="Interception latency p50 (ms)",
        zorder=3,
    )
    # Also show p95 as dashed
    lat_p95_ms = df["latency_us_p95"].astype(float) / 1000.0
    ax2.plot(
        x,
        lat_p95_ms,
        color="#0A7373",
        marker="s",
        markersize=6,
        linewidth=1.6,
        linestyle="--",
        label="Interception latency p95 (ms)",
        zorder=3,
    )
    ax2.set_ylabel("Interception latency (ms)")
    ymax = max(lat_p95_ms.max() * 1.35, 0.05)
    ax2.set_ylim(0, ymax)

    for b, v in zip(bars, df["rejection_rate_pct"].astype(float)):
        ax1.text(
            b.get_x() + b.get_width() / 2,
            v + 1.5,
            f"{v:.1f}%",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper right", frameon=True)

    ax1.set_title("AegisSpark defense: rejection rate & interception latency")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path.with_suffix(".pdf"))
    fig.savefig(out_path.with_suffix(".png"))
    plt.close(fig)
    print(f"wrote {out_path.with_suffix('.pdf')} and .png")


def plot_throughput_vs_attack(df: pd.DataFrame, out_path: Path, metric: str = "benign_tps") -> None:
    """Fig.2: throughput vs malicious request percentage."""
    df = df.copy()
    df["attack_pct"] = df["attack_pct"].astype(float)
    df[metric] = df[metric].astype(float)

    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    palette = {"AegisSpark": "#2C5F2D", "CentralizedACL": "#C0593A"}
    markers = {"AegisSpark": "o", "CentralizedACL": "s"}

    for system, g in df.groupby("system"):
        g = g.sort_values("attack_pct")
        ax.plot(
            g["attack_pct"],
            g[metric],
            label=system,
            color=palette.get(system, None),
            marker=markers.get(system, "o"),
            linewidth=2.2,
            markersize=7,
        )

    ylabel = {
        "benign_tps": "Benign throughput (TPS)",
        "throughput_mbs": "Benign throughput (MB/s)",
        "achieved_rps": "Achieved request rate (RPS)",
    }.get(metric, metric)

    ax.set_xlabel("Malicious request percentage (%)")
    ax.set_ylabel(ylabel)
    ax.set_title("Throughput under mixed malicious traffic")
    ax.set_xticks(sorted(df["attack_pct"].unique()))
    ax.legend(frameon=True)
    ax.grid(True, which="major", alpha=0.35)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path.with_suffix(".pdf"))
    fig.savefig(out_path.with_suffix(".png"))
    plt.close(fig)
    print(f"wrote {out_path.with_suffix('.pdf')} and .png")


def plot_throughput_dual(df: pd.DataFrame, out_dir: Path) -> None:
    """Emit both TPS and MB/s figures for the paper."""
    plot_throughput_vs_attack(df, out_dir / "fig2_throughput_vs_attack_tps", "benign_tps")
    plot_throughput_vs_attack(df, out_dir / "fig2_throughput_vs_attack_mbs", "throughput_mbs")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "result" / "malicious-worker",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="defaults to --data-dir/figures",
    )
    args = ap.parse_args()
    data_dir: Path = args.data_dir
    out_dir: Path = args.out_dir or (data_dir / "figures")

    scenario_csv = data_dir / "scenario_defense_metrics.csv"
    throughput_csv = data_dir / "throughput_vs_attack_pct.csv"
    if not scenario_csv.exists():
        raise SystemExit(f"missing {scenario_csv}; run test_malicious_worker_attack.py first")

    _style()
    sdf = pd.read_csv(scenario_csv)
    plot_scenario_defense(sdf, out_dir / "fig1_scenario_rejection_latency")

    if throughput_csv.exists():
        tdf = pd.read_csv(throughput_csv)
        plot_throughput_dual(tdf, out_dir)
    else:
        print(f"skip fig2: {throughput_csv} not found")

    print("plotting DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
