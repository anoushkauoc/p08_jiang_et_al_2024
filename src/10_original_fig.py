"""
export_figures.py

Produces original exploratory figures for inclusion in the LaTeX report.
These figures are not replicated from Jiang et al. (2024) — they are original
analysis of the underlying FFIEC bank panel data.

Run this script as part of the doit pipeline before compiling the LaTeX document.

Usage:
    python export_figures.py

Outputs (written to OUTPUT_DIR):
    - figure_asset_dist_{REPORT_DATE}.png
    - figure_uninsured_ratio_{REPORT_DATE}.png
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def export_figure_asset_dist(bank_panel: pd.DataFrame) -> None:
    """
    Plot the distribution of total assets across U.S. commercial banks
    on a log scale, with a vertical line at the small/large threshold.
    """
    threshold = 1.384e6  # $1.384B in thousands
    log_assets = np.log10(bank_panel["Total Asset"].replace(0, np.nan).dropna())

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(log_assets, bins=100, color="steelblue", edgecolor="white", linewidth=0.3)
    ax.axvline(
        np.log10(threshold),
        color="red",
        linestyle="--",
        linewidth=1.2,
        label="\\$1.384B threshold",
    )
    ax.set_xticks([3, 4, 5, 6, 7, 8, 9])
    ax.set_xticklabels(["$1M", "$10M", "$100M", "$1B", "$10B", "$100B", "$1T"])
    ax.set_xlabel("Total Assets", fontsize=11)
    ax.set_ylabel("Number of Banks", fontsize=11)
    ax.set_title(
        "Distribution of Bank Total Assets (Log Scale)",
        fontsize=12,
        fontweight="bold",
    )
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    out = OUTPUT_DIR / f"figure_asset_dist_{REPORT_DATE}.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


def export_figure_uninsured_ratio(bank_panel: pd.DataFrame) -> None:
    """
    Plot the distribution of uninsured deposits as a percentage of total
    assets across all U.S. commercial banks.
    """
    bank_panel = bank_panel.copy()
    bank_panel["uninsured_ratio"] = (
        bank_panel["Uninsured Deposit"] / bank_panel["Total Asset"] * 100
    )
    ratio = (
        bank_panel["uninsured_ratio"]
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .clip(0, 100)
    )

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(ratio, bins=100, color="firebrick", edgecolor="white", linewidth=0.3)
    ax.set_xlabel("Uninsured Deposits / Total Assets (%)", fontsize=11)
    ax.set_ylabel("Number of Banks", fontsize=11)
    ax.set_title(
        "Distribution of Uninsured Deposit Ratio Across U.S. Banks",
        fontsize=12,
        fontweight="bold",
    )
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    out = OUTPUT_DIR / f"figure_uninsured_ratio_{REPORT_DATE}.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


def main() -> None:
    panel_path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(f"Missing bank panel: {panel_path}")

    bank_panel = pd.read_parquet(panel_path)

    print("Exporting original figures for LaTeX report...\n")
    export_figure_asset_dist(bank_panel)
    export_figure_uninsured_ratio(bank_panel)
    print("\nDone.")


if __name__ == "__main__":
    main()