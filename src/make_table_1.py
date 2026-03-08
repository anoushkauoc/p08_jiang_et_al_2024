from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from pull_gsib_banks import pull_gsib_list
from settings import config


DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")

SMALL_CUTOFF = 1.384e6

BUCKETS = [
    ("lt1y", "d_tsy_1Y"),
    ("1_3y", "d_tsy_3Y"),
    ("3_5y", "d_tsy_5Y"),
    ("5_10y", "d_tsy_10Y"),
    ("10_15y", "d_tsy_20Y"),
    ("15plus", "d_tsy_30Y"),
]


def generate_table1(bank_panel):

    summary = bank_panel.groupby("bank_group").agg(
        total_assets=("assets", "sum"),
        uninsured_deposits=("uninsured_deposits", "sum"),
        securities=("securities", "sum"),
    )

    summary = summary.round(2)

    table_path = OUTPUT_DIR / "table1.tex"

    summary.to_latex(
        table_path,
        caption="Bank balance sheet exposure by bank group",
        label="tab:table1",
        float_format="%.2f"
    )

    print(f"Table saved to {table_path}")

def generate_updated_table(bank_panel):

    updated = bank_panel.groupby("bank_group").agg(
        assets=("assets", "sum"),
        unrealized_losses=("loss_estimate", "sum")
    )

    updated = updated.round(2)

    updated.to_latex(
        OUTPUT_DIR / "table1_updated.tex",
        caption="Updated unrealized losses through 2025",
        label="tab:table1_updated",
        float_format="%.2f"
    )

def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace({0: np.nan})
    return out.replace([np.inf, -np.inf], np.nan)


def _fmt_med_sd(
    x: pd.Series,
    scale: float = 1.0,
    digits: int = 1,
    suffix: str = "",
) -> tuple[str, str]:
    v = x.dropna() * scale
    if len(v) == 0:
        return ("", "")
    med = np.nanmedian(v)
    sd = np.nanstd(v, ddof=0)
    return (f"{med:.{digits}f}{suffix}", f"({sd:,.{digits}f})")


def _fmt_agg_loss_thousands(x: pd.Series) -> str:
    total_dollars = float(np.nansum(x.values)) * 1000
    abs_total = abs(total_dollars)

    if abs_total >= 1e12:
        return f"{total_dollars / 1e12:.1f}T"
    if abs_total >= 1e9:
        return f"{total_dollars / 1e9:.1f}B"
    if abs_total >= 1e6:
        return f"{total_dollars / 1e6:.1f}M"
    return f"{total_dollars:.0f}"


def _winsorize_series(x: pd.Series, p: float = 0.01) -> pd.Series:
    v = x.dropna()
    if len(v) == 0:
        return x
    lo = v.quantile(p)
    hi = v.quantile(1 - p)
    return x.clip(lower=lo, upper=hi)


def main() -> None:
    bank_panel_path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    shocks_path = DATA_DIR / "market_shocks.parquet"

    if not bank_panel_path.exists():
        raise FileNotFoundError(f"Missing bank panel: {bank_panel_path}")
    if not shocks_path.exists():
        raise FileNotFoundError(f"Missing market shocks: {shocks_path}")

    banks = pd.read_parquet(bank_panel_path)
    shocks = pd.read_parquet(shocks_path).iloc[0]

    required_base_cols = [
        "rssd_id_call",
        "Total Asset",
        "Uninsured Deposit",
    ]

    required_bucket_cols = []
    for suffix, _ in BUCKETS:
        required_bucket_cols.extend(
            [
                f"rmbs_{suffix}",
                f"treasury_{suffix}",
                f"other_assets_{suffix}",
                f"res_mtg_{suffix}",
                f"other_loan_{suffix}",
            ]
        )

    required_cols = required_base_cols + required_bucket_cols
    missing = [c for c in required_cols if c not in banks.columns]
    if missing:
        raise ValueError(
            "Missing required columns for bucket-based Table 1:\n"
            + ", ".join(missing)
        )

    banks["rssd_id_call"] = pd.to_numeric(
        banks["rssd_id_call"], errors="coerce"
    ).astype("Int64")

    gsib_df = pull_gsib_list()
    gsib_ids = set(
        pd.to_numeric(gsib_df["rssd_id_call"], errors="coerce")
        .dropna()
        .astype(int)
    )
    banks["is_gsib"] = banks["rssd_id_call"].isin(gsib_ids).astype(int)

    print("GSIB counts:", banks["is_gsib"].value_counts(dropna=False).to_dict())

    banks["size_group"] = "large_non_gsib"
    banks.loc[banks["Total Asset"] < SMALL_CUTOFF, "size_group"] = "small"
    banks.loc[banks["is_gsib"] == 1, "size_group"] = "gsib"

    rmbs_multiplier = float(shocks["rmbs_multiplier"])

    print("\nUsing shocks:")
    for _, shock_col in BUCKETS:
        print(f"  {shock_col} = {float(shocks[shock_col]):.4f}")
    print(f"  rmbs_multiplier = {rmbs_multiplier:.4f}")

    banks["loss_rmbs"] = 0.0
    banks["loss_tsy_other"] = 0.0
    banks["loss_res_mtg"] = 0.0
    banks["loss_other_loan"] = 0.0

    for suffix, shock_col in BUCKETS:
        shock = float(shocks[shock_col])

        banks["loss_rmbs"] += (
            banks[f"rmbs_{suffix}"].fillna(0) * shock * rmbs_multiplier
        )

        banks["loss_res_mtg"] += (
            banks[f"res_mtg_{suffix}"].fillna(0) * shock * rmbs_multiplier
        )

        banks["loss_tsy_other"] += (
            banks[f"treasury_{suffix}"].fillna(0) * shock
            + banks[f"other_assets_{suffix}"].fillna(0) * shock
        )

        banks["loss_other_loan"] += (
            banks[f"other_loan_{suffix}"].fillna(0) * shock
        )

    banks["loss_total"] = (
        banks["loss_rmbs"]
        + banks["loss_tsy_other"]
        + banks["loss_res_mtg"]
        + banks["loss_other_loan"]
    )

    banks["mm_assets"] = banks["Total Asset"] - banks["loss_total"]

    print("Banks with non-positive mm_assets:", int((banks["mm_assets"] <= 0).sum()))
    print(
        "\nLoss sums by channel:\n",
        banks[["loss_rmbs", "loss_tsy_other", "loss_res_mtg", "loss_other_loan"]].sum()
    )

    banks["share_rmbs"] = 100 * _safe_div(banks["loss_rmbs"], banks["loss_total"])
    banks["share_tsy_other"] = 100 * _safe_div(
        banks["loss_tsy_other"], banks["loss_total"]
    )
    banks["share_res_mtg"] = 100 * _safe_div(
        banks["loss_res_mtg"], banks["loss_total"]
    )
    banks["share_other_loan"] = 100 * _safe_div(
        banks["loss_other_loan"], banks["loss_total"]
    )

    banks["loss_asset_pct"] = 100 * _safe_div(
        banks["loss_total"], banks["Total Asset"]
    )

    banks["unins_dep_mm_asset_pct"] = 100 * _safe_div(
        banks["Uninsured Deposit"], banks["mm_assets"]
    )

    groups = {
        "All Banks": banks,
        "Small\n(0, 1.384B)": banks[banks["size_group"] == "small"],
        "Large (non-GSIB)\n[1.384B, )": banks[banks["size_group"] == "large_non_gsib"],
        "GSIB": banks[banks["size_group"] == "gsib"],
    }

    out_rows = []
    out_index = []

    out_index.append("Aggregate Loss")
    out_rows.append(
        {k: _fmt_agg_loss_thousands(df["loss_total"]) for k, df in groups.items()}
    )

    out_index.append("Bank-Level Loss")
    out_rows.append(
        {
            k: _fmt_med_sd(
                _winsorize_series(df["loss_total"], 0.01),
                scale=1 / 1000,
                digits=1,
                suffix="M",
            )[0]
            for k, df in groups.items()
        }
    )

    out_index.append("")
    out_rows.append(
        {
            k: _fmt_med_sd(
                _winsorize_series(df["loss_total"], 0.01),
                scale=1 / 1000,
                digits=1,
            )[1]
            for k, df in groups.items()
        }
    )

    share_map = {
        "Share RMBS": "share_rmbs",
        "Share Treasury and Other": "share_tsy_other",
        "Share Residential Mortgage": "share_res_mtg",
        "Share Other Loan": "share_other_loan",
    }

    for label, col in share_map.items():
        out_index.append(label)
        out_rows.append(
            {
                k: _fmt_med_sd(_winsorize_series(df[col], 0.01))[0]
                for k, df in groups.items()
            }
        )
        out_index.append("")
        out_rows.append(
            {
                k: _fmt_med_sd(_winsorize_series(df[col], 0.01))[1]
                for k, df in groups.items()
            }
        )

    out_index.append("Loss/Asset")
    out_rows.append(
        {
            k: _fmt_med_sd(_winsorize_series(df["loss_asset_pct"], 0.01))[0]
            for k, df in groups.items()
        }
    )

    out_index.append("")
    out_rows.append(
        {
            k: _fmt_med_sd(_winsorize_series(df["loss_asset_pct"], 0.01))[1]
            for k, df in groups.items()
        }
    )

    out_index.append("Uninsured Deposit/MM Asset")
    out_rows.append(
        {
            k: _fmt_med_sd(
                _winsorize_series(df["unins_dep_mm_asset_pct"], 0.01)
            )[0]
            for k, df in groups.items()
        }
    )

    out_index.append("")
    out_rows.append(
        {
            k: _fmt_med_sd(
                _winsorize_series(df["unins_dep_mm_asset_pct"], 0.01)
            )[1]
            for k, df in groups.items()
        }
    )

    out_index.append("Number of Banks")
    out_rows.append({k: f"{df.shape[0]:,}" for k, df in groups.items()})

    table = pd.DataFrame(out_rows, index=out_index)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "table_1.csv"
    tex_path = OUTPUT_DIR / "table_1.tex"

    table.to_csv(csv_path)

    latex_str = table.to_latex(
        escape=False,
        column_format="lcccc",
    )
    with open(tex_path, "w") as f:
        f.write(latex_str)

    print("\nTable 1:")
    print(table)
    print(f"\nSaved -> {csv_path}")
    print(f"Saved -> {tex_path}")


if __name__ == "__main__":
    main()
