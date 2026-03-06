from __future__ import annotations

import numpy as np
import pandas as pd

from settings import config

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# Assets are in thousands of dollars
# $1.384B = 1.384e6 (thousands)
SMALL_CUTOFF = 1.384e6


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace({0: np.nan})
    return out.replace([np.inf, -np.inf], np.nan)


def _fmt_med_sd(x: pd.Series, scale: float = 1.0, digits: int = 1, suffix: str = ""):
    v = x.dropna() * scale
    if len(v) == 0:
        return ("", "")
    med = np.nanmedian(v)
    sd = np.nanstd(v, ddof=0)
    return (f"{med:.{digits}f}{suffix}", f"({sd:.{digits}f})")


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

    # -----------------------------
    # Load processed bank panel
    # -----------------------------
    banks = pd.read_parquet(DATA_DIR / "bank_panel_03312022.parquet")

    # -----------------------------
    # Clean bank ID
    # -----------------------------
    banks["rssd_id_call"] = pd.to_numeric(
        banks["rssd_id_call"], errors="coerce"
    ).astype("Int64")

    # -----------------------------
    # GSIB list (from FFIEC script)
    # -----------------------------
    GSIB_IDS = [
        934329, 488318, 212465, 449038, 476810, 3382547, 852218, 651448,
        480228, 1443266, 413208, 3357620, 1015560, 2980209, 214807, 304913,
        670560, 2325882, 2182786, 3066025, 398668, 541101, 229913, 1456501,
        2489805, 722777, 35301, 93619, 352745, 812164, 925411, 3212149,
        451965, 688079, 1225761, 2362458, 2531991
    ]

    banks["is_gsib"] = banks["rssd_id_call"].isin(GSIB_IDS).astype(int)

    print("GSIB counts:", banks["is_gsib"].value_counts().to_dict())

    # -----------------------------
    # Bank size groups
    # -----------------------------
    banks["size_group"] = "large_non_gsib"

    banks.loc[banks["Total Asset"] < SMALL_CUTOFF, "size_group"] = "small"
    banks.loc[banks["is_gsib"] == 1, "size_group"] = "gsib"

    # -----------------------------
    # Approximate MTM losses
    # (placeholder shocks)
    # -----------------------------
    banks["loss_rmbs"] = banks["security_rmbs"].fillna(0) * 0.20

    banks["loss_tsy_other"] = (
        banks["security_treasury"].fillna(0)
        + banks["security_cmbs"].fillna(0)
        + banks["security_abs"].fillna(0)
        + banks["security_other"].fillna(0)
    ) * 0.10

    banks["loss_res_mtg"] = banks["Residential_Mortgage"].fillna(0) * 0.20

    banks["loss_other_loan"] = (
        banks["Commerical_Mortgage"].fillna(0)
        + banks["Other_Real_Estate_Mortgage"].fillna(0)
        + banks["Agri_Loan"].fillna(0)
        + banks["Comm_Indu_Loan"].fillna(0)
        + banks["Consumer_Loan"].fillna(0)
        + banks["Non_Rep_Loan"].fillna(0)
    ) * 0.05

    banks["loss_total"] = (
        banks["loss_rmbs"]
        + banks["loss_tsy_other"]
        + banks["loss_res_mtg"]
        + banks["loss_other_loan"]
    )

    # Mark-to-market assets
    banks["mm_assets"] = banks["Total Asset"] - banks["loss_total"]

    # -----------------------------
    # Bank-level ratios
    # -----------------------------
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

    # -----------------------------
    # Table groups
    # -----------------------------
    groups = {
        "All Banks": banks,
        "Small\n(0, 1.384B)": banks[banks["size_group"] == "small"],
        "Large (non-GSIB)\n[1.384B, )": banks[banks["size_group"] == "large_non_gsib"],
        "GSIB": banks[banks["size_group"] == "gsib"],
    }

    out_rows = []
    out_index = []

    # Aggregate Loss
    out_index.append("Aggregate Loss")
    out_rows.append(
        {k: _fmt_agg_loss_thousands(df["loss_total"]) for k, df in groups.items()}
    )

    # Bank-Level Loss
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

    # Shares
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

    # Loss/Asset
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

    # Uninsured Deposit/MM Asset
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

    # Number of Banks
    out_index.append("Number of Banks")
    out_rows.append({k: f"{df.shape[0]:,}" for k, df in groups.items()})

    table = pd.DataFrame(out_rows, index=out_index)

    # -----------------------------
    # Save outputs
    # -----------------------------
    OUTPUT_DIR.mkdir(exist_ok=True)

    table.to_csv(OUTPUT_DIR / "table_1.csv")

    try:
        table.to_latex(OUTPUT_DIR / "table_1.tex", escape=False)
    except Exception as e:
        print("LaTeX export skipped:", e)

    print(table)
    print("\nSaved ->", OUTPUT_DIR / "table_1.csv")


if __name__ == "__main__":
    main()