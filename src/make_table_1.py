from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from pull_gsib_banks import pull_gsib_list
from settings import config


DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")

# Assets are in thousands
SMALL_CUTOFF = 1.384e6


BUCKETS = [
    ("lt1y", ("d_tsy_lt1y", "d_tsy_1Y")),
    ("1_3y", ("d_tsy_1_3y", "d_tsy_3Y")),
    ("3_5y", ("d_tsy_3_5y", "d_tsy_5Y")),
    ("5_10y", ("d_tsy_5_10y", "d_tsy_10Y")),
    ("10_15y", ("d_tsy_10_15y", "d_tsy_20Y")),
    ("15plus", ("d_tsy_15plus", "d_tsy_30Y")),
]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace({0: np.nan})
    return out.replace([np.inf, -np.inf], np.nan)


def _fmt_mean(x, scale=1.0, digits=1, suffix=""):
    v = (x.dropna() * scale).astype(float)
    if len(v) == 0:
        return ""
    return f"{np.nanmean(v):.{digits}f}{suffix}"


def _fmt_sd(x, scale=1.0, digits=1):
    v = (x.dropna() * scale).astype(float)
    if len(v) <= 1:
        return ""
    return f"({np.nanstd(v, ddof=1):,.{digits}f})"


def _fmt_agg_loss(x):

    total = float(np.nansum(x.values)) * 1000
    abs_total = abs(total)

    if abs_total >= 1e12:
        return f"{total/1e12:.1f}T"
    if abs_total >= 1e9:
        return f"{total/1e9:.1f}B"
    if abs_total >= 1e6:
        return f"{total/1e6:.1f}M"

    return f"{total:.0f}"


def _resolve_shock_col(shocks, candidates):
    for c in candidates:
        if c in shocks.index:
            return c
    raise KeyError(candidates)


def main():

    bank_panel_path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    shocks_path = DATA_DIR / "market_shocks.parquet"

    banks = pd.read_parquet(bank_panel_path)
    shocks = pd.read_parquet(shocks_path).iloc[0]

    banks["rssd_id_call"] = pd.to_numeric(banks["rssd_id_call"], errors="coerce").astype("Int64")
    banks["Total Asset"] = pd.to_numeric(banks["Total Asset"], errors="coerce")
    banks["Uninsured Deposit"] = pd.to_numeric(banks["Uninsured Deposit"], errors="coerce")

    # GSIB flags
    gsib_df = pull_gsib_list()
    gsib_ids = set(pd.to_numeric(gsib_df["rssd_id_call"], errors="coerce").dropna().astype(int))
    banks["is_gsib"] = banks["rssd_id_call"].isin(gsib_ids).astype(int)

    banks["size_group"] = "large_non_gsib"
    banks.loc[banks["Total Asset"] < SMALL_CUTOFF, "size_group"] = "small"
    banks.loc[banks["is_gsib"] == 1, "size_group"] = "gsib"

    rmbs_multiplier = float(shocks["rmbs_multiplier"])

    resolved_shocks = {}
    for suffix, candidates in BUCKETS:
        col = _resolve_shock_col(shocks, candidates)
        resolved_shocks[suffix] = float(shocks[col])

    # initialize
    for c in ["loss_rmbs","loss_res_mtg","loss_tsy_other","loss_other_loan"]:
        banks[c] = 0.0

    for suffix,_ in BUCKETS:

        shock = resolved_shocks[suffix]

        rmbs = pd.to_numeric(banks[f"rmbs_{suffix}"],errors="coerce").fillna(0)
        res  = pd.to_numeric(banks[f"res_mtg_{suffix}"],errors="coerce").fillna(0)
        tsy  = pd.to_numeric(banks[f"treasury_{suffix}"],errors="coerce").fillna(0)
        oth  = pd.to_numeric(banks[f"other_assets_{suffix}"],errors="coerce").fillna(0)
        loan = pd.to_numeric(banks[f"other_loan_{suffix}"],errors="coerce").fillna(0)

        banks["loss_rmbs"] += rmbs * shock * rmbs_multiplier
        banks["loss_res_mtg"] += res * shock * rmbs_multiplier
        banks["loss_tsy_other"] += tsy * shock + oth * shock
        banks["loss_other_loan"] += loan * shock

    banks["loss_total"] = (
        banks["loss_rmbs"]
        + banks["loss_res_mtg"]
        + banks["loss_tsy_other"]
        + banks["loss_other_loan"]
    )

    banks["mm_assets"] = banks["Total Asset"] - banks["loss_total"]

    # exposures
    banks["exp_rmbs"] = banks.filter(like="rmbs_").sum(axis=1)
    banks["exp_res_mtg"] = banks.filter(like="res_mtg_").sum(axis=1)
    banks["exp_tsy_other"] = banks.filter(like="treasury_").sum(axis=1) + banks.filter(like="other_assets_").sum(axis=1)
    banks["exp_other_loan"] = banks.filter(like="other_loan_").sum(axis=1)

    banks["total_exposure"] = (
        banks["exp_rmbs"]
        + banks["exp_res_mtg"]
        + banks["exp_tsy_other"]
        + banks["exp_other_loan"]
    )

    # shares (correct denominator)
    banks["share_rmbs"] = 100 * banks["exp_rmbs"] / banks["total_exposure"]
    banks["share_tsy_other"] = 100 * banks["exp_tsy_other"] / banks["total_exposure"]
    banks["share_res_mtg"] = 100 * banks["exp_res_mtg"] / banks["total_exposure"]
    banks["share_other_loan"] = 100 * banks["exp_other_loan"] / banks["total_exposure"]

    banks["loss_asset_pct"] = 100 * _safe_div(banks["loss_total"], banks["Total Asset"])
    banks["unins_dep_mm_asset_pct"] = 100 * _safe_div(banks["Uninsured Deposit"], banks["mm_assets"])

    groups = {
        "All Banks": banks,
        "Small\n(0,1.384B)": banks[banks["size_group"]=="small"],
        "Large (non-GSIB)\n[1.384B,)": banks[banks["size_group"]=="large_non_gsib"],
        "GSIB": banks[banks["size_group"]=="gsib"],
    }

    rows=[]
    idx=[]

    idx.append("Aggregate Loss")
    rows.append({k:_fmt_agg_loss(df["loss_total"]) for k,df in groups.items()})

    idx.append("Bank-Level Loss")
    rows.append({k:_fmt_mean(df["loss_total"],scale=1/1000,suffix="M") for k,df in groups.items()})
    idx.append("")
    rows.append({k:_fmt_sd(df["loss_total"],scale=1/1000) for k,df in groups.items()})

    share_map={
        "Share RMBS":"share_rmbs",
        "Share Treasury and Other":"share_tsy_other",
        "Share Residential Mortgage":"share_res_mtg",
        "Share Other Loan":"share_other_loan",
    }

    for label,col in share_map.items():

        idx.append(label)
        rows.append({k:_fmt_mean(df[col]) for k,df in groups.items()})

        idx.append("")
        rows.append({k:_fmt_sd(df[col]) for k,df in groups.items()})

    idx.append("Loss/Asset")
    rows.append({k:_fmt_mean(df["loss_asset_pct"]) for k,df in groups.items()})
    idx.append("")
    rows.append({k:_fmt_sd(df["loss_asset_pct"]) for k,df in groups.items()})

    idx.append("Uninsured Deposit/MM Asset")
    rows.append({k:_fmt_mean(df["unins_dep_mm_asset_pct"]) for k,df in groups.items()})
    idx.append("")
    rows.append({k:_fmt_sd(df["unins_dep_mm_asset_pct"]) for k,df in groups.items()})

    idx.append("Number of Banks")
    rows.append({k:f"{df.shape[0]:,}" for k,df in groups.items()})

    table=pd.DataFrame(rows,index=idx)

    OUTPUT_DIR.mkdir(parents=True,exist_ok=True)

    table.to_csv(OUTPUT_DIR/"table_1.csv")
    table.to_latex(OUTPUT_DIR/"table_1.tex",escape=False)

    print(table)


if __name__=="__main__":
    main()
