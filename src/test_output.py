from __future__ import annotations

from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")


def _read_table1() -> pd.DataFrame:
    path = OUTPUT_DIR / "table_1.csv"
    assert path.exists(), f"Missing file: {path}"
    return pd.read_csv(path, index_col=0)


def _to_float(x):
    s = str(x).replace(",", "").strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def test_bank_panel_exists():
    path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    assert path.exists(), f"Missing file: {path}"


def test_market_shocks_exists():
    path = DATA_DIR / "market_shocks.parquet"
    assert path.exists(), f"Missing file: {path}"


def test_table1_exists():
    path = OUTPUT_DIR / "table_1.csv"
    assert path.exists(), f"Missing file: {path}"


def test_summary_stats_exists():
    path = OUTPUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx"
    assert path.exists(), f"Missing file: {path}"


def test_figure_a1_exists():
    path = OUTPUT_DIR / f"figure_A1_{REPORT_DATE}.png"
    assert path.exists(), f"Missing file: {path}"


def test_bank_panel_required_columns():
    path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    df = pd.read_parquet(path)

    required = [
        "rssd_id_call",
        "Total Asset",
        "Uninsured Deposit",
        "rmbs_lt1y",
        "rmbs_1_3y",
        "rmbs_3_5y",
        "rmbs_5_10y",
        "rmbs_10_15y",
        "rmbs_15plus",
        "treasury_lt1y",
        "treasury_1_3y",
        "treasury_3_5y",
        "treasury_5_10y",
        "treasury_10_15y",
        "treasury_15plus",
        "other_assets_lt1y",
        "other_assets_1_3y",
        "other_assets_3_5y",
        "other_assets_5_10y",
        "other_assets_10_15y",
        "other_assets_15plus",
        "res_mtg_lt1y",
        "res_mtg_1_3y",
        "res_mtg_3_5y",
        "res_mtg_5_10y",
        "res_mtg_10_15y",
        "res_mtg_15plus",
        "other_loan_lt1y",
        "other_loan_1_3y",
        "other_loan_3_5y",
        "other_loan_5_10y",
        "other_loan_10_15y",
        "other_loan_15plus",
    ]

    missing = [c for c in required if c not in df.columns]
    assert not missing, f"Missing required columns: {missing}"


def test_bank_panel_positive_assets():
    path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    df = pd.read_parquet(path)

    assert (df["Total Asset"] > 0).all(), "Some banks have non-positive Total Asset"


def test_market_shocks_columns():
    df = pd.read_parquet(DATA_DIR / "market_shocks.parquet")

    required = [
        "market_start_date",
        "market_end_date",
        "d_tsy_1Y",
        "d_tsy_3Y",
        "d_tsy_5Y",
        "d_tsy_10Y",
        "d_tsy_20Y",
        "d_tsy_30Y",
        "rmbs_multiplier",
    ]

    missing = [c for c in required if c not in df.columns]
    assert not missing, f"Missing market shock columns: {missing}"


def test_market_shocks_nonnegative():
    df = pd.read_parquet(DATA_DIR / "market_shocks.parquet").iloc[0]

    for col in ["d_tsy_1Y", "d_tsy_3Y", "d_tsy_5Y", "d_tsy_10Y", "d_tsy_20Y", "d_tsy_30Y"]:
        assert df[col] >= 0, f"{col} should be nonnegative"

    assert df["rmbs_multiplier"] > 0, "rmbs_multiplier should be positive"


def test_table1_has_expected_rows():
    df = _read_table1()

    expected_rows = [
        "Aggregate Loss",
        "Bank-Level Loss",
        "Share RMBS",
        "Share Treasury and Other",
        "Share Residential Mortgage",
        "Share Other Loan",
        "Loss/Asset",
        "Uninsured Deposit/MM Asset",
        "Number of Banks",
    ]

    for row in expected_rows:
        assert row in df.index, f"Missing row in Table 1: {row}"


def test_table1_has_expected_columns():
    df = _read_table1()

    expected_cols = [
        "All Banks",
        "Small\n(0, 1.384B)",
        "Large (non-GSIB)\n[1.384B, )",
        "GSIB",
    ]

    for col in expected_cols:
        assert col in df.columns, f"Missing column in Table 1: {col}"


def test_table1_number_of_banks_positive():
    df = _read_table1()
    row = df.loc["Number of Banks"]

    for val in row:
        cleaned = str(val).replace(",", "").strip()
        if cleaned:
            assert float(cleaned) > 0, f"Bank count should be positive, got {val}"


def test_table1_aggregate_loss_not_zero():
    df = _read_table1()
    val = str(df.loc["Aggregate Loss", "All Banks"]).strip()
    assert val not in {"0", "0.0", "0.0M", "0.0B", "0.0T"}, (
        f"Aggregate Loss looks zero: {val}"
    )


def test_table1_loss_asset_positive():
    df = _read_table1()

    for col in df.columns:
        val = _to_float(df.loc["Loss/Asset", col])
        assert val is not None, f"Could not parse Loss/Asset for {col}"
        assert val >= 0, f"Loss/Asset should be nonnegative for {col}"


def test_table1_uninsured_deposit_mm_asset_positive():
    df = _read_table1()

    for col in df.columns:
        val = _to_float(df.loc["Uninsured Deposit/MM Asset", col])
        assert val is not None, f"Could not parse Uninsured Deposit/MM Asset for {col}"
        assert val >= 0, f"Uninsured Deposit/MM Asset should be nonnegative for {col}"


def test_table1_shares_sum_reasonably():
    df = _read_table1()

    share_rows = [
        "Share RMBS",
        "Share Treasury and Other",
        "Share Residential Mortgage",
        "Share Other Loan",
    ]

    for col in df.columns:
        vals = [_to_float(df.loc[row, col]) for row in share_rows]
        vals = [v for v in vals if v is not None]
        total = sum(vals)
        assert 95 <= total <= 105, f"Shares in {col} sum to {total}, not ~100"


def test_table1_bank_level_loss_positive():
    df = _read_table1()

    for col in df.columns:
        val = str(df.loc["Bank-Level Loss", col]).replace("M", "").replace(",", "").strip()
        if val:
            parsed = float(val)
            assert parsed >= 0, f"Bank-Level Loss should be nonnegative for {col}"


def test_bank_panel_has_rows():
    path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    df = pd.read_parquet(path)
    assert len(df) > 0, "Bank panel is empty"


def test_market_shocks_single_row():
    df = pd.read_parquet(DATA_DIR / "market_shocks.parquet")
    assert len(df) == 1, f"Expected 1 row in market_shocks.parquet, got {len(df)}"
