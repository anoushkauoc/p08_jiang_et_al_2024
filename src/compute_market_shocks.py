from __future__ import annotations

from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
MARKET_START_DATE = pd.to_datetime(config("MARKET_START_DATE"))
MARKET_END_DATE = pd.to_datetime(config("MARKET_END_DATE"))


def _nearest_value(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    target: pd.Timestamp,
) -> float:
    temp = df[[date_col, value_col]].dropna().copy()
    temp[date_col] = pd.to_datetime(temp[date_col])
    temp["dist"] = (temp[date_col] - target).abs()
    row = temp.sort_values("dist").iloc[0]
    return float(row[value_col])


def main() -> None:
    treasury_path = DATA_DIR / "treasury_price_index.parquet"
    mbs_path = DATA_DIR / "mbs_etfs.parquet"

    if not treasury_path.exists():
        raise FileNotFoundError(f"Missing Treasury data: {treasury_path}")
    if not mbs_path.exists():
        raise FileNotFoundError(f"Missing MBS ETF data: {mbs_path}")

    tsy = pd.read_parquet(treasury_path)
    tsy["date"] = pd.to_datetime(tsy["date"])

    mbs = pd.read_parquet(mbs_path)
    mbs["date"] = pd.to_datetime(mbs["date"])

    tsy_start = _nearest_value(tsy, "date", "treasury_tr_10y", MARKET_START_DATE)
    tsy_end = _nearest_value(tsy, "date", "treasury_tr_10y", MARKET_END_DATE)
    treasury_return = (tsy_end / tsy_start) - 1.0
    treasury_loss = max(0.0, -treasury_return)

    rmbs_start = _nearest_value(mbs, "date", "rmbs_px", MARKET_START_DATE)
    rmbs_end = _nearest_value(mbs, "date", "rmbs_px", MARKET_END_DATE)
    rmbs_return = (rmbs_end / rmbs_start) - 1.0
    rmbs_loss = max(0.0, -rmbs_return)

    cmbs_start = _nearest_value(mbs, "date", "cmbs_px", MARKET_START_DATE)
    cmbs_end = _nearest_value(mbs, "date", "cmbs_px", MARKET_END_DATE)
    cmbs_return = (cmbs_end / cmbs_start) - 1.0
    cmbs_loss = max(0.0, -cmbs_return)

    rmbs_multiplier = rmbs_loss / treasury_loss if treasury_loss > 0 else 1.0

    shocks = pd.DataFrame(
        [
            {
                "market_start_date": MARKET_START_DATE,
                "market_end_date": MARKET_END_DATE,
                "treasury_loss": treasury_loss,
                "rmbs_loss": rmbs_loss,
                "cmbs_loss": cmbs_loss,
                "rmbs_multiplier": rmbs_multiplier,
            }
        ]
    )

    outpath = DATA_DIR / "market_shocks.parquet"
    shocks.to_parquet(outpath, index=False)

    print(shocks.T)
    print(f"\nWrote {outpath}")


if __name__ == "__main__":
    main()
