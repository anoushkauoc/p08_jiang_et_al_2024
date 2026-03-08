from __future__ import annotations

from pathlib import Path

import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
MARKET_START_DATE = pd.to_datetime(config("MARKET_START_DATE"))
MARKET_END_DATE = pd.to_datetime(config("MARKET_END_DATE"))


def _nearest_row(df: pd.DataFrame, target_date: pd.Timestamp) -> pd.Series:
    temp = df.copy()
    temp["date"] = pd.to_datetime(temp["date"])
    temp["dist"] = (temp["date"] - target_date).abs()
    return temp.sort_values("dist").iloc[0]


def main() -> None:
    yields_path = DATA_DIR / "treasury_yields.parquet"
    mbs_path = DATA_DIR / "mbs_etfs.parquet"

    if not yields_path.exists():
        raise FileNotFoundError(f"Missing Treasury yields file: {yields_path}")
    if not mbs_path.exists():
        raise FileNotFoundError(f"Missing MBS ETF file: {mbs_path}")

    yields = pd.read_parquet(yields_path)
    mbs = pd.read_parquet(mbs_path)

    yields["date"] = pd.to_datetime(yields["date"])
    mbs["date"] = pd.to_datetime(mbs["date"])

    y0 = _nearest_row(yields, MARKET_START_DATE)
    y1 = _nearest_row(yields, MARKET_END_DATE)

    m0 = _nearest_row(mbs, MARKET_START_DATE)
    m1 = _nearest_row(mbs, MARKET_END_DATE)

    durations = {
        "1Y": 1.0,
        "3Y": 2.5,
        "5Y": 4.5,
        "10Y": 8.0,
        "20Y": 15.0,
        "30Y": 20.0,
    }

    shocks = {}

    for tenor, duration in durations.items():
        if tenor not in yields.columns:
            raise ValueError(f"Missing Treasury tenor column: {tenor}")

        delta_y = (float(y1[tenor]) - float(y0[tenor])) / 100.0
        price_change = -duration * delta_y

        # Loss should be positive if prices fall
        shocks[f"d_tsy_{tenor}"] = max(0.0, -price_change)

    rmbs_return = float(m1["rmbs_px"]) / float(m0["rmbs_px"]) - 1.0
    cmbs_return = float(m1["cmbs_px"]) / float(m0["cmbs_px"]) - 1.0

    rmbs_loss = max(0.0, -rmbs_return)
    cmbs_loss = max(0.0, -cmbs_return)

    avg_tsy_loss = sum(shocks.values()) / len(shocks)
    rmbs_multiplier = rmbs_loss / avg_tsy_loss if avg_tsy_loss > 0 else 1.0

    out = pd.DataFrame(
        [
            {
                "market_start_date": MARKET_START_DATE,
                "market_end_date": MARKET_END_DATE,
                **shocks,
                "rmbs_loss": rmbs_loss,
                "cmbs_loss": cmbs_loss,
                "rmbs_multiplier": rmbs_multiplier,
            }
        ]
    )

    outpath = DATA_DIR / "market_shocks.parquet"
    out.to_parquet(outpath, index=False)

    print(out.T)
    print(f"Saved {outpath}")


if __name__ == "__main__":
    main()
