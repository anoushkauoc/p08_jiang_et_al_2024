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
    signed_tsy_price_changes = {}

    for tenor, duration in durations.items():
        if tenor not in yields.columns:
            raise ValueError(f"Missing Treasury tenor column: {tenor}")

        delta_y = (float(y1[tenor]) - float(y0[tenor])) / 100.0
        signed_price_change = -duration * delta_y
        signed_tsy_price_changes[tenor] = signed_price_change

        # store positive loss magnitudes for Table 1 use
        shocks[f"d_tsy_{tenor}"] = max(0.0, -signed_price_change)

    # Signed ETF returns over the window
    rmbs_return = float(m1["rmbs_px"]) / float(m0["rmbs_px"]) - 1.0
    cmbs_return = float(m1["cmbs_px"]) / float(m0["cmbs_px"]) - 1.0

    # Positive loss magnitudes
    rmbs_loss = max(0.0, -rmbs_return)
    cmbs_loss = max(0.0, -cmbs_return)

    # Use absolute average Treasury price move for multiplier construction
    avg_abs_tsy_move = sum(abs(v) for v in signed_tsy_price_changes.values()) / len(
        signed_tsy_price_changes
    )

    # Multiplier should reflect relative sensitivity, not collapse to zero just
    # because the ETF had a positive return over the chosen window.
    rmbs_multiplier = abs(rmbs_return) / avg_abs_tsy_move if avg_abs_tsy_move > 0 else 1.0
    cmbs_multiplier = abs(cmbs_return) / avg_abs_tsy_move if avg_abs_tsy_move > 0 else 1.0

    out = pd.DataFrame(
        [
            {
                "market_start_date": MARKET_START_DATE,
                "market_end_date": MARKET_END_DATE,
                **shocks,
                "rmbs_return": rmbs_return,
                "cmbs_return": cmbs_return,
                "rmbs_loss": rmbs_loss,
                "cmbs_loss": cmbs_loss,
                "avg_abs_tsy_move": avg_abs_tsy_move,
                "rmbs_multiplier": rmbs_multiplier,
                "cmbs_multiplier": cmbs_multiplier,
            }
        ]
    )

    outpath = DATA_DIR / "market_shocks.parquet"
    out.to_parquet(outpath, index=False)

    print("Treasury window used:")
    print(" start target:", MARKET_START_DATE.date(), "| matched:", pd.to_datetime(y0["date"]).date())
    print(" end target:  ", MARKET_END_DATE.date(), "| matched:", pd.to_datetime(y1["date"]).date())

    print("\nTreasury yields at start:")
    print(y0[["1Y", "3Y", "5Y", "10Y", "20Y", "30Y"]])

    print("\nTreasury yields at end:")
    print(y1[["1Y", "3Y", "5Y", "10Y", "20Y", "30Y"]])

    print("\nMBS window used:")
    print(" start target:", MARKET_START_DATE.date(), "| matched:", pd.to_datetime(m0["date"]).date())
    print(" end target:  ", MARKET_END_DATE.date(), "| matched:", pd.to_datetime(m1["date"]).date())
    print(f" RMBS price start/end: {float(m0['rmbs_px']):.4f} -> {float(m1['rmbs_px']):.4f}")
    print(f" CMBS price start/end: {float(m0['cmbs_px']):.4f} -> {float(m1['cmbs_px']):.4f}")

    print("\nComputed shocks:")
    print(out.T)
    print(f"\nSaved {outpath}")


if __name__ == "__main__":
    main()