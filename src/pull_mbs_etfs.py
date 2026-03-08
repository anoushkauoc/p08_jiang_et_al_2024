from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import yfinance as yf

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
MARKET_START_DATE = config("MARKET_START_DATE")
MARKET_END_DATE = config("MARKET_END_DATE")

TICKERS: Dict[str, str] = {
    "rmbs_px": "SPMB",  # Paper uses SPDR Portfolio Mortgage-Backed Bond ETF
    "cmbs_px": "CMBS",  # Paper uses iShares CMBS ETF
}


def _get_adj_close(px: pd.DataFrame, ticker: str) -> pd.Series:
    """
    yfinance sometimes returns:
    - single-index columns: ['Open','High','Low','Close','Adj Close','Volume']
    - multi-index columns: level0 = field, level1 = ticker
    Handle both.
    """
    if isinstance(px.columns, pd.MultiIndex):
        for field in ("Adj Close", "Close"):
            if (field, ticker) in px.columns:
                return px[(field, ticker)]
        for field in ("Adj Close", "Close"):
            if field in px.columns.get_level_values(0):
                return px[field].iloc[:, 0]
        raise KeyError(f"No Adj Close/Close found for {ticker}. Columns: {px.columns}")

    if "Adj Close" in px.columns:
        return px["Adj Close"]
    if "Close" in px.columns:
        return px["Close"]

    raise KeyError(
        f"No Adj Close/Close found for {ticker}. Columns: {px.columns.tolist()}"
    )


def pull_etf_prices(
    start: str = "2000-01-01",
    end: str | None = None,
    tickers: Dict[str, str] = TICKERS,
) -> pd.DataFrame:
    series = []

    for col, tkr in tickers.items():
        px = yf.download(
            tkr,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
        )
        if px is None or len(px) == 0:
            raise RuntimeError(f"No data returned for {tkr}.")
        s = _get_adj_close(px, tkr).rename(col)
        series.append(s)

    df = (
        pd.concat(series, axis=1)
        .dropna()
        .reset_index()
        .rename(columns={"Date": "date"})
    )
    df["date"] = pd.to_datetime(df["date"])

    # Optional returns for diagnostics
    df = df.sort_values("date").reset_index(drop=True)
    df["rmbs_ret"] = df["rmbs_px"].pct_change()
    df["cmbs_ret"] = df["cmbs_px"].pct_change()

    return df


def save_mbs_etfs(df: pd.DataFrame, filename: str = "mbs_etfs.parquet") -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    outpath = DATA_DIR / filename
    df.to_parquet(outpath, index=False)
    print(f"Wrote {outpath} | rows={len(df):,} cols={df.shape[1]}")
    print(
        f"Date range: {df['date'].min().date()} to {df['date'].max().date()}"
    )


if __name__ == "__main__":
    # Pull a long enough history to ensure the configured market window is covered
    df = pull_etf_prices(start="2000-01-01", end=None)
    save_mbs_etfs(df)
