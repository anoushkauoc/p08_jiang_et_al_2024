from __future__ import annotations

from pathlib import Path
from typing import Dict
import pandas as pd
import yfinance as yf

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

TICKERS: Dict[str, str] = {
    "rmbs_px": "SPMB",  # Residential MBS ETF proxy
    "cmbs_px": "CMBS",  # Commercial MBS ETF proxy
}

def _get_adj_close(px: pd.DataFrame, ticker: str) -> pd.Series:
    """
    yfinance sometimes returns:
    - single-index columns: ['Open','High','Low','Close','Adj Close','Volume']
    - multi-index columns: level0 = field, level1 = ticker
    Handle both.
    """
    # MultiIndex case
    if isinstance(px.columns, pd.MultiIndex):
        # prefer Adj Close, else Close
        for field in ("Adj Close", "Close"):
            if (field, ticker) in px.columns:
                return px[(field, ticker)]
        # sometimes ticker may be lowercase/renamed; fall back by field only
        for field in ("Adj Close", "Close"):
            if field in px.columns.get_level_values(0):
                return px[field].iloc[:, 0]
        raise KeyError(f"No Adj Close/Close found for {ticker}. Columns: {px.columns}")

    # Normal columns case
    if "Adj Close" in px.columns:
        return px["Adj Close"]
    if "Close" in px.columns:
        return px["Close"]

    raise KeyError(f"No Adj Close/Close found for {ticker}. Columns: {px.columns.tolist()}")


def pull_etf_prices(
    start: str = "2000-01-01",
    end: str | None = None,
    tickers: Dict[str, str] = TICKERS,
) -> pd.DataFrame:
    series = []
    for col, tkr in tickers.items():
        px = yf.download(tkr, start=start, end=end, auto_adjust=False, progress=False)
        if px is None or len(px) == 0:
            raise RuntimeError(f"No data returned for {tkr}.")
        s = _get_adj_close(px, tkr).rename(col)
        series.append(s)

    df = pd.concat(series, axis=1).dropna().reset_index().rename(columns={"Date": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def save_mbs_etfs(df: pd.DataFrame, filename: str = "mbs_etfs.parquet") -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(DATA_DIR / filename, index=False)
    print(f"Wrote {DATA_DIR / filename} | rows={len(df):,} cols={df.shape[1]}")

if __name__ == "__main__":
    df = pull_etf_prices()
    save_mbs_etfs(df)
