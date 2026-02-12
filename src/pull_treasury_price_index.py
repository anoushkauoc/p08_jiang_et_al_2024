# src/pull_treasury_price_index.py
from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path("_data")
SERIES_ID = "NASDAQNCPXT"

def fred_csv_url(series_id: str) -> str:
    return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

def main():
    DATA_DIR.mkdir(exist_ok=True)

    url = fred_csv_url(SERIES_ID)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    text = r.text.strip()

    # If FRED returns HTML (rate limit, error page, etc.), fail loudly with context
    if text.lower().startswith("<!doctype html") or "<html" in text[:200].lower():
        raise RuntimeError(
            "FRED returned HTML, not CSV. "
            f"URL: {url}\n"
            f"First 200 chars:\n{text[:200]}"
        )

    df = pd.read_csv(StringIO(text))

    # Date column is usually DATE, but sometimes observation_date (other endpoints)
    date_col = None
    for cand in ("DATE", "date", "observation_date", "Date"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        raise RuntimeError(
            f"Could not find a date column in FRED response. Columns={df.columns.tolist()[:20]}\n"
            f"First 5 lines of response:\n" + "\n".join(text.splitlines()[:5])
        )

    # Value column: either SERIES_ID or last column
    value_col = SERIES_ID if SERIES_ID in df.columns else df.columns[-1]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date", value_col: "treasury_tr_10y"})
    df = df.dropna(subset=["date"]).sort_values("date")

    outpath = DATA_DIR / "treasury_price_index.parquet"
    df[["date", "treasury_tr_10y"]].to_parquet(outpath, index=False)
    print(f"Wrote {outpath} | rows={len(df):,}")

if __name__ == "__main__":
    main()
