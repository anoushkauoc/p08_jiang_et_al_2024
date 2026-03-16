from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
SERIES_ID = "NASDAQNCPXT"


def fred_csv_url(series_id: str) -> str:
    return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = fred_csv_url(SERIES_ID)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    text = r.text.strip()

    # If FRED returns HTML instead of CSV, fail loudly
    if text.lower().startswith("<!doctype html") or "<html" in text[:200].lower():
        raise RuntimeError(
            "FRED returned HTML, not CSV.\n"
            f"URL: {url}\n"
            f"First 200 chars:\n{text[:200]}"
        )

    df = pd.read_csv(StringIO(text))

    # Find date column
    date_col = None
    for cand in ("DATE", "date", "observation_date", "Date"):
        if cand in df.columns:
            date_col = cand
            break

    if date_col is None:
        raise RuntimeError(
            "Could not find a date column in FRED response.\n"
            f"Columns: {df.columns.tolist()[:20]}\n"
            "First 5 lines of response:\n"
            + "\n".join(text.splitlines()[:5])
        )

    # Find value column
    value_col = SERIES_ID if SERIES_ID in df.columns else df.columns[-1]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df = df.rename(columns={date_col: "date", value_col: "treasury_tr_10y"})
    df = df.dropna(subset=["date", "treasury_tr_10y"]).sort_values("date").reset_index(drop=True)

    outpath = DATA_DIR / "treasury_price_index.parquet"
    df[["date", "treasury_tr_10y"]].to_parquet(outpath, index=False)

    print(f"Wrote {outpath} | rows={len(df):,}")
    if not df.empty:
        print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")


if __name__ == "__main__":
    main()