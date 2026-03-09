from __future__ import annotations

from io import StringIO
from pathlib import Path
import time

import pandas as pd
import requests

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = DATA_DIR / "treasury_yields.parquet"

SERIES = {
    #"dgs1": "DGS1",
    "dgs3": "DGS3",
    "dgs5": "DGS5",
    "dgs10": "DGS10",
    "dgs20": "DGS20",
    "dgs30": "DGS30",
}

BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"


def pull(series_code: str, max_retries: int = 4, timeout: int = 20) -> pd.DataFrame:
    url = BASE_URL.format(series=series_code)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()

            df = pd.read_csv(StringIO(resp.text))
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={series_code.lower(): "value"})
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["date"])

            return df

        except Exception as e:
            last_err = e
            print(f"Attempt {attempt}/{max_retries} failed for {series_code}: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)

    raise RuntimeError(f"Failed to pull {series_code} after {max_retries} attempts") from last_err


def main() -> None:
    # If file already exists, you can keep using it when live pull fails
    existing = None
    if OUTPUT_PATH.exists():
        existing = pd.read_parquet(OUTPUT_PATH)

    frames = []

    try:
        for out_name, fred_code in SERIES.items():
            df = pull(fred_code).rename(columns={"value": out_name})
            frames.append(df)

        out = frames[0]
        for df in frames[1:]:
            out = out.merge(df, on="date", how="outer")

        out = out.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
        out.to_parquet(OUTPUT_PATH, index=False)

        print(f"Wrote {OUTPUT_PATH} | rows={len(out)} cols={out.shape[1]}")

    except Exception as e:
        if existing is not None:
            print(f"Live pull failed. Using cached file at {OUTPUT_PATH}")
            print(f"Original error: {e}")
            print(f"Cached file has rows={len(existing)} cols={existing.shape[1]}")
        else:
            raise


if __name__ == "__main__":
    main()