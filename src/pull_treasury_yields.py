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
    "dgs1": "DGS1",
    "dgs3": "DGS3",
    "dgs5": "DGS5",
    "dgs10": "DGS10",
    "dgs20": "DGS20",
    "dgs30": "DGS30",
}

BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?cosd=1900-01-01&id={series}"


def pull(series_code: str, max_retries: int = 2, timeout: int = 15) -> pd.DataFrame:
    url = BASE_URL.format(series=series_code)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,*/*",
        }
    )

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
                time.sleep(1)

    raise RuntimeError(f"Failed to pull {series_code}") from last_err


def write_placeholder() -> None:
    placeholder = pd.DataFrame(
        {
            "date": pd.to_datetime([]),
            "dgs1": pd.Series(dtype="float64"),
            "dgs3": pd.Series(dtype="float64"),
            "dgs5": pd.Series(dtype="float64"),
            "dgs10": pd.Series(dtype="float64"),
            "dgs20": pd.Series(dtype="float64"),
            "dgs30": pd.Series(dtype="float64"),
        }
    )
    placeholder.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote placeholder file to {OUTPUT_PATH}")


def main() -> None:
    pulled = []

    for out_name, fred_code in SERIES.items():
        try:
            df = pull(fred_code).rename(columns={"value": out_name})
            pulled.append(df)
        except Exception as e:
            print(f"Skipping {fred_code}: {e}")

    if pulled:
        out = pulled[0]
        for df in pulled[1:]:
            out = out.merge(df, on="date", how="outer")

        out = out.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
        out.to_parquet(OUTPUT_PATH, index=False)
        print(f"Wrote {OUTPUT_PATH} | rows={len(out)} cols={out.shape[1]}")
    else:
        write_placeholder()


if __name__ == "__main__":
    main()