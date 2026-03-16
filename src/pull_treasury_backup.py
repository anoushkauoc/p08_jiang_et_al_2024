from pathlib import Path
import pandas as pd

DATA_DIR = Path("_data")

SERIES = {
    "DGS1": "dgs1",
    "DGS3": "dgs3",
    "DGS5": "dgs5",
    "DGS10": "dgs10",
    "DGS20": "dgs20",
    "DGS30": "dgs30",
}

dfs = []

for fred_name, col_name in SERIES.items():

    path = DATA_DIR / f"{fred_name}.csv"

    print(f"Reading {path}")

    df = pd.read_csv(path)

    df.columns = [c.lower() for c in df.columns]

    df["observation_date"] = pd.to_datetime(df["observation_date"], errors="coerce")

    value_col = fred_name.lower()

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df = df[["observation_date", value_col]].rename(columns={value_col: col_name})

    dfs.append(df)

out = dfs[0]

for df in dfs[1:]:
    out = out.merge(df, on="observation_date", how="outer")

out = out.sort_values("observation_date").reset_index(drop=True)

csv_out = DATA_DIR / "treasury_yields.csv"
parquet_out = DATA_DIR / "treasury_yields.parquet"

out.to_csv(csv_out, index=False)
out.to_parquet(parquet_out, index=False)

print("\nSaved:")
print(csv_out)
print(parquet_out)
print("\nColumns:", out.columns.tolist())
print("Rows:", len(out))