import pandas as pd
import requests
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

series = {
    "1Y": "DGS1",
    "3Y": "DGS3",
    "5Y": "DGS5",
    "10Y": "DGS10",
    "20Y": "DGS20",
    "30Y": "DGS30"
}

def pull(series_id):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date","value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"],errors="coerce")
    return df

df_all = None
for k,v in series.items():
    df = pull(v).rename(columns={"value":k})
    if df_all is None:
        df_all = df
    else:
        df_all = df_all.merge(df,on="date",how="outer")

DATA_DIR.mkdir(exist_ok=True)
df_all.to_parquet(DATA_DIR/"treasury_yields.parquet")
print("Saved treasury_yields.parquet")
