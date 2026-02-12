from __future__ import annotations

from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

series_to_pull = {
    ## Macro
    "GDP": "GDP",
    "CPIAUCNS": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
    "GDPC1": "Real Gross Domestic Product",
    ## Finance
    "DPCREDIT": "Discount Window Primary Credit Rate",
    "EFFR": "Effective Federal Funds Rate",
    "OBFR": "Overnight Bank Funding Rate",
    "SOFR": "SOFR",
    "IORR": "Interest on Required Reserves",
    "IOER": "Interest on Excess Reserves",
    "IORB": "Interest on Reserve Balances",
    "DFEDTARU": "Federal Funds Target Range - Upper Limit",
    "DFEDTARL": "Federal Funds Target Range - Lower Limit",
    "WALCL": "Federal Reserve Total Assets",  # Millions, converted to billions below
    "TOTRESNS": "Reserves of Depository Institutions: Total",  # Billions
    "TREAST": "Treasuries Held by Federal Reserve",  # Millions, Converted to Billions below
    "CURRCIR": "Currency in Circulation",  # Billions
    "GFDEBTN": "Federal Debt: Total Public Debt",  # Millions, Converted to Billions below
    "WTREGEN": "Treasury General Account",  # Billions
    "RRPONTSYAWARD": "Fed ON/RRP Award Rate",
    "RRPONTSYD": "Treasuries Fed Sold In Temp Open Mark",  # Billions
    "RPONTSYD": "Treasuries Fed Purchased In Temp Open Mark",  # Billions
    "WSDONTL": "SOMA Sec Overnight Lending Volume",  # Millions, Converted to Billions below
}

series_descriptions = series_to_pull.copy()
series_descriptions["MY_RPONTSYAWARD"] = "Fed ON/RP Award Rate"
series_descriptions["Gen_IORB"] = "Interest on Reserves"
series_descriptions["ONRRP_CTPY_LIMIT"] = "Counter-party Limit at Fed ON/RRP Facility"
series_descriptions["ONRP_AGG_LIMIT"] = "Aggregate Limit at Fed Standing Repo Facility"

manual_ONRRP_cntypty_limits = {  # in $ Billions
    "2013-Sep-22": 0,
    "2013-Sep-23": 1,
    "2014-Jan-29": 3,
    "2014-Feb-3": 7,
    "2014-Feb-21": 10,
    "2014-Jul-11": 30,
    "2021-Mar-17": 80,
    "2021-Jun-3": 160,
}


def _fred_series_csv(series_id: str) -> pd.Series:
    """
    Pull a single FRED series via the public CSV endpoint (no API key).
    Works with either DATE or observation_date as the date column.
    Returns a Series indexed by date with name = series_id.
    """
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    text = r.text.strip()
    if text.lower().startswith("<!doctype html") or "<html" in text[:200].lower():
        raise RuntimeError(f"FRED returned HTML instead of CSV for {series_id}. URL={url}")

    df = pd.read_csv(StringIO(text))

    # Date column can be DATE or observation_date depending on endpoint
    date_col = None
    for cand in ("DATE", "observation_date", "date", "Date"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        raise RuntimeError(
            f"Could not find date column for {series_id}. Columns={df.columns.tolist()[:10]}. "
            f"First lines:\n" + "\n".join(text.splitlines()[:5])
        )

    # Value column is usually series_id
    value_col = series_id if series_id in df.columns else df.columns[-1]

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    s = pd.to_numeric(df[value_col], errors="coerce")
    s.index = df[date_col]
    s.name = series_id
    return s



def pull_fred(start_date=START_DATE, end_date=END_DATE, ffill=True):
    """
    Lookup series code, e.g.:
    https://fred.stlouisfed.org/series/RPONTSYD

    This version does NOT use pandas_datareader (works with pandas 3.0+).
    """
    # Pull all series and join into one DataFrame
    series_list = []
    for code in series_to_pull.keys():
        series_list.append(_fred_series_csv(code))

    df = pd.concat(series_list, axis=1).sort_index()

    # Restrict to requested date window
    df = df.loc[pd.to_datetime(start_date) : pd.to_datetime(end_date)]

    # Convert millions to billions
    millions_to_billions = ["TREAST", "GFDEBTN", "WALCL", "WSDONTL"]
    for s in millions_to_billions:
        if s in df.columns:
            df[s] = df[s] / 1_000

    # Forward fill selected series
    if ffill:
        forward_fill = [
            "OBFR",
            "DPCREDIT",
            "TREAST",
            "TOTRESNS",
            "WTREGEN",
            "WALCL",
            "CURRCIR",
            "RRPONTSYAWARD",
            "WSDONTL",
        ]
        for s in forward_fill:
            if s in df.columns:
                df[s] = df[s].ffill()

    # When IORB is missing, use IOER (interest on excess reserves)
    if "IORB" in df.columns and "IOER" in df.columns:
        df["Gen_IORB"] = df["IORB"].fillna(df["IOER"])
    else:
        df["Gen_IORB"] = np.nan

    # Manual ONRRP counterparty limits
    df["ONRRP_CTPY_LIMIT"] = np.nan
    for key, val in manual_ONRRP_cntypty_limits.items():
        date = pd.to_datetime(key)
        df.loc[date, "ONRRP_CTPY_LIMIT"] = val
    df["ONRRP_CTPY_LIMIT"] = df["ONRRP_CTPY_LIMIT"].ffill()

    # Standing repo facility aggregate limit
    df["ONRP_AGG_LIMIT"] = np.nan
    df.loc[pd.to_datetime("2021-07-28"), "ONRP_AGG_LIMIT"] = 500
    df["ONRP_AGG_LIMIT"] = df["ONRP_AGG_LIMIT"].ffill()

    # Drop original reserve rate columns like before
    df_focused = df.drop(columns=["IORR", "IOER", "IORB"], errors="ignore")
    return df_focused


def load_fred(data_dir=DATA_DIR):
    """
    Must first run this module as main to pull and save data.
    """
    file_path = Path(data_dir) / "fred.parquet"
    df = pd.read_parquet(file_path)
    return df


def demo():
    _ = load_fred()


if __name__ == "__main__":
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    end_date = today

    df = pull_fred(START_DATE, end_date)

    filedir = Path(DATA_DIR)
    filedir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(filedir / "fred.parquet")
    df.to_csv(filedir / "fred.csv")
    print(f"Wrote {filedir / 'fred.parquet'} and {filedir / 'fred.csv'} | rows={len(df):,} cols={df.shape[1]}")
