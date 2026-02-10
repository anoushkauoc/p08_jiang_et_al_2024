"""
WRDS Call Report Pull Script

What this script pulls:
    - RCFD series 1: bank.wrds_call_rcfd_1 (selected columns used in analysis)
    - RCFD series 2: bank.wrds_call_rcfd_2 (selected columns used in analysis)
    - RCON series 1: bank.wrds_call_rcon_1 (selected columns used in analysis)
    - RCON series 2: bank.wrds_call_rcon_2 (selected columns used in analysis)
    - (optional) RCFN series 1: bank.wrds_call_rcfn_1 for foreign deposits if needed


"""

from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Dict, Any

import pandas as pd
import wrds
from pathlib import Path
from dotenv import load_dotenv

# -----------------------------
# Config and utilities
# -----------------------------

DEFAULT_START = "2021-12-31"
DEFAULT_END = "2023-09-30"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "_data"
DATA_DIR.mkdir(exist_ok=True)
ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)


def _require_wrds_username() -> str:
    user = os.getenv("WRDS_USERNAME")
    if not user:
        raise ValueError(
            "WRDS_USERNAME not set. Put it in your .env or export it, e.g.\n"
            "export WRDS_USERNAME='your_wrds_user'"
        )
    return user




def _connect_wrds(wrds_username: str) -> wrds.Connection:
    return wrds.Connection(wrds_username=wrds_username)


def _pull_sql(
    db: wrds.Connection,
    sql: str,
    date_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    if date_cols is None:
        date_cols = []
    return db.raw_sql(sql, date_cols=list(date_cols))


def _save_parquet(df: pd.DataFrame, filename: str) -> None:
    out_path = DATA_DIR / filename
    df.to_parquet(out_path, index=False)




# -----------------------------
# Column sets borrowed from previous replicators
# -----------------------------
# These are the columns your downstream functions expect (get_RMBS, get_treasuries, get_loans, etc.)

RCFD_1_COLS = [
    "rssd9001", "rssd9999",
    "rcfd0010",
    "rcfd1773",
    "rcfdg301", "rcfdg303", "rcfdg305", "rcfdg307", "rcfdg309", "rcfdg311",
    "rcfdg313", "rcfdg315", "rcfdg317", "rcfdg319", "rcfdg321", "rcfdg323",
    "rcfdk143", "rcfdk145", "rcfdk147", "rcfdk149",
    "rcfdk151", "rcfdk153", "rcfdk155", "rcfdk157",
    "rcfdc988", "rcfdc027",
    "rcfd1738", "rcfd1741", "rcfd1743", "rcfd1746",
    "rcfdf158", "rcfdf159",
    "rcfd5367", "rcfd5368",
    "rcfdf160", "rcfdf161",
    "rcfd1590",
    "rcfd1763", "rcfd1764",
    "rcfdb538", "rcfdb539",
    "rcfdk137",
    "rcfdk207",
    "rcfd2930",
    "rcfd3230"
]



RCFD_2_COLS = [
    "rssd9001",
    "rcfd1771",
    "rcfd0213", "rcfd1287",
    "rcfd2122",
    "rcfd1420", "rcfd1797", "rcfd1460", "rcfdb989",
    "rcfd2948",
    "rcfdg105",
    "rcfd3838", "rcfd3632",
    "rcfd2170"
]



RCON_1_COLS = [
    "rssd9001", "rssd9999",
    "rcon0071",
    "rcon1773",
    "rconht55", "rconht57",
    "rcong309", "rcong311", "rcong313", "rcong315",
    "rcong317", "rcong319", "rcong321", "rcong323",
    "rconk143", "rconk145", "rconk147", "rconk149",
    "rconk151", "rconk153", "rconk155", "rconk157",
    "rconc988", "rconc027",
    "rconht59", "rconht61",
    "rcon1743", "rcon1746",
    "rconf158", "rconf159",
    "rcon5367", "rcon5368",
    "rconf160", "rconf161",
    "rcon1590",
    "rcon1766",
    "rconb538",
    "rconk137", "rconk207",
    "rconj454", "rconj451",
    "rconb987",
    "rconmt91", "rconmt87",
    "rconhk14", "rconhk15",
    "rconb993",
    "rcon3230"
]



RCON_2_COLS = [
    "rssd9001",
    "rcon0081",
    "rcon1771",
    "rcon0213", "rcon1287",
    "rcon1738", "rcon1741",
    "rcon2122",
    "rcon1420", "rcon1797", "rcon1460",
    "rconb539",
    "rconj464",
    "rconb989",
    "rcon2200",
    "rconhk05",
    "rconj474",
    "rconb995",
    "rconk222",
    "rcon2948",
    "rcon2930",
    "rcong105",
    "rcon3838", "rcon3632",
    "rcon2170"
]



# -----------------------------
# Pull functions 
# -----------------------------

def pull_wrds_call_rcfd_1(
    db: wrds.Connection,
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    library: str = "bank",
    table: str = "wrds_call_rcfd_1",
) -> pd.DataFrame:
    """
    Pull RCFD Series 1 (domestic + foreign) call report items used for RMBS and certain loan buckets.
    """
    cols = ", ".join([f"b.{c}" for c in RCFD_1_COLS])
    sql = f"""
        SELECT {cols}
        FROM {library}.{table} AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    return _pull_sql(db, sql, date_cols=["rssd9999"])


def pull_wrds_call_rcfd_2(
    db: wrds.Connection,
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    library: str = "bank",
    table: str = "wrds_call_rcfd_2",
) -> pd.DataFrame:
    """
    Pull RCFD Series 2 (domestic + foreign) call report items used for treasuries and total assets.
    """
    cols = ", ".join([f"b.{c}" for c in RCFD_2_COLS])
    sql = f"""
        SELECT {cols}
        FROM {library}.{table} AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    return _pull_sql(db, sql, date_cols=["rssd9999"])


def pull_wrds_call_rcon_1(
    db: wrds.Connection,
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    library: str = "bank",
    table: str = "wrds_call_rcon_1",
) -> pd.DataFrame:
    """
    Pull RCON Series 1 (domestic only) for insured and uninsured deposit construction.
    """
    cols = ", ".join([f"b.{c}" for c in RCON_1_COLS])
    sql = f"""
        SELECT {cols}
        FROM {library}.{table} AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    return _pull_sql(db, sql, date_cols=["rssd9999"])


def pull_wrds_call_rcon_2(
    db: wrds.Connection,
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    library: str = "bank",
    table: str = "wrds_call_rcon_2",
) -> pd.DataFrame:
    """
    Pull RCON Series 2 (domestic only) used for treasuries, loans, and total assets.
    """
    cols = ", ".join([f"b.{c}" for c in RCON_2_COLS])
    sql = f"""
        SELECT {cols}
        FROM {library}.{table} AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    return _pull_sql(db, sql, date_cols=["rssd9999"])



#########################################################
#Testing if code is working

# user = _require_wrds_username()
# db = _connect_wrds(user)

# df = pull_wrds_call_rcfd_1(
#     db)

# # df.isna().mean().sort_values(ascending=False).head(10)

# df2 = pull_wrds_call_rcfd_2(
#     db)

# # df2.isna().mean().sort_values(ascending=False).head(10)


# df3 = pull_wrds_call_rcon_1(
#     db)

# df4 = pull_wrds_call_rcon_2(
#     db)



##############################################################

# def pull_wrds_call_rcfn_1(
#     db: wrds.Connection,
#     start_date: str = DEFAULT_START,
#     end_date: str = DEFAULT_END,
#     library: str = "bank",
#     table: str = "wrds_call_rcfn_1",
# ) -> pd.DataFrame:
#     """
#     Optional pull: RCFN Series 1 (foreign office items). Often used for foreign deposits (rcfn2200).

#     Only needed if your liability section uses foreign deposits explicitly.
#     """
#     cols = ", ".join([f"b.{c}" for c in RCFN_SERIES_1_COLS])
#     sql = f"""
#         SELECT {cols}
#         FROM {library}.{table} AS b
#         WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
#     """
#     return _pull_sql(db, sql, date_cols=["rssd9999"])


# -----------------------------
# Orchestrator
# -----------------------------

def pull_all_call_reports(
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    out_dir: Path = DATA_DIR,
) -> Dict[str, Path]:
    """
    Pull all required call report series and save them as parquet.

    Returns:
        dict mapping dataset key to output parquet path
    """
    user = _require_wrds_username()
    db = _connect_wrds(user)

    outputs: Dict[str, Path] = {}

    try:
        # RCFD 1
        rcfd1 = pull_wrds_call_rcfd_1(db, start_date, end_date)
        p1 = out_dir / "RCFD_Series_1.parquet"
        _save_parquet(rcfd1, p1)
        outputs["RCFD_Series_1"] = p1

        # RCFD 2
        rcfd2 = pull_wrds_call_rcfd_2(db, start_date, end_date)
        p2 = out_dir / "RCFD_Series_2.parquet"
        _save_parquet(rcfd2, p2)
        outputs["RCFD_Series_2"] = p2

        # RCON 1
        rcon1 = pull_wrds_call_rcon_1(db, start_date, end_date)
        p3 = out_dir / "RCON_Series_1.parquet"
        _save_parquet(rcon1, p3)
        outputs["RCON_Series_1"] = p3

        # RCON 2
        rcon2 = pull_wrds_call_rcon_2(db, start_date, end_date)
        p4 = out_dir / "RCON_Series_2.parquet"
        _save_parquet(rcon2, p4)
        outputs["RCON_Series_2"] = p4

    finally:
        db.close()

    return outputs


def main() -> None:
    start_date = os.getenv("CALLREPORT_START", DEFAULT_START)
    end_date = os.getenv("CALLREPORT_END", DEFAULT_END)

    outs = pull_all_call_reports(
        start_date=start_date,
        end_date=end_date,
        out_dir=DATA_DIR
    )

    print("Saved:")
    for k, v in outs.items():
        print(f"  {k}: {v}")




if __name__ == "__main__":
    main()
