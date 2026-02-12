from __future__ import annotations

from pathlib import Path
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# US GSIB holding companies (RSSD IDs) used for GSIB column splits
US_GSIB_HC = [
    ("JPMORGAN CHASE & CO.", 1039502),
    ("BANK OF AMERICA CORPORATION", 1073757),
    ("CITIGROUP INC.", 1951350),
    ("GOLDMAN SACHS GROUP, INC., THE", 2380443),
    ("MORGAN STANLEY", 2162966),
    ("WELLS FARGO & COMPANY", 1120754),
    ("BANK OF NEW YORK MELLON CORPORATION, THE", 3587146),
    ("STATE STREET CORPORATION", 1111435),
]

def pull_gsib_list() -> pd.DataFrame:
    df = pd.DataFrame(US_GSIB_HC, columns=["name", "rssd_id"])
    df["is_gsib"] = 1
    return df

def save_gsib_list(df: pd.DataFrame, filename: str = "gsib_list.parquet") -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(DATA_DIR / filename, index=False)
    print(f"Wrote {DATA_DIR / filename} | rows={len(df):,} cols={df.shape[1]}")

if __name__ == "__main__":
    df = pull_gsib_list()
    save_gsib_list(df)
