import pandas as pd
import os
from pathlib import Path

#specifying the directory
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "_data"
os.chdir(DATA_DIR)


#loading parquets
rcfd_1 = pd.read_parquet(DATA_DIR / "RCFD_Series_1.parquet")
rcfd_2 = pd.read_parquet(DATA_DIR / "RCFD_Series_2.parquet")
rcon_1 = pd.read_parquet(DATA_DIR / "RCON_Series_1.parquet")
rcon_2 = pd.read_parquet(DATA_DIR / "RCON_Series_2.parquet")


