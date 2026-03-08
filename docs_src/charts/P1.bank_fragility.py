import pandas as pd
from pathlib import Path

OUTPUT = Path("_output")

df = pd.read_csv(OUTPUT / "table_1.csv")

df