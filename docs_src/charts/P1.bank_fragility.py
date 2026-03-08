# ---
# title: Bank Fragility Table
# ---

from pathlib import Path
import pandas as pd

OUTPUT = Path("_output")

df = pd.read_csv(OUTPUT / "table_1.csv")

df