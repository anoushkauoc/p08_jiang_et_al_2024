import pandas as pd
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

START = "2024-12-31"
END   = "2025-12-31"

yields = pd.read_parquet(DATA_DIR/"treasury_yields.parquet")
mbs = pd.read_parquet(DATA_DIR/"mbs_prices.parquet")

yields = yields.set_index("date")
mbs = mbs.set_index("date")

start = yields.loc[:START].iloc[-1]
end = yields.loc[:END].iloc[-1]

# Duration approximation
durations = {
    "1Y":1,
    "3Y":2.5,
    "5Y":4.5,
    "10Y":8,
    "20Y":15,
    "30Y":20
}

shocks = {}

for k in durations:
    dy = (end[k]-start[k])/100
    shocks[f"d_tsy_{k}"] = -durations[k]*dy

# RMBS multiplier
rmbs_return = mbs.loc[END,"rmbs"]/mbs.loc[START,"rmbs"] - 1
tsy_return = sum(shocks.values())/len(shocks)

shocks["rmbs_multiplier"] = rmbs_return/tsy_return

pd.DataFrame([shocks]).to_parquet(DATA_DIR/"market_shocks.parquet")

print("Saved market_shocks.parquet")main()
