import pandas as pd
from pathlib import Path

data = {
    "d_tsy_lt1y": [0.02],
    "d_tsy_1_3y": [0.025],
    "d_tsy_3_5y": [0.03],
    "d_tsy_5_10y": [0.035],
    "d_tsy_10_15y": [0.04],
    "d_tsy_15plus": [0.045],
    "rmbs_multiplier": [1.25],
}

df = pd.DataFrame(data)

Path("_data").mkdir(exist_ok=True)
df.to_parquet("_data/market_shocks.parquet", index=False)

print(df)
print("market_shocks.parquet created.")