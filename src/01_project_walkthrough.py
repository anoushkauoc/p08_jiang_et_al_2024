# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Bank Balance Sheet Risk Replication (Jiang et al. 2024)
#
# This notebook provides a walkthrough of the replication pipeline used in this project.
#
# The workflow:
#
# 1. Load the cleaned FFIEC bank panel
# 2. Inspect balance sheet exposures
# 3. Load market shocks from Treasury and MBS markets
# 4. Review simulated losses
# 5. Display the generated Table 1
#
# All data are produced by the automated pipeline using `doit`.

# %%
from pathlib import Path
import pandas as pd
import plotly.express as px

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")

print("Data directory:", DATA_DIR)
print("Output directory:", OUTPUT_DIR)

# %% [markdown]
# ## Load the cleaned bank panel
#
# The bank panel contains FFIEC Call Report data transformed into maturity buckets for
# securities and loan exposures.

# %%
bank_panel = pd.read_parquet(DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet")

print("Shape:", bank_panel.shape)

bank_panel.head()

# %% [markdown]
# ## Bank asset distribution
#
# The dataset contains thousands of U.S. banks with varying balance sheet sizes.

# %%
fig = px.histogram(
    bank_panel,
    x="Total Asset",
    nbins=100,
    title="Distribution of Bank Assets",
)

fig

# %% [markdown]
# ## Uninsured deposits
#
# Uninsured deposits are important for understanding potential bank fragility.

# %%
fig = px.histogram(
    bank_panel,
    x="Uninsured Deposit",
    nbins=100,
    title="Distribution of Uninsured Deposits",
)

fig

# %% [markdown]
# ## Exposure buckets
#
# Securities and loans are mapped into maturity buckets used to estimate mark-to-market losses.

# %%
bucket_cols = [c for c in bank_panel.columns if "treasury_" in c]

bank_panel[bucket_cols].sum()

# %% [markdown]
# ## Load market shocks
#
# Market shocks are estimated from Treasury yield changes and MBS ETF price movements.

# %%
shocks = pd.read_parquet(DATA_DIR / "market_shocks.parquet")

shocks

# %% [markdown]
# ## Inspect shock magnitudes

# %%
shock_cols = [c for c in shocks.columns if c.startswith("d_tsy_")]

fig = px.bar(
    x=shock_cols,
    y=shocks.loc[0, shock_cols],
    title="Treasury Yield Shock by Maturity",
)

fig

# %% [markdown]
# ## Load Table 1 results
#
# Table 1 summarizes simulated mark-to-market losses by bank size group.

# %%
table1 = pd.read_csv(OUTPUT_DIR / "table_1.csv", index_col=0)

table1

# %% [markdown]
# ## Bank counts by size
#
# The dataset categorizes banks into:
#
# * Small banks
# * Large non-GSIB banks
# * GSIB banks

# %%
bank_panel["Total Asset"].describe()

# %%
print("Total banks:", len(bank_panel))

# %% [markdown]
# ## Summary
#
# This notebook demonstrates the key outputs of the replication pipeline:
#
# * Cleaned FFIEC bank panel
# * Estimated market shocks
# * Mark-to-market loss calculations
# * Table 1 summary statistics
#
# The full pipeline is automated using `doit` to ensure reproducibility.
