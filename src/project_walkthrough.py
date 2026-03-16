# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.0
#   kernelspec:
#     display_name: finm
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
# 1. Pull FFIEC data directly
# 2. Process FFIEC data and make table 1A and figure 1A
# 3. Load the cleaned FFIEC bank panel
# 4. Inspect balance sheet exposures
# 5. Load market shocks from Treasury and MBS markets
# 6. Review simulated losses
# 7. Display the generated Table 1
#
# All data are produced by the automated pipeline using `doit`.

# %% [markdown]
# ## Overview
#
# This notebook explores the cleaned data produced by the automated `doit` pipeline.
# Before running this notebook, ensure the pipeline has been run:
# ```bash
# doit
# ```
#
# The pipeline downloads FFIEC Call Report data, processes it into a bank panel,
# computes market shocks, and generates Table 1.

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
# ## Bank Panel
#
# The bank panel is built from FFIEC Call Report schedules and contains one row per bank.
# It includes:
#
# - **Total Asset / Uninsured Deposit** — balance sheet size and deposit fragility
# - **rmbs_lt1y, rmbs_1_3y ...** — RMBS holdings broken into maturity buckets
# - **treasury_lt1y, treasury_1_3y ...** — Treasury holdings by maturity
# - **res_mtg_lt1y ...** — Residential mortgage loans by maturity
# - **other_loan_lt1y ...** — Other loans by maturity
#
# These maturity buckets are the key inputs for computing mark-to-market losses
# when interest rates rise.

# %%
bank_panel = pd.read_parquet(DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet")

print("Shape:", bank_panel.shape)

bank_panel.head()

# %% [markdown]
# ## Bank Size Categories
#
# Banks are classified into three groups based on total assets:
#
# - **Small** — below $1.384B
# - **Large (non-GSIB)** — above $1.384B but not a GSIB
# - **GSIB** — Global Systemically Important Banks (e.g. JPMorgan, BofA, Citi)

# %%
threshold = 1.384e6  # in thousands

small = (bank_panel["Total Asset"] < threshold).sum()
large_non_gsib = (bank_panel["Total Asset"] >= threshold).sum()

print(f"Total banks:      {len(bank_panel)}")
print(f"Small banks:      {small}")
print(f"Large non-GSIB:   {large_non_gsib}")

# %% [markdown]
# ## Bank Asset Distribution
#
# The vast majority of U.S. banks are small community banks with under $1B in assets.
# A handful of mega-banks (GSIBs) hold a disproportionate share of total assets.
# We use a log scale to show the full distribution clearly.

# %%
import numpy as np

fig = px.histogram(
    bank_panel,
    x=np.log10(bank_panel["Total Asset"].replace(0, np.nan)),
    nbins=100,
    title="Distribution of Bank Assets (Log Scale)",
    labels={"x": "Total Assets"},
)

fig.update_xaxes(
    tickvals=[3, 4, 5, 6, 7, 8, 9],
    ticktext=["$1M", "$10M", "$100M", "$1B", "$10B", "$100B", "$1T"]
)

# Add threshold line
fig.add_vline(
    x=np.log10(1.384e6),
    line_dash="dash",
    line_color="red",
    annotation_text="$1.384B threshold",
)

fig

# %% [markdown]
# ## Uninsured Deposits
#
# Uninsured deposits (above the FDIC $250K insurance limit) are a key measure of 
# bank fragility in Jiang et al. Banks with high uninsured deposits are more vulnerable 
# to runs when they have large unrealized losses — depositors have more incentive to 
# withdraw since their funds are not protected.

# %%
fig = px.histogram(
    bank_panel,
    x=np.log10(bank_panel["Uninsured Deposit"].replace(0, np.nan)),
    nbins=100,
    title="Distribution of Uninsured Deposits (Log Scale)",
    labels={"x": "Log10(Uninsured Deposit)"},
)

fig.update_xaxes(
    tickvals=[3, 4, 5, 6, 7, 8, 9],
    ticktext=["$1M", "$10M", "$100M", "$1B", "$10B", "$100B", "$1T"]
)

fig

# %%
bank_panel["uninsured_ratio"] = (
    bank_panel["Uninsured Deposit"] / bank_panel["Total Asset"] * 100
)

fig = px.histogram(
    bank_panel,
    x="uninsured_ratio",
    nbins=100,
    title="Uninsured Deposits as % of Total Assets",
    labels={"uninsured_ratio": "Uninsured Deposit / Total Asset (%)"},
)

fig

# %% [markdown]
# ## Balance Sheet Composition (Figure A1)
#
# The chart below shows the aggregate U.S. banking system balance sheet composition.
# On the asset side, the major categories are cash, securities, real estate loans, and other loans.
# On the liability side, the key distinction is between insured and uninsured deposits —
# uninsured deposits are the primary source of run risk in Jiang et al.

# %%
from IPython.display import Image
Image(str(OUTPUT_DIR / f"figure_A1_{REPORT_DATE}.png"))

# %% [markdown]
# ## Maturity Bucket Exposures
#
# To compute mark-to-market losses, each bank's securities and loan holdings are broken into maturity buckets: lt1y, 1_3y, 3_5y, 5_10y, 10_15y, 15plus.
#
# Note that RMBS holdings have actual maturity data from FFIEC. However, Treasury, residential mortgage, and other loan buckets are allocated using fixed assumed weights since FFIEC does not report maturity breakdowns for these categories.

# %%
prefixes = ["rmbs_", "treasury_", "res_mtg_", "other_loan_"]

rows = []
for prefix in prefixes:
    cols = [c for c in bank_panel.columns if c.startswith(prefix)]
    rows.append({
        "Asset Class": prefix.replace("_", " ").title().strip(),
        **{c.replace(prefix, ""): round(bank_panel[c].sum() * 1000 / 1e9, 1) for c in cols}
    })

bucket_summary = pd.DataFrame(rows).set_index("Asset Class")
bucket_summary.columns.name = "Maturity Bucket"

# Add a total column
bucket_summary["Total"] = bucket_summary.sum(axis=1)

bucket_summary.style.format("${:,.1f}B")

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
