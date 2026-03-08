import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("../_output")
OUTPUT_DIR.mkdir(exist_ok=True)

def make_summary_stats(df):

    summary = df[[
        "assets",
        "uninsured_deposits",
        "securities",
        "loans"
    ]].describe().T

    summary = summary[["mean", "std", "min", "max"]]

    summary.to_latex(
        OUTPUT_DIR / "summary_stats.tex",
        caption="Summary statistics for bank balance sheet variables",
        label="tab:summary_stats",
        float_format="%.2f"
    )
