"""
dodo.py — pydoit build file for the Jiang et al. replication project

Pipeline:
    1_pull_ffiec.py                -> downloads FFIEC zip into _data/
    2_process_ffiec.py             -> reads zip, produces figure A1 and Table A1
    3_pull_gsib_banks.py           -> writes GSIB list parquet
    4_pull_mbs_etfs.py             -> writes MBS ETF parquet
    7_pull_treasury_yields.py      -> writes Treasury yield parquet
    8_compute_market_shocks.py     -> writes market shock parquet
    9_make_table_1.py              -> writes Table 1 csv/tex
    10_original_fig.py             -> creates original exploratory figures
    export_tables.py               -> exports summary_assets.tex
    report.tex                     -> compiled into report.pdf

Usage:
    doit
    doit list
    doit clean
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
DATA_DIR = BASE_DIR / "_data"
OUT_DIR = BASE_DIR / "_output"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")

REPORT_DATE = os.getenv("REPORT_DATE", "12312025")
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")

# FFIEC data
ZIP_FILE = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"
BANK_PANEL = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
GSIB_PARQUET = DATA_DIR / "gsib_list.parquet"
TREASURY_YIELDS_PARQUET = DATA_DIR / "treasury_yields.parquet"
MBS_ETF_PARQUET = DATA_DIR / "mbs_etfs.parquet"
MARKET_SHOCKS_PARQUET = DATA_DIR / "market_shocks.parquet"

# Outputs
SUMMARY_XLSX = OUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx"
FIGURE_A1_PNG = OUT_DIR / f"figure_A1_{REPORT_DATE}.png"
FIGURE_ASSET_DIST = OUT_DIR / f"figure_asset_dist_{REPORT_DATE}.png"
FIGURE_UNINSURED = OUT_DIR / f"figure_uninsured_ratio_{REPORT_DATE}.png"
TABLE1_CSV = OUT_DIR / "table_1.csv"
TABLE1_TEX = OUT_DIR / "table_1.tex"
SUMMARY_ASSETS_TEX = OUT_DIR / "summary_assets.tex"

# LaTeX
REPORT_TEX = BASE_DIR / "report.tex"
REPORT_PDF = OUT_DIR / "report.pdf"


def _run(script_name: str) -> str:
    return f'python "{SRC_DIR / script_name}"'


DOIT_CONFIG = {
    "default_tasks": [
        "pull_ffiec",
        "process_ffiec",
        "pull_gsib",
        "pull_treasury_yields",
        "pull_mbs_etfs",
        "compute_market_shocks",
        "make_table_1",
        "export_tables",
        "export_figures",
        "compile_latex",
    ]
}


def task_pull_ffiec():
    """Download FFIEC Call Report zip from FFIEC."""
    return {
        "actions": [_run("1_pull_ffiec.py")],
        "file_dep": [str(SRC_DIR / "1_pull_ffiec.py")],
        "targets": [str(ZIP_FILE)],
        "verbosity": 2,
        "clean": True,
    }


def task_process_ffiec():
    """Process FFIEC zip into bank panel parquet, summary stats, and Figure A1."""
    return {
        "actions": [_run("2_process_ffiec.py")],
        "task_dep": ["pull_ffiec", "pull_gsib"],
        "file_dep": [
            str(ZIP_FILE),
            str(GSIB_PARQUET),
            str(SRC_DIR / "2_process_ffiec.py"),
        ],
        "targets": [
            str(BANK_PANEL),
            str(SUMMARY_XLSX),
            str(FIGURE_A1_PNG),
            str(OUT_DIR / "summary_assets.tex"),
            str(OUT_DIR / "summary_liabilities.tex"),
        ],
        "verbosity": 2,
        "clean": True,
    }



def task_pull_gsib():
    """Create GSIB list parquet used for bank classification."""
    return {
        "actions": [_run("3_pull_gsib_banks.py")],
        "file_dep": [str(SRC_DIR / "3_pull_gsib_banks.py")],
        "targets": [str(GSIB_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


def task_pull_mbs_etfs():
    """Pull MBS ETF prices used for RMBS / CMBS market proxies."""
    return {
        "actions": [_run("4_pull_mbs_etfs.py")],
        "file_dep": [str(SRC_DIR / "4_pull_mbs_etfs.py")],
        "targets": [str(MBS_ETF_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


def task_pull_treasury_yields():
    """Pull Treasury yield data used to construct bucket-specific shocks."""
    return {
        "actions": [_run("7_pull_treasury_yields.py")],
        "file_dep": [str(SRC_DIR / "7_pull_treasury_yields.py")],
        "targets": [str(TREASURY_YIELDS_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


def task_compute_market_shocks():
    """Compute maturity-specific market shocks from Treasury yields and MBS ETF data."""
    return {
        "actions": [_run("8_compute_market_shocks.py")],
        "task_dep": ["pull_treasury_yields", "pull_mbs_etfs"],
        "file_dep": [
            str(TREASURY_YIELDS_PARQUET),
            str(MBS_ETF_PARQUET),
            str(SRC_DIR / "8_compute_market_shocks.py"),
        ],
        "targets": [str(MARKET_SHOCKS_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


def task_make_table_1():
    """Build Table 1 outputs from processed bank panel and computed market shocks."""
    return {
        "actions": [_run("9_make_table_1.py")],
        "task_dep": ["process_ffiec", "pull_gsib", "compute_market_shocks"],
        "file_dep": [
            str(BANK_PANEL),
            str(GSIB_PARQUET),
            str(MARKET_SHOCKS_PARQUET),
            str(SRC_DIR / "9_make_table_1.py"),
        ],
        "targets": [str(TABLE1_CSV), str(TABLE1_TEX)],
        "verbosity": 2,
        "clean": True,
    }



def task_export_figures():
    """Export original exploratory figures for the LaTeX report."""
    return {
        "actions": [_run("10_original_fig.py")],
        "task_dep": ["process_ffiec"],
        "file_dep": [
            str(BANK_PANEL),
            str(SRC_DIR / "10_original_fig.py"),
        ],
        "targets": [str(FIGURE_ASSET_DIST), str(FIGURE_UNINSURED)],
        "verbosity": 2,
        "clean": True,
    }


def task_compile_latex():
    """Compile the LaTeX report to PDF."""
    return {
        "actions": [
            f"cd {OUT_DIR} && pdflatex -interaction=nonstopmode {REPORT_TEX}",
            f"cd {OUT_DIR} && pdflatex -interaction=nonstopmode {REPORT_TEX}",
        ],
        "task_dep": ["make_table_1", "export_tables", "export_figures"],
        "file_dep": [
            str(REPORT_TEX),
            str(TABLE1_TEX),
            str(SUMMARY_ASSETS_TEX),
            str(FIGURE_A1_PNG),
            str(FIGURE_ASSET_DIST),
            str(FIGURE_UNINSURED),
        ],
        "targets": [str(REPORT_PDF)],
        "verbosity": 2,
        "clean": True,
    }


def task_clean_outputs():
    """Remove all generated data and output files."""
    files_to_remove = [
        ZIP_FILE,
        BANK_PANEL,
        SUMMARY_XLSX,
        FIGURE_A1_PNG,
        FIGURE_ASSET_DIST,
        FIGURE_UNINSURED,
        GSIB_PARQUET,
        TREASURY_YIELDS_PARQUET,
        MBS_ETF_PARQUET,
        MARKET_SHOCKS_PARQUET,
        TABLE1_CSV,
        TABLE1_TEX,
        SUMMARY_ASSETS_TEX,
        REPORT_PDF,
    ]
    return {
        "actions": [f"rm -f {' '.join(str(p) for p in files_to_remove)}"],
        "verbosity": 2,
    }


def task_charts():
    """Build jupyter-book chartbook."""
    return {
        "actions": [
            "jupyter-book build docs_src",
            "rm -rf docs && cp -R docs_src/_build/html docs",
        ],
        "targets": ["docs/index.html"],
        "verbosity": 2,
    }