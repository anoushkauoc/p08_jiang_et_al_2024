"""
dodo.py — pydoit build file for the Jiang et al. replication project

Pipeline:
    1. pull_ffiec_hashir.py         -> downloads FFIEC zip into _data/
    2. processing_ffiec_data_3.py   -> reads zip, writes bank panel and A1-style outputs
    3. pull_gsib_banks.py           -> writes GSIB list parquet
    4. pull_treasury_yields.py      -> writes Treasury yield parquet
    5. pull_mbs_etfs.py             -> writes MBS ETF parquet
    6. compute_market_shocks.py     -> writes market shock parquet
    7. make_table_1.py              -> writes Table 1 csv/tex

Usage:
    doit
    doit list
    doit clean
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
DATA_DIR = BASE_DIR / "_data"
OUT_DIR = BASE_DIR / "_output"

DATA_DIR.mkdir(exist_ok=True)
OUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------
load_dotenv(BASE_DIR / ".env")

REPORT_DATE = os.getenv("REPORT_DATE", "12312025")
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")

# ---------------------------------------------------------------------
# Derived file paths
# ---------------------------------------------------------------------
ZIP_FILE = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"

BANK_PANEL = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"

SUMMARY_XLSX = OUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx"
FIGURE_A1_PNG = OUT_DIR / f"figure_A1_{REPORT_DATE}.png"

GSIB_PARQUET = DATA_DIR / "gsib_list.parquet"
TREASURY_YIELDS_PARQUET = DATA_DIR / "treasury_yields.parquet"
MBS_ETF_PARQUET = DATA_DIR / "mbs_etfs.parquet"
MARKET_SHOCKS_PARQUET = DATA_DIR / "market_shocks.parquet"

TABLE1_CSV = OUT_DIR / "table_1.csv"
TABLE1_TEX = OUT_DIR / "table_1.tex"

# Optional future outputs
TABLEA1_CSV = OUT_DIR / "table_A1.csv"
TABLEA1_TEX = OUT_DIR / "table_A1.tex"
FIGUREA1_FINAL = OUT_DIR / "figure_A1_final.png"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _run(script_name: str) -> str:
    return f'python "{SRC_DIR / script_name}"'


def _exists(path: Path) -> bool:
    return Path(path).exists()


def _all_exist(paths: list[Path]) -> bool:
    return all(Path(p).exists() for p in paths)


DOIT_CONFIG = {
    "default_tasks": [
        "pull_ffiec",
        "process_ffiec",
        "pull_gsib",
        "pull_treasury_yields",
        "pull_mbs_etfs",
        "compute_market_shocks",
        "make_table_1",
    ]
}


# ---------------------------------------------------------------------
# Task 1: Download FFIEC zip
# ---------------------------------------------------------------------
def task_pull_ffiec():
    """Download FFIEC Call Report zip from FFIEC."""
    return {
        "actions": [_run("pull_ffiec_hashir.py")],
        "targets": [str(ZIP_FILE)],
        "uptodate": [lambda: _exists(ZIP_FILE)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 2: Process FFIEC zip into bank panel + summary outputs
# ---------------------------------------------------------------------
def task_process_ffiec():
    """Process FFIEC zip into bank panel parquet and current summary outputs."""
    return {
        "actions": [_run("processing_ffiec_data_3.py")],
        "task_dep": ["pull_ffiec"],
        "file_dep": [str(ZIP_FILE)],
        "targets": [str(BANK_PANEL), str(SUMMARY_XLSX), str(FIGURE_A1_PNG)],
        "uptodate": [lambda: _all_exist([BANK_PANEL, SUMMARY_XLSX, FIGURE_A1_PNG])],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 3: Build GSIB list
# ---------------------------------------------------------------------
def task_pull_gsib():
    """Create GSIB list parquet used for bank classification."""
    return {
        "actions": [_run("pull_gsib_banks.py")],
        "targets": [str(GSIB_PARQUET)],
        "uptodate": [lambda: _exists(GSIB_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 4: Pull Treasury yields
# ---------------------------------------------------------------------
def task_pull_treasury_yields():
    """Pull Treasury yield data used to construct bucket-specific shocks."""
    return {
        "actions": [_run("pull_treasury_yields.py")],
        "targets": [str(TREASURY_YIELDS_PARQUET)],
        "uptodate": [lambda: _exists(TREASURY_YIELDS_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 5: Pull MBS ETF prices
# ---------------------------------------------------------------------
def task_pull_mbs_etfs():
    """Pull MBS ETF prices used for RMBS / CMBS market proxies."""
    return {
        "actions": [_run("pull_mbs_etfs.py")],
        "targets": [str(MBS_ETF_PARQUET)],
        "uptodate": [lambda: _exists(MBS_ETF_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 6: Compute market shocks
# ---------------------------------------------------------------------
def task_compute_market_shocks():
    """Compute maturity-specific market shocks from Treasury yields and MBS ETF data."""
    return {
        "actions": [_run("compute_market_shocks.py")],
        "task_dep": ["pull_treasury_yields", "pull_mbs_etfs"],
        "file_dep": [str(TREASURY_YIELDS_PARQUET), str(MBS_ETF_PARQUET)],
        "targets": [str(MARKET_SHOCKS_PARQUET)],
        "uptodate": [lambda: _exists(MARKET_SHOCKS_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 7: Make Table 1
# ---------------------------------------------------------------------
def task_make_table_1():
    """Build Table 1 outputs from processed bank panel and computed market shocks."""
    return {
        "actions": [_run("make_table_1.py")],
        "task_dep": ["process_ffiec", "pull_gsib", "compute_market_shocks"],
        "file_dep": [
            str(BANK_PANEL),
            str(GSIB_PARQUET),
            str(MARKET_SHOCKS_PARQUET),
            str(SRC_DIR / "make_table_1.py"),
        ],
        "targets": [str(TABLE1_CSV), str(TABLE1_TEX)],
        "uptodate": [lambda: _all_exist([TABLE1_CSV, TABLE1_TEX])],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Optional future tasks
# ---------------------------------------------------------------------
# def task_make_table_A1():
#     """Build Table A1 outputs."""
#     return {
#         "actions": [_run("make_table_A1.py")],
#         "task_dep": ["process_ffiec", "pull_gsib"],
#         "file_dep": [
#             str(BANK_PANEL),
#             str(GSIB_PARQUET),
#             str(SRC_DIR / "make_table_A1.py"),
#         ],
#         "targets": [str(TABLEA1_CSV), str(TABLEA1_TEX)],
#         "uptodate": [lambda: _all_exist([TABLEA1_CSV, TABLEA1_TEX])],
#         "verbosity": 2,
#         "clean": True,
#     }
#
#
# def task_make_figure_A1():
#     """Build final Figure A1."""
#     return {
#         "actions": [_run("make_figure_A1.py")],
#         "task_dep": ["process_ffiec"],
#         "file_dep": [
#             str(BANK_PANEL),
#             str(SRC_DIR / "make_figure_A1.py"),
#         ],
#         "targets": [str(FIGUREA1_FINAL)],
#         "uptodate": [lambda: _exists(FIGUREA1_FINAL)],
#         "verbosity": 2,
#         "clean": True,
#     }


# ---------------------------------------------------------------------
# Clean task
# ---------------------------------------------------------------------
def task_clean_outputs():
    """Remove generated output files."""
    files_to_remove = [
        ZIP_FILE,
        BANK_PANEL,
        SUMMARY_XLSX,
        FIGURE_A1_PNG,
        GSIB_PARQUET,
        TREASURY_YIELDS_PARQUET,
        MBS_ETF_PARQUET,
        MARKET_SHOCKS_PARQUET,
        TABLE1_CSV,
        TABLE1_TEX,
        TABLEA1_CSV,
        TABLEA1_TEX,
        FIGUREA1_FINAL,
    ]

    existing = [p for p in files_to_remove if p.exists()]

    return {
        "actions": [(lambda: [p.unlink() for p in existing])],
        "verbosity": 2,
    }


def task_tables():
    return {
        "actions": ["python src/make_table_1.py"],
        "targets": ["_output/table1.tex"],
    }

def task_summary():
    return {
        "actions": ["python src/make_summary_stats.py"],
        "targets": ["_output/summary_stats.tex"],
    }

def task_charts():
    return {
        "actions": ["python src/make_charts.py"],
        "targets": ["_output/bank_asset_distribution.png"],
    }
