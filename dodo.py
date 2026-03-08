"""
dodo.py  —  pydoit build file for P08_JIANG_ET_AL_2024
Pipeline:
    1. pull_ffiec_hashir.py   → downloads FFIEC zip into _data/
    2. processing_ffiec_data_3.py → reads zip, writes parquet + xlsx + png to _data/ and _output/

Usage:
    doit          # run all tasks
    doit list     # show available tasks
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Resolve paths ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR  = BASE_DIR / "src"
DATA_DIR = BASE_DIR / "_data"
OUT_DIR  = BASE_DIR / "_output"

# ── Load .env to get REPORT_DATE values ──────────────────────────────────────
load_dotenv(BASE_DIR / '.env')

REPORT_DATE       = os.getenv("REPORT_DATE",       "03312022")   # used by processing script
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "03/31/2022") # used by pull script

# ── Derived file paths ────────────────────────────────────────────────────────
ZIP_FILE    = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"
PARQUET     = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
SUMMARY     = OUT_DIR  / f"summary_stats_{REPORT_DATE}.xlsx"
FIGURE      = OUT_DIR  / f"figure_A1_{REPORT_DATE}.png"


# ── Task 1: Download FFIEC data ───────────────────────────────────────────────
def task_pull_ffiec():
    """Download FFIEC Call Report zip from cdr.ffiec.gov via Selenium."""
    return {
        'actions':  [f'python "{SRC_DIR / "pull_ffiec_hashir.py"}"'],
        'targets':  [str(ZIP_FILE)],
        'uptodate': [lambda: ZIP_FILE.exists()],  # skip if zip already present
        'verbosity': 2,
    }


# ── Task 2: Process FFIEC data ──────────────────────────"""
dodo.py — pydoit build file for the Jiang et al. replication project

Current pipeline:
    1. pull_ffiec_hashir.py         -> downloads FFIEC zip into _data/
    2. processing_ffiec_data_3.py   -> reads zip, writes bank_panel parquet and A1-style outputs
    3. pull_gsib_banks.py           -> writes GSIB list parquet
    4. pull_treasury_price_index.py -> writes Treasury price index parquet
    5. pull_mbs_etfs.py             -> writes MBS ETF parquet
    6. make_table_1.py              -> writes Table 1 csv/tex

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

REPORT_DATE = os.getenv("REPORT_DATE", "03312022")
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "03/31/2022")

# ---------------------------------------------------------------------
# Derived file paths
# ---------------------------------------------------------------------
ZIP_FILE = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"

BANK_PANEL = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"

SUMMARY_XLSX = OUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx"
FIGURE_A1_PNG = OUT_DIR / f"figure_A1_{REPORT_DATE}.png"

GSIB_PARQUET = DATA_DIR / "gsib_list.parquet"
TREASURY_PARQUET = DATA_DIR / "treasury_price_index.parquet"
MBS_ETF_PARQUET = DATA_DIR / "mbs_etfs.parquet"

TABLE1_CSV = OUT_DIR / "table_1.csv"
TABLE1_TEX = OUT_DIR / "table_1.tex"

# Optional future outputs if/when you add these scripts
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
        "pull_treasury",
        "pull_mbs_etfs",
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
# Task 4: Pull Treasury price index
# ---------------------------------------------------------------------
def task_pull_treasury():
    """Pull Treasury price index used for mark-to-market Treasury shocks."""
    return {
        "actions": [_run("pull_treasury_price_index.py")],
        "targets": [str(TREASURY_PARQUET)],
        "uptodate": [lambda: _exists(TREASURY_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 5: Pull MBS ETF prices
# ---------------------------------------------------------------------
def task_pull_mbs_etfs():
    """Pull MBS ETF prices used as RMBS / CMBS proxies."""
    return {
        "actions": [_run("pull_mbs_etfs.py")],
        "targets": [str(MBS_ETF_PARQUET)],
        "uptodate": [lambda: _exists(MBS_ETF_PARQUET)],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Task 6: Make Table 1
# ---------------------------------------------------------------------
def task_make_table_1():
    """Build Table 1 outputs from processed bank panel and price inputs."""
    return {
        "actions": [_run("make_table_1.py")],
        "task_dep": ["process_ffiec", "pull_gsib", "pull_treasury", "pull_mbs_etfs"],
        "file_dep": [
            str(BANK_PANEL),
            str(GSIB_PARQUET),
            str(TREASURY_PARQUET),
            str(MBS_ETF_PARQUET),
            str(SRC_DIR / "make_table_1.py"),
        ],
        "targets": [str(TABLE1_CSV), str(TABLE1_TEX)],
        "uptodate": [lambda: _all_exist([TABLE1_CSV, TABLE1_TEX])],
        "verbosity": 2,
        "clean": True,
    }


# ---------------------------------------------------------------------
# Optional future tasks
# Uncomment once these scripts exist
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
        TREASURY_PARQUET,
        MBS_ETF_PARQUET,
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
    }──────────────────────
def task_process_ffiec():
    return {
        'actions':   [f'python "{SRC_DIR / "processing_ffiec_data_3.py"}"'],
        'task_dep':  ['pull_ffiec'],   
        'file_dep':  [str(ZIP_FILE)],
        'targets':   [str(PARQUET), str(SUMMARY), str(FIGURE)],
        'uptodate':  [lambda: all(p.exists() for p in [PARQUET, SUMMARY, FIGURE])],
        'verbosity': 2,
    }
