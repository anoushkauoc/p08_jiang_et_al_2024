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


# ── Task 2: Process FFIEC data ────────────────────────────────────────────────
def task_process_ffiec():
    return {
        'actions':   [f'python "{SRC_DIR / "processing_ffiec_data_3.py"}"'],
        'task_dep':  ['pull_ffiec'],   
        'file_dep':  [str(ZIP_FILE)],
        'targets':   [str(PARQUET), str(SUMMARY), str(FIGURE)],
        'uptodate':  [lambda: all(p.exists() for p in [PARQUET, SUMMARY, FIGURE])],
        'verbosity': 2,
    }