"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ
from pathlib import Path

from settings import config

DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")
USER = config("USER")

## Helpers for handling Jupyter Notebook tasks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    """Clear the output of a notebook"""
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    """Move a file to a folder"""
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        command = f"mv {from_path} {to_path}"
    else:
        command = f"move {from_path} {to_path}"
    return command


def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull data from external sources"""
    yield {
        "name": "fred",
        "doc": "Pull data from FRED",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_fred.py",
        ],
        "targets": [DATA_DIR / "fred.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_fred.py"],
        "clean": [],
    }

    yield {
        "name": "treasury_price_index",
        "doc": "Pull Treasury price index (FRED series)",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_treasury_price_index.py"],
        "targets": [DATA_DIR / "treasury_price_index.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_treasury_price_index.py"],
        "clean": [],
    }

    yield {
        "name": "mbs_etfs",
        "doc": "Pull RMBS + CMBS ETF prices (SPMB, CMBS)",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_mbs_etfs.py"],
        "targets": [DATA_DIR / "mbs_etfs.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_mbs_etfs.py"],
        "clean": [],
    }

    yield {
        "name": "gsib",
        "doc": "Write GSIB RSSD list",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_gsib_banks.py"],
        "targets": [DATA_DIR / "gsib_list.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_gsib_banks.py"],
        "clean": [],
    }

    # If your FFIEC pull script writes a parquet, add it too (update filename)
    yield {
        "name": "ffiec",
        "doc": "Pull Call Reports from FFIEC",
        "actions": ["ipython ./src/settings.py", "ipython ./src/pull_ffiec_hashir.py"],
        "targets": [DATA_DIR / "ffiec_call_reports.parquet"],  # <-- change to your real output name
        "file_dep": ["./src/settings.py", "./src/pull_ffiec_hashir.py"],
        "clean": [],
    }





def task_chart_repo_rates():
    """Example charts for Chartbook"""
    file_dep = [
        "./src/pull_fred.py",
        "./src/chart_relative_repo_rates.py",
    ]
    targets = [
        DATA_DIR / "repo_public.parquet",
        DATA_DIR / "repo_public.xlsx",
        DATA_DIR / "repo_public_relative_fed.parquet",
        DATA_DIR / "repo_public_relative_fed.xlsx",
        OUTPUT_DIR / "repo_rates.html",
        OUTPUT_DIR / "repo_rates_normalized.html",
        OUTPUT_DIR / "repo_rates_normalized_w_balance_sheet.html",
    ]

    return {
        "actions": [
            "ipython ./src/chart_relative_repo_rates.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }


notebook_tasks = {
    "01_example_notebook_interactive_ipynb": {
        "path": "./src/01_example_notebook_interactive_ipynb.py",
        "file_dep": [],
        "targets": [],
    },
    "02_example_with_dependencies_ipynb": {
        "path": "./src/02_example_with_dependencies_ipynb.py",
        "file_dep": ["./src/pull_fred.py"],
        "targets": [OUTPUT_DIR / "GDP_graph.png"],
    },
}


# fmt: off
def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """
    for notebook in notebook_tasks.keys():
        pyfile_path = Path(notebook_tasks[notebook]["path"])
        notebook_path = pyfile_path.with_suffix(".ipynb")
        yield {
            "name": notebook,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
                f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
                jupyter_execute_notebook(notebook_path),
                jupyter_to_html(notebook_path),
                mv(notebook_path, OUTPUT_DIR),
                """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                pyfile_path,
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook}.html",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
        }
# fmt: on

###############################################################
## Task below is for LaTeX compilation
###############################################################


def task_compile_latex_docs():
    """Compile the LaTeX documents to PDFs"""
    file_dep = [
        "./reports/report_example.tex",
        "./reports/my_article_header.sty",
        "./reports/slides_example.tex",
        "./reports/my_beamer_header.sty",
        "./reports/my_common_header.sty",
        "./reports/report_simple_example.tex",
        "./reports/slides_simple_example.tex",
        "./src/example_plot.py",
        "./src/example_table.py",
    ]
    targets = [
        "./reports/report_example.pdf",
        "./reports/slides_example.pdf",
        "./reports/report_simple_example.pdf",
        "./reports/slides_simple_example.pdf",
    ]

    return {
        "actions": [
            # My custom LaTeX templates
            "latexmk -xelatex -halt-on-error -cd ./reports/report_example.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/report_example.tex",  # Clean
            "latexmk -xelatex -halt-on-error -cd ./reports/slides_example.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/slides_example.tex",  # Clean
            # Simple templates based on small adjustments to Overleaf templates
            "latexmk -xelatex -halt-on-error -cd ./reports/report_simple_example.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/report_simple_example.tex",  # Clean
            "latexmk -xelatex -halt-on-error -cd ./reports/slides_simple_example.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/slides_simple_example.tex",  # Clean
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }

sphinx_targets = [
    "./docs/index.html",
]


def task_build_chartbook_site():
    """Compile Sphinx Docs"""
    notebook_scripts = [
        Path(notebook_tasks[notebook]["path"])
        for notebook in notebook_tasks.keys()
    ]
    file_dep = [
        "./README.md",
        "./chartbook.toml",
        *notebook_scripts,
    ]

    return {
        "actions": [
            "chartbook build -f",
        ],  # Use docs as build destination
        "targets": sphinx_targets,
        "file_dep": file_dep,
        "task_dep": [
            "run_notebooks",
        ],
        "clean": True,
    }

##############################################################
# R Tasks - Uncomment if you have R installed
##############################################################


###############################################################
## Stata Tasks - Uncomment if you have Stata installed
###############################################################

