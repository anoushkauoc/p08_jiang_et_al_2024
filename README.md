# Bank Fragility Monitor
======================

## About this project

Replication and extension of Jiang et al. (2024) using FFIEC Call Report data to monitor
U.S. bank fragility. This project replicates Table 1, Table A1, and Figure A1 from the
original paper, and extends the analysis to the most recently available Call Report data.

This project builds on the prior replication work of **Xitaaz Rampersad and Samuel Rubidge**,
whose WRDS-based pipeline served as a reference for variable construction and analysis.
Our implementation differs in that we scrape raw Call Report files directly from the FFIEC
website rather than using WRDS.

## Project Responsibilities

| Task | Owner |
|------|-------|
| FFIEC data scraping (`1_pull_ffiec.py`) | Hashir Bawany |
| FFIEC data cleaning and processing (`2_process_ffiec.py`) | Hashir Bawany |
| Table A1 (balance sheet summary stats) | Hashir Bawany |
| Figure A1 (balance sheet composition) | Hashir Bawany |
| LaTeX report (`report.tex`) | Hashir Bawany |
| GSIB bank list (`3_pull_gsib_banks.py`) | Anoushka Gehani |
| MBS ETF data (`4_pull_mbs_etfs.py`) | Anoushka Gehani |
| Treasury yield data (`7_pull_treasury_yields.py`) | Anoushka Gehani |
| Market shock computation (`8_compute_market_shocks.py`) | Anoushka Gehani |
| Table 1 (mark-to-market losses) (`9_make_table_1.py`) | Anoushka Gehani |
| Original exploratory figures (`10_original_fig.py`) | Anoushka Gehani |

## Quick Start

You must have TexLive (or another LaTeX distribution) installed on your computer and
available in your path. You can download it here
([Windows](https://tug.org/texlive/windows.html#install) and
[Mac](https://tug.org/mactex/mactex-download.html) installers).

First, install the `conda` package manager (e.g., via
[miniforge](https://github.com/conda-forge/miniforge)).

Create and activate the conda environment:
```bash
conda env create -f environment.yml
conda activate p08_jiang_et_al_2024
```

Set up your `.env` file (see `.env.example` for required variables):
```bash
cp .env.example .env
# then edit .env to set REPORT_DATE and other config
```

Finally, run the full pipeline:
```bash
doit
```

This will:
1. Scrape the FFIEC website and download the Call Report zip file
2. Process the raw data into a clean bank panel
3. Pull market data (Treasury yields, MBS ETFs)
4. Compute mark-to-market losses
5. Generate all tables and figures
6. Compile the LaTeX report to PDF

### How the FFIEC Download Works

The FFIEC website does not expose a conventional REST API. We automate the download
using **Selenium**, a Python package that controls a Chrome browser instance
programmatically. This allows us to navigate the FFIEC CDR bulk download page,
select the correct report date and format, and trigger the download automatically.

### Other Commands

#### Unit Tests
```bash
pytest --doctest-modules
```

#### Code Formatting
```bash
ruff format . && ruff check --select I --fix . && ruff check --fix .
```

#### Setting Environment Variables

On Mac/Linux:
```bash
set -a
source .env
set +a
```

On Windows (PowerShell):
```powershell
Get-Content .env | ForEach-Object { if ($_ -match '^([^=]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process') } }
```

## General Directory Structure

- `src/` --- All Python scripts for data pulling, processing, and analysis
- `_data/` --- Downloaded and processed data (excluded from Git, recreatable by running `doit`)
- `_output/` --- Generated tables, figures, and the compiled PDF report
- `data_manual/` --- Manually curated data that cannot be auto-recreated (version controlled)
- `assets/` --- Hand-drawn figures or other static assets
- `report.tex` --- LaTeX source for the final report

## Data and Output Storage

The `_data` and `_output` directories are excluded from Git and can be fully recreated
by running `doit`. Any data that cannot be automatically recreated is stored in
`data_manual/` and is version controlled.

Directory paths and other configuration (e.g., `REPORT_DATE`) are managed via the
`.env` file and `settings.py`. The `.env` file must never be committed to Git.

## References

Jiang, E., Matvos, G., Piskorski, T., & Seru, A. (2024).
*Monetary Tightening and U.S. Bank Fragility in 2023:
Mark-to-Market Losses and Uninsured Depositor Runs?*
Journal of Financial Economics.

Rampersad, X. & Rubidge, S. Prior replication project (WRDS-based pipeline),
used as a reference for variable construction and analysis structure.
