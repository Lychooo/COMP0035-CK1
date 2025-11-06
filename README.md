# COMP0035 Coursework 1 – Code README

This repository provides three Python scripts to (i) explore a CSV dataset, (ii) prepare it, and (iii) build an SQLite database.  
It documents only what the code actually uses.

## Repository Layout
```
src/coursework1/
├─ 1.1.py   # Data description & exploration (EDA)
├─ 1.2.py   # Data preparation
└─ db.py    # SQLite database creation (3NF schema + audit)
```

## Environment Setup
```bash
python -m venv .venv
. .venv/bin/activate            # macOS/Linux
# .\.venv\Scripts\Activate.ps1  # Windows PowerShell

python -m pip install --upgrade pip
pip install -r requirements.txt
```

### requirements.txt (used by the code)
```
pandas>=2.2,<3.0
matplotlib>=3.8,<4.0
tabulate>=0.9
```
- `tabulate` is required for `DataFrame.to_markdown()` used by 1.1/1.2.
- Standard library modules used (no install required): `argparse`, `pathlib`, `logging`, `sqlite3`.

If Windows PowerShell blocks activation:
```bash
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## How to Run

### 1) Section 1.1 – EDA (structure, types, missing values, distributions, trends)
```bash
python src/coursework1/1.1.py --csv "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (3).csv"
```
**Outputs → `eda_output/`** (auto-created)
- `00_overview.md/csv`, `01_dtypes.md/csv`, `02_missing_by_column.md/csv`, etc.
- Plots: `hist_*.png`, `box_*.png`, `trend_*.png`

### 2) Section 1.2 – Data Preparation (cleaning + 3 question-oriented views)
```bash
python src/coursework1/1.2.py --csv "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (3).csv"
```
**Outputs → `prep_output/`** (auto-created)
- Cleaned preview/table markdown & csv
- Plots: `q1_bar_*_latest_<YEAR>.png`, `q2_trend_lines_salary_by_university.png`, `q3_scatter_*.png`

### 3) Section 2 – SQLite Database (3NF schema + audit)
- Detects a CSV in the working directory containing the word "graduate" **or** use `--csv` explicitly.
- Writes an audit if `gross_monthly_median < basic_monthly_median` and clamps the inserted value to avoid constraint violations.
```bash
python src/coursework1/db.py --csv "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (3).csv" --reset
```
**Outputs (in repo root):**
- `ges_2_1.db` – SQLite database
- `erd.md` – ER diagram (Mermaid)
- `audit_gross_lt_basic.csv` – salary anomaly log (only if anomalies exist)

## Notes
- Default CSV paths in `1.1.py` / `1.2.py` point to the repository root. Use `--csv` to override.
- `db.py` creates indices and enforces checks (percentages 0..100, non-negative salaries, valid years 2000..2100).
- All figures are generated with `pandas.DataFrame.plot()`/matplotlib (as used in code).
