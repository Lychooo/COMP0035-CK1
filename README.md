[![Open in Codespaces](https://classroom.github.com/assets/launch-codespace-2972f46106e565e64193e422d61a12cf1da4916b45550586e14ef0a7c637dd04.svg)](https://classroom.github.com/open-in-codespaces?assignment_repo_id=20779389)
# COMP0035 coursework repository
This repository contains the code for:
- Section 1.1 – Data description & exploration (EDA)
- Section 1.2 – Data preparation
- Section 2.1 – Database design (SQLAlchemy ORM)
- Section 2.2 – Database creation (sqlite3 only)

## Environment setup and run instructions (Windows PowerShell)

Create and activate a virtual environment, then install dependencies:
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install -r requirements.txt

Note: Sections 1.1 and 1.2 use pandas.DataFrame.to_markdown(), which requires tabulate (already included in requirements.txt).

Run the scripts (they will read the CSV path configured in each script; change if needed):
    python src/coursework1/1.1.py
    python src/coursework1/1.2.py
    python src/coursework1/2.1.py
    python src/coursework1/2.2.py

Outputs:
- eda_output/  → Section 1.1 results (tables and plots)
- prep_output/ → Section 1.2, 2.1, 2.2 results (tables, plots, ERD, anomaly logs)
- ges.db / ges_sqlite.db → SQLite databases
- Markdown (.md) and CSV tables are generated for easy report inclusion

Dependencies (requirements.txt):
    pandas>=2.2,<3.0
    matplotlib>=3.8,<4.0
    SQLAlchemy>=2.0,<3.0
    tabulate>=0.9

Tested with Python 3.10–3.13. If PowerShell shows permission errors when activating the venv:
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
