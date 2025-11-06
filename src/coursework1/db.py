import os
import sqlite3
import pandas as pd
import argparse
import logging

DEFAULT_DB = "ges_2_1.db"
DEFAULT_ERD = "erd.md"
DEFAULT_AUDIT = "audit_gross_lt_basic.csv"

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


# =========================
# 1. Create schema
# =========================
def create_schema(conn: sqlite3.Connection):
    """Create 3NF schema with constraints."""
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA foreign_keys = OFF;
        DROP TABLE IF EXISTS fact_employment;
        DROP TABLE IF EXISTS dim_degree;
        DROP TABLE IF EXISTS dim_school;
        DROP TABLE IF EXISTS dim_university;
        DROP TABLE IF EXISTS survey_year;
        PRAGMA foreign_keys = ON;
        """
    )

    cur.executescript(
        """
        CREATE TABLE dim_university (
            university_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            university_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE dim_school (
            school_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            school_name  TEXT NOT NULL,
            university_id INTEGER NOT NULL,
            UNIQUE (university_id, school_name),
            FOREIGN KEY (university_id) REFERENCES dim_university(university_id)
                ON DELETE CASCADE
        );

        CREATE TABLE dim_degree (
            degree_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_name  TEXT NOT NULL,
            school_id    INTEGER NOT NULL,
            UNIQUE (school_id, degree_name),
            FOREIGN KEY (school_id) REFERENCES dim_school(school_id)
                ON DELETE CASCADE
        );

        CREATE TABLE survey_year (
            year_id INTEGER PRIMARY KEY AUTOINCREMENT,
            year    INTEGER UNIQUE NOT NULL
                CHECK (year BETWEEN 2000 AND 2100)
        );

        CREATE TABLE fact_employment (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_id INTEGER NOT NULL,
            year_id   INTEGER NOT NULL,
            employment_rate_overall REAL CHECK (employment_rate_overall BETWEEN 0 AND 100),
            employment_rate_ft_perm  REAL CHECK (employment_rate_ft_perm  BETWEEN 0 AND 100),
            basic_monthly_mean       REAL CHECK (basic_monthly_mean >= 0),
            basic_monthly_median     REAL CHECK (basic_monthly_median >= 0),
            gross_monthly_mean       REAL CHECK (gross_monthly_mean >= 0),
            gross_monthly_median     REAL CHECK (gross_monthly_median >= 0),
            gross_mthly_25_percentile REAL,
            gross_mthly_75_percentile REAL,
            UNIQUE(degree_id, year_id),
            FOREIGN KEY(degree_id) REFERENCES dim_degree(degree_id)
                ON DELETE CASCADE,
            FOREIGN KEY(year_id) REFERENCES survey_year(year_id)
                ON DELETE CASCADE
        );

        CREATE INDEX ix_fact_degree_year ON fact_employment(degree_id, year_id);
        CREATE INDEX ix_fact_year ON fact_employment(year_id);
        """
    )
    conn.commit()
    logging.info("Database schema created successfully.")


# =========================
# 2. CSV detection & cleaning
# =========================
def detect_csv() -> str | None:
    """Find a CSV containing 'graduate' in its filename."""
    for fn in os.listdir("."):
        if fn.lower().endswith(".csv") and "graduate" in fn.lower():
            return fn
    return None


def clean_numeric(series: pd.Series) -> pd.Series:
    """Remove commas, % signs, and coerce to float."""
    return pd.to_numeric(
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip(),
        errors="coerce",
    )


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic validation with p25>p75 warning."""
    required_cols = {"university", "school", "degree", "year"}
    missing = required_cols - set(df.columns)
    if missing:
        logging.warning(f"Missing columns: {missing}. Filling with defaults.")
        for m in missing:
            df[m] = "UNKNOWN" if m != "year" else 2020

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df[df["year"].between(2000, 2100)]

    # p25 > p75 consistency check
    if {"gross_mthly_25_percentile", "gross_mthly_75_percentile"} <= set(df.columns):
        invalid = df[
            (df["gross_mthly_25_percentile"].notna())
            & (df["gross_mthly_75_percentile"].notna())
            & (df["gross_mthly_25_percentile"] > df["gross_mthly_75_percentile"])
        ]
        if len(invalid) > 0:
            logging.warning(f"{len(invalid)} rows have p25 > p75.")
    return df


def load_csv(csv_path: str) -> pd.DataFrame:
    """Load CSV robustly with fallback encoding and clean numeric fields."""
    try:
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding="latin1")
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename_map = {
        "employment_overall": "employment_rate_overall",
        "employment_ft_perm": "employment_rate_ft_perm",
        "programme": "degree",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    num_cols = [
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
        "gross_mthly_25_percentile",
        "gross_mthly_75_percentile",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = clean_numeric(df[c])

    df = validate_dataframe(df)
    logging.info(f"CSV loaded: {len(df)} rows; columns={list(df.columns)}")
    return df


# =========================
# 3. Insert to DB with Audit
# =========================
def get_or_insert(cur, table, col, value):
    cur.execute(f"SELECT rowid FROM {table} WHERE {col} = ?", (value,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(f"INSERT INTO {table} ({col}) VALUES (?)", (value,))
    return cur.lastrowid


def load_to_db(
    conn: sqlite3.Connection, df: pd.DataFrame, audit_path: str = DEFAULT_AUDIT
):
    """Load to DB and log rows where gross < basic."""
    cur = conn.cursor()
    inserted = 0
    audit_rows = []

    for _, row in df.iterrows():
        try:
            u = str(row["university"]).strip()
            s = str(row["school"]).strip()
            d = str(row["degree"]).strip()
            y = int(row["year"])

            u_id = get_or_insert(cur, "dim_university", "university_name", u)
            cur.execute(
                "SELECT school_id FROM dim_school WHERE school_name=? AND university_id=?",
                (s, u_id),
            )
            r = cur.fetchone()
            if r:
                s_id = r[0]
            else:
                cur.execute(
                    "INSERT INTO dim_school (school_name, university_id) VALUES (?, ?)",
                    (s, u_id),
                )
                s_id = cur.lastrowid

            cur.execute(
                "SELECT degree_id FROM dim_degree WHERE degree_name=? AND school_id=?",
                (d, s_id),
            )
            r = cur.fetchone()
            if r:
                d_id = r[0]
            else:
                cur.execute(
                    "INSERT INTO dim_degree (degree_name, school_id) VALUES (?, ?)",
                    (d, s_id),
                )
                d_id = cur.lastrowid

            cur.execute("SELECT year_id FROM survey_year WHERE year=?", (y,))
            r = cur.fetchone()
            if r:
                y_id = r[0]
            else:
                cur.execute("INSERT INTO survey_year (year) VALUES (?)", (y,))
                y_id = cur.lastrowid

            bm_med = row.get("basic_monthly_median")
            gm_med = row.get("gross_monthly_median")

            if pd.notna(bm_med) and pd.notna(gm_med) and float(gm_med) < float(bm_med):
                audit_rows.append(
                    {
                        "university": u,
                        "school": s,
                        "degree": d,
                        "year": y,
                        "basic_monthly_median": bm_med,
                        "gross_monthly_median": gm_med,
                    }
                )
                gm_med = bm_med  # clamp to avoid constraint violation

            vals = [
                row.get("employment_rate_overall"),
                row.get("employment_rate_ft_perm"),
                row.get("basic_monthly_mean"),
                bm_med,
                row.get("gross_monthly_mean"),
                gm_med,
                row.get("gross_mthly_25_percentile"),
                row.get("gross_mthly_75_percentile"),
            ]
            cur.execute(
                """
                INSERT OR REPLACE INTO fact_employment (
                    degree_id, year_id,
                    employment_rate_overall, employment_rate_ft_perm,
                    basic_monthly_mean, basic_monthly_median,
                    gross_monthly_mean, gross_monthly_median,
                    gross_mthly_25_percentile, gross_mthly_75_percentile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (d_id, y_id, *vals),
            )
            inserted += 1
        except Exception as e:
            logging.error(
                f"Insert failed at row ({row.get('university', 'unknown')}): {e}"
            )

    conn.commit()
    logging.info(f"Inserted {inserted} rows into database.")

    if audit_rows:
        pd.DataFrame(audit_rows).to_csv(audit_path, index=False)
        logging.warning(f"Audit saved: {audit_path} ({len(audit_rows)} rows)")
    else:
        logging.info("No audit anomalies detected.")


# =========================
# 4. ERD (Preserved)
# =========================
def export_erd(path=DEFAULT_ERD):
    mermaid = """```mermaid
erDiagram
    DIM_UNIVERSITY ||--o{ DIM_SCHOOL : has
    DIM_SCHOOL     ||--o{ DIM_DEGREE : has
    DIM_DEGREE     ||--o{ FACT_EMPLOYMENT : has
    SURVEY_YEAR    ||--o{ FACT_EMPLOYMENT : has

    DIM_UNIVERSITY {
        int    university_id PK
        string university_name "unique, not null"
    }
    DIM_SCHOOL {
        int    school_id PK
        string school_name "not null"
        int    university_id FK
    }
    DIM_DEGREE {
        int    degree_id PK
        string degree_name "not null"
        int    school_id FK
    }
    SURVEY_YEAR {
        int year_id PK
        int year "unique, not null, 2000–2100"
    }
    FACT_EMPLOYMENT {
        int   record_id PK
        int   degree_id FK
        int   year_id   FK
        float employment_rate_overall "0–100"
        float employment_rate_ft_perm "0–100"
        float basic_monthly_mean "≥0"
        float basic_monthly_median "≥0"
        float gross_monthly_mean "≥0"
        float gross_monthly_median "≥0"
        float gross_mthly_25_percentile
        float gross_mthly_75_percentile
        string uq "unique (degree_id, year_id)"
    }
```"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)
    logging.info(f"ERD exported: {path}")


# =========================
# 5. Main
# =========================
def main():
    ap = argparse.ArgumentParser(
        description="COMP0035 Section 2.1 – Enhanced SQLite3 with Audit"
    )
    ap.add_argument("--csv", type=str, default=None)
    ap.add_argument("--db", type=str, default=DEFAULT_DB)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--erd", type=str, default=DEFAULT_ERD)
    ap.add_argument("--audit", type=str, default=DEFAULT_AUDIT)
    args = ap.parse_args()

    csv_path = args.csv or detect_csv()
    if not csv_path:
        logging.error(
            "No CSV file found. Place one containing 'graduate' or use --csv."
        )
        return

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON;")

    if args.reset:
        create_schema(conn)

    try:
        df = load_csv(csv_path)
        load_to_db(conn, df, args.audit)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        export_erd(args.erd)
        conn.close()
        logging.info(f"Database saved: {args.db}")


if __name__ == "__main__":
    main()
