from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd

# ======== Config ========
CSV_PATH = r"C:\Users\30769\Desktop\comp0035-cw-Lychooo\7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv"
DB_PATH = Path("ges_sqlite.db")  # 本节专用数据库（不覆盖 2.1 的 ges.db）
OUT_DIR = Path("prep_output")
OUT_DIR.mkdir(exist_ok=True, parents=True)

# 违反 gross>=basic 的处理策略： "clamp"（把 gross 调到 basic） / "drop"（丢弃）
GROSS_BASIC_STRATEGY = "clamp"

# ======== Schema (SQLite DDL) ========
DDL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS survey_result;
DROP TABLE IF EXISTS programme;
DROP TABLE IF EXISTS survey_year;
DROP TABLE IF EXISTS university;

CREATE TABLE university (
    university_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    short_name      TEXT    UNIQUE
);

CREATE TABLE programme (
    programme_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    university_id   INTEGER NOT NULL,
    name            TEXT    NOT NULL,
    CONSTRAINT uq_programme_university_name UNIQUE (university_id, name),
    CONSTRAINT fk_prog_uni FOREIGN KEY (university_id)
        REFERENCES university(university_id)
        ON DELETE CASCADE
);

CREATE TABLE survey_year (
    year_id INTEGER PRIMARY KEY AUTOINCREMENT,
    year    INTEGER NOT NULL UNIQUE,
    CONSTRAINT ck_year_range CHECK (year BETWEEN 2000 AND 2100)
);

CREATE TABLE survey_result (
    result_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    programme_id            INTEGER NOT NULL,
    year_id                 INTEGER NOT NULL,
    employment_rate_overall REAL,
    employment_rate_ft_perm REAL,
    basic_monthly_mean      REAL,
    basic_monthly_median    REAL,
    gross_monthly_mean      REAL,
    gross_monthly_median    REAL,
    CONSTRAINT uq_prog_year UNIQUE (programme_id, year_id),
    CONSTRAINT fk_res_prog FOREIGN KEY (programme_id)
        REFERENCES programme(programme_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_res_year FOREIGN KEY (year_id)
        REFERENCES survey_year(year_id)
        ON DELETE CASCADE,
    -- 业务约束
    CONSTRAINT ck_emp_overall CHECK (
        employment_rate_overall IS NULL OR
        (employment_rate_overall >= 0 AND employment_rate_overall <= 100)
    ),
    CONSTRAINT ck_emp_ft CHECK (
        employment_rate_ft_perm IS NULL OR
        (employment_rate_ft_perm >= 0 AND employment_rate_ft_perm <= 100)
    ),
    CONSTRAINT ck_emp_logic CHECK (
        employment_rate_overall IS NULL OR
        employment_rate_ft_perm IS NULL OR
        employment_rate_ft_perm <= employment_rate_overall
    ),
    CONSTRAINT ck_salary_nonneg CHECK (
        (basic_monthly_mean    IS NULL OR basic_monthly_mean    >= 0) AND
        (basic_monthly_median  IS NULL OR basic_monthly_median  >= 0) AND
        (gross_monthly_mean    IS NULL OR gross_monthly_mean    >= 0) AND
        (gross_monthly_median  IS NULL OR gross_monthly_median  >= 0)
    ),
    CONSTRAINT ck_salary_logic CHECK (
        (gross_monthly_mean   IS NULL OR basic_monthly_mean   IS NULL OR gross_monthly_mean   >= basic_monthly_mean) AND
        (gross_monthly_median IS NULL OR basic_monthly_median IS NULL OR gross_monthly_median >= basic_monthly_median)
    )
);

-- 索引
CREATE INDEX ix_programme_university ON programme(university_id);
CREATE INDEX ix_year_year ON survey_year(year);
CREATE INDEX ix_result_prog_year ON survey_result(programme_id, year_id);
"""


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def read_and_prepare(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    # 标准化关键文本
    for c in ["university", "degree"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # 转数值
    num_cols = [
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
        "year",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 只保留必要列；主键字段不缺失
    needed = {
        "university",
        "degree",
        "year",
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
    }
    df = df[[c for c in df.columns if c in needed]].dropna(
        subset=["university", "degree", "year"]
    )
    # 合理范围软过滤
    df = df[df["year"].between(2000, 2100)]
    for col in ["employment_rate_overall", "employment_rate_ft_perm"]:
        if col in df.columns:
            df = df[(df[col].isna()) | (df[col].between(0, 100))]
    for col in [
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
    ]:
        if col in df.columns:
            df = df[(df[col].isna()) | (df[col] >= 0)]

    # 处理 gross < basic
    EPS = 1e-6
    mask_mean = (
        df["gross_monthly_mean"].notna()
        & df["basic_monthly_mean"].notna()
        & (df["gross_monthly_mean"] + EPS < df["basic_monthly_mean"])
    )
    mask_median = (
        df["gross_monthly_median"].notna()
        & df["basic_monthly_median"].notna()
        & (df["gross_monthly_median"] + EPS < df["basic_monthly_median"])
    )
    bad_mask = mask_mean | mask_median
    if bad_mask.any():
        audit = OUT_DIR / "2.2_anomaly_gross_lt_basic.csv"
        df.loc[bad_mask].to_csv(audit, index=False, encoding="utf-8-sig")
        print(
            f"[audit] gross<basic rows saved to {audit.resolve()}  rows={int(bad_mask.sum())}"
        )
        if GROSS_BASIC_STRATEGY.lower() == "clamp":
            df.loc[mask_mean, "gross_monthly_mean"] = df.loc[
                mask_mean, "basic_monthly_mean"
            ]
            df.loc[mask_median, "gross_monthly_median"] = df.loc[
                mask_median, "basic_monthly_median"
            ]
        elif GROSS_BASIC_STRATEGY.lower() == "drop":
            df = df[~bad_mask].copy()

    return df


def upsert_dim_tables(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    # university
    unis = sorted(df["university"].dropna().unique().tolist())
    cur.executemany(
        "INSERT OR IGNORE INTO university(name) VALUES (?)", [(u,) for u in unis]
    )

    # survey_year
    years = sorted(df["year"].dropna().astype(int).unique().tolist())
    cur.executemany(
        "INSERT OR IGNORE INTO survey_year(year) VALUES (?)", [(y,) for y in years]
    )

    # programme（依赖 university）
    # 先查 map
    uni_map = dict(cur.execute("SELECT name, university_id FROM university").fetchall())
    # 组合 (university_id, degree_name)
    prows = []
    for _, r in df[["university", "degree"]].drop_duplicates().iterrows():
        u_id = uni_map.get(str(r["university"]))
        if u_id is not None:
            prows.append((u_id, str(r["degree"])))
    cur.executemany(
        "INSERT OR IGNORE INTO programme(university_id, name) VALUES (?, ?)", prows
    )
    conn.commit()


def load_fact_table(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    # 外键映射
    uni_map = dict(cur.execute("SELECT name, university_id FROM university").fetchall())
    prog_map = dict(
        cur.execute(
            """
        SELECT p.university_id || '||' || p.name, p.programme_id
        FROM programme p
    """
        ).fetchall()
    )
    year_map = dict(cur.execute("SELECT year, year_id FROM survey_year").fetchall())

    # 批量准备
    rows = []
    for _, r in df.iterrows():
        u_id = uni_map.get(str(r["university"]))
        key = f"{u_id}||{str(r['degree'])}" if u_id is not None else None
        p_id = prog_map.get(key)
        y_id = year_map.get(int(r["year"]))
        if (p_id is None) or (y_id is None):
            continue
        rows.append(
            (
                p_id,
                y_id,
                r.get("employment_rate_overall"),
                r.get("employment_rate_ft_perm"),
                r.get("basic_monthly_mean"),
                r.get("basic_monthly_median"),
                r.get("gross_monthly_mean"),
                r.get("gross_monthly_median"),
            )
        )

    cur.executemany(
        """
        INSERT OR IGNORE INTO survey_result(
            programme_id, year_id,
            employment_rate_overall, employment_rate_ft_perm,
            basic_monthly_mean, basic_monthly_median,
            gross_monthly_mean, gross_monthly_median
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        rows,
    )
    conn.commit()


def main():
    if DB_PATH.exists():
        DB_PATH.unlink()  # 可重复执行
    conn = sqlite3.connect(DB_PATH)
    try:
        create_schema(conn)
        df = read_and_prepare(CSV_PATH)
        upsert_dim_tables(conn, df)
        load_fact_table(conn, df)
        # 可选：创建只读视图便于 2.3/报告使用
        conn.executescript(
            """
        DROP VIEW IF EXISTS v_result;
        CREATE VIEW v_result AS
        SELECT u.name AS university, p.name AS degree, y.year,
               employment_rate_overall, employment_rate_ft_perm,
               basic_monthly_mean, basic_monthly_median,
               gross_monthly_mean, gross_monthly_median
        FROM survey_result sr
        JOIN programme p ON p.programme_id = sr.programme_id
        JOIN university u ON u.university_id = p.university_id
        JOIN survey_year y ON y.year_id = sr.year_id;
        """
        )
        conn.commit()

        # 输出行数统计（便于写进报告）
        cur = conn.cursor()
        n_uni = cur.execute("SELECT COUNT(*) FROM university").fetchone()[0]
        n_prog = cur.execute("SELECT COUNT(*) FROM programme").fetchone()[0]
        n_year = cur.execute("SELECT COUNT(*) FROM survey_year").fetchone()[0]
        n_res = cur.execute("SELECT COUNT(*) FROM survey_result").fetchone()[0]

        print("✅ Section 2.2 done.")
        print(f"   - DB: {DB_PATH.resolve()}")
        print(
            f"   - Tables -> university: {n_uni}, programme: {n_prog}, survey_year: {n_year}, survey_result: {n_res}"
        )
        print("   - View  -> v_result (ready for queries)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
