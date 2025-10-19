#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Section 2.1 – Database design (ERD) + load data (SQLAlchemy, SQLite)

USAGE (simple):
  python 2.1.py

USAGE (custom):
  python 2.1.py --csv "path/to/your.csv" --db "ges.db" --reset

What this script does
---------------------
1) Builds a 3NF relational schema (SQLite) with strong constraints:
   - Tables: university, programme, survey_year, survey_result
   - PK/FK with ON DELETE CASCADE (SQLite foreign_keys=ON)
   - CHECK constraints (rates in 0..100, non-negative salaries, gross >= basic)
   - UNIQUE constraints and indexes
2) Loads the CSV into the schema (idempotent upserts).
3) BEFORE INSERT: audit rows with gross < basic, clamp gross=basic, then insert.
   -> writes audit_gross_lt_basic.csv with original values.
4) Exports a Mermaid ER diagram to erd.md.
"""

from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)


# ----------------------------
# SQLAlchemy base & engine
# ----------------------------
class Base(DeclarativeBase):
    pass


def build_engine(db_url: str):
    """Create engine and enable PRAGMA foreign_keys=ON for SQLite."""
    engine = create_engine(db_url, echo=False, future=True)
    if engine.url.get_backend_name() == "sqlite":

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.close()

    return engine


# ----------------------------
# 3NF schema
# ----------------------------
class University(Base):
    __tablename__ = "university"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    country: Mapped[Optional[str]] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(100))

    programmes: Mapped[List["Programme"]] = relationship(
        back_populates="university", cascade="all, delete-orphan"
    )


class Programme(Base):
    __tablename__ = "programme"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    university_id: Mapped[int] = mapped_column(
        ForeignKey("university.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    code: Mapped[Optional[str]] = mapped_column(String(80))

    __table_args__ = (
        UniqueConstraint("university_id", "name", name="uq_programme_uni_name"),
        Index("ix_programme_name", "name"),
    )

    university: Mapped["University"] = relationship(back_populates="programmes")
    results: Mapped[List["SurveyResult"]] = relationship(
        back_populates="programme", cascade="all, delete-orphan"
    )


class SurveyYear(Base):
    __tablename__ = "survey_year"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)

    __table_args__ = (CheckConstraint("year BETWEEN 2000 AND 2100", name="ck_year_range"),)

    results: Mapped[List["SurveyResult"]] = relationship(
        back_populates="year", cascade="all, delete-orphan"
    )


class SurveyResult(Base):
    __tablename__ = "survey_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    programme_id: Mapped[int] = mapped_column(
        ForeignKey("programme.id", ondelete="CASCADE"), nullable=False, index=True
    )
    year_id: Mapped[int] = mapped_column(
        ForeignKey("survey_year.id", ondelete="CASCADE"), nullable=False, index=True
    )

    employment_overall: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    employment_ft_perm: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    basic_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    gross_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    __table_args__ = (
        UniqueConstraint("programme_id", "year_id", name="uq_prog_year"),
        CheckConstraint(
            "(employment_overall IS NULL) OR (employment_overall BETWEEN 0 AND 100)",
            name="ck_emp_overall_pct",
        ),
        CheckConstraint(
            "(employment_ft_perm IS NULL) OR (employment_ft_perm BETWEEN 0 AND 100)",
            name="ck_emp_ft_perm_pct",
        ),
        CheckConstraint(
            "(basic_monthly_median IS NULL) OR (basic_monthly_median >= 0)",
            name="ck_basic_nonneg",
        ),
        CheckConstraint(
            "(gross_monthly_median IS NULL) OR (gross_monthly_median >= 0)",
            name="ck_gross_nonneg",
        ),
        CheckConstraint(
            "(gross_monthly_median IS NULL) OR "
            "(basic_monthly_median IS NULL) OR "
            "(gross_monthly_median >= basic_monthly_median)",
            name="ck_gross_ge_basic",
        ),
        Index("ix_result_emp_overall", "employment_overall"),
        Index("ix_result_basic", "basic_monthly_median"),
        Index("ix_result_gross", "gross_monthly_median"),
    )

    programme: Mapped["Programme"] = relationship(back_populates="results")
    year: Mapped["SurveyYear"] = relationship(back_populates="results")


# ----------------------------
# Data model and mapping
# ----------------------------
@dataclass
class Row:
    university: str
    programme: str
    year: int
    emp_overall: Optional[float]
    emp_ft_perm: Optional[float]
    basic_median: Optional[float]
    gross_median: Optional[float]


def _as_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _clean_year(y) -> Optional[int]:
    try:
        yy = int(y)
        return yy if 2000 <= yy <= 2100 else None
    except Exception:
        return None


def parse_dataframe(df: pd.DataFrame) -> List[Row]:
    """
    Map raw GES CSV columns to the unified Row model.

    programme:
      - If both 'school' and 'degree' exist, use "school - degree".
      - Otherwise fall back to 'degree' only (typical in this dataset).
    """
    rename_map = {
        "university": "university",
        "year": "year",
        "employment_rate_overall": "employment_overall",
        "employment_rate_ft_perm": "employment_ft_perm",
        "basic_monthly_median": "basic_monthly_median",
        "gross_monthly_median": "gross_monthly_median",
    }
    df2 = df.rename(columns=rename_map).copy()

    # Build 'programme'
    if "degree" in df2.columns and "school" in df2.columns:
        df2["programme"] = (
            df2["school"].astype(str).str.strip()
            + " - "
            + df2["degree"].astype(str).str.strip()
        ).str.strip(" -")
    elif "degree" in df2.columns:
        df2["programme"] = df2["degree"].astype(str).str.strip()
    else:
        df2["programme"] = df2.get("programme", df2.get("university", "")).astype(str)

    required = [
        "university",
        "programme",
        "year",
        "employment_overall",
        "employment_ft_perm",
        "basic_monthly_median",
        "gross_monthly_median",
    ]
    missing = [c for c in required if c not in df2.columns]
    if missing:
        raise ValueError(f"Missing required columns after rename: {missing}")

    out: List[Row] = []
    for _, r in df2.iterrows():
        yr = _clean_year(r["year"])
        if yr is None:
            continue
        out.append(
            Row(
                university=str(r["university"]).strip(),
                programme=str(r["programme"]).strip(),
                year=yr,
                emp_overall=_as_float(r["employment_overall"]),
                emp_ft_perm=_as_float(r["employment_ft_perm"]),
                basic_median=_as_float(r["basic_monthly_median"]),
                gross_median=_as_float(r["gross_monthly_median"]),
            )
        )
    return out


# ----------------------------
# Upserts
# ----------------------------
def get_or_create_university(sess: Session, name: str) -> University:
    obj = sess.query(University).filter_by(name=name).one_or_none()
    if obj:
        return obj
    obj = University(name=name)
    sess.add(obj)
    sess.flush()
    return obj


def get_or_create_programme(sess: Session, uni_id: int, name: str) -> Programme:
    obj = sess.query(Programme).filter_by(university_id=uni_id, name=name).one_or_none()
    if obj:
        return obj
    obj = Programme(university_id=uni_id, name=name)
    sess.add(obj)
    sess.flush()
    return obj


def get_or_create_year(sess: Session, year: int) -> SurveyYear:
    obj = sess.query(SurveyYear).filter_by(year=year).one_or_none()
    if obj:
        return obj
    obj = SurveyYear(year=year)
    sess.add(obj)
    sess.flush()
    return obj


def upsert_result(sess: Session, prog_id: int, year_id: int, r: Row) -> None:
    obj = (
        sess.query(SurveyResult)
        .filter_by(programme_id=prog_id, year_id=year_id)
        .one_or_none()
    )
    if obj is None:
        obj = SurveyResult(
            programme_id=prog_id,
            year_id=year_id,
            employment_overall=r.emp_overall,
            employment_ft_perm=r.emp_ft_perm,
            basic_monthly_median=r.basic_median,
            gross_monthly_median=r.gross_median,
        )
        sess.add(obj)
    else:
        obj.employment_overall = r.emp_overall
        obj.employment_ft_perm = r.emp_ft_perm
        obj.basic_monthly_median = r.basic_median
        obj.gross_monthly_median = r.gross_median


# ----------------------------
# ERD export (Mermaid, parse-safe)
# ----------------------------
def export_mermaid_erd(path: str = "erd.md") -> None:
    # Mermaid erDiagram 仅支持：类型 名称 [PK|FK]
    # 关系、约束等用关系线或在报告里说明，不要写在字段行里。
    mermaid = """```mermaid
erDiagram
  UNIVERSITY  ||--o{ PROGRAMME     : has
  PROGRAMME   ||--o{ SURVEY_RESULT : has
  SURVEY_YEAR ||--o{ SURVEY_RESULT : has

  UNIVERSITY {
    int    id      PK
    string name
    string country
    string region
  }

  PROGRAMME {
    int    id           PK
    int    university_id FK
    string name
    string code
  }

  SURVEY_YEAR {
    int    id    PK
    int    year
  }

  SURVEY_RESULT {
    int    id                 PK
    int    programme_id       FK
    int    year_id            FK
    float  employment_overall
    float  employment_ft_perm
    float  basic_monthly_median
    float  gross_monthly_median
  }
```"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()

    # Default paths relative to repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    default_csv = os.path.join(
        repo_root, "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv"
    )
    default_db = os.path.join(repo_root, "ges.db")

    ap.add_argument(
        "--csv",
        required=False,
        default=default_csv,
        help=f"Input CSV file (default: {default_csv})",
    )
    ap.add_argument(
        "--db",
        required=False,
        default=default_db,
        help=f"SQLite DB file path (default: {default_db})",
    )
    ap.add_argument("--reset", action="store_true", help="Drop & recreate schema")

    args = ap.parse_args()

    if not os.path.exists(args.csv):
        raise FileNotFoundError(args.csv)

    engine = build_engine(f"sqlite:///{args.db}")
    SessionLocal = sessionmaker(bind=engine, future=True)

    if args.reset:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    df = pd.read_csv(args.csv)
    rows = parse_dataframe(df)
    print(f"[INFO] Parsed valid rows: {len(rows)}")

    audit_rows: List[dict] = []

    with SessionLocal.begin() as sess:
        cache_uni: Dict[str, int] = {}
        cache_prog: Dict[Tuple[int, str], int] = {}
        cache_year: Dict[int, int] = {}

        for r in rows:
            # dimensions
            if r.university not in cache_uni:
                u = get_or_create_university(sess, r.university)
                cache_uni[r.university] = u.id
            u_id = cache_uni[r.university]

            key = (u_id, r.programme)
            if key not in cache_prog:
                p = get_or_create_programme(sess, u_id, r.programme)
                cache_prog[key] = p.id
            p_id = cache_prog[key]

            if r.year not in cache_year:
                y = get_or_create_year(sess, r.year)
                cache_year[r.year] = y.id
            y_id = cache_year[r.year]

            # --- PRE-INSERT AUDIT & CLAMP ---
            bm = r.basic_median
            gm = r.gross_median
            if (bm is not None) and (gm is not None) and (gm < bm):
                audit_rows.append(
                    {
                        "university": r.university,
                        "programme": r.programme,
                        "year": r.year,
                        "gross_before": gm,
                        "basic": bm,
                    }
                )
                r = Row(
                    university=r.university,
                    programme=r.programme,
                    year=r.year,
                    emp_overall=r.emp_overall,
                    emp_ft_perm=r.emp_ft_perm,
                    basic_median=bm,
                    gross_median=bm,  # clamp
                )
            # --------------------------------

            upsert_result(sess, p_id, y_id, r)

    # audit export
    if audit_rows:
        pd.DataFrame(audit_rows).to_csv("audit_gross_lt_basic.csv", index=False)
        print(f"[AUDIT] gross < basic rows: {len(audit_rows)} -> audit_gross_lt_basic.csv")
    else:
        print("[AUDIT] gross < basic rows: 0")

    # counts
    with SessionLocal() as sess:
        n_u = sess.query(University).count()
        n_p = sess.query(Programme).count()
        n_y = sess.query(SurveyYear).count()
        n_r = sess.query(SurveyResult).count()
    print(f"[COUNTS] university={n_u}, programme={n_p}, year={n_y}, results={n_r}")

    export_mermaid_erd("erd.md")
    print("[OK] Mermaid ERD exported -> erd.md")
    print("[OK] Done.")


if __name__ == "__main__":
    main()
