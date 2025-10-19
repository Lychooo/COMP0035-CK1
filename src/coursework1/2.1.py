#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Section 2.1 – Database design (ERD) + load data (SQLAlchemy, SQLite)

USAGE:
  python 2.1.py --csv "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv" --db "results.db"
  python 2.1.py --csv data.csv --db results.db --reset

What this script does
---------------------
1) Builds a 3NF relational schema (SQLite) with strong constraints:
   - Tables: university, programme, survey_year, survey_result
   - PK/FK with ON DELETE CASCADE
   - CHECK constraints for business rules (rates in 0..100, non-negative salaries, gross >= basic, etc.)
   - UNIQUE constraints and indexes
2) Loads the CSV into the schema (idempotent upserts for dimension tables).
3) Audits rows where gross < basic; writes an audit CSV; then clamps gross to basic.
4) Exports a Mermaid ER diagram snippet to 'erd.md' for your report.

IMPORTANT (SQLite FKs):
-----------------------
SQLite disables foreign key enforcement by default. We ENABLE it here via an
SQLAlchemy 'connect' event hook (PRAGMA foreign_keys = ON). This is the critical fix.

"""

from __future__ import annotations
import argparse
import math
import os
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import pandas as pd
from sqlalchemy import (
    create_engine, event, Integer, String, Numeric, CheckConstraint, ForeignKey,
    UniqueConstraint, Index
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, Session, sessionmaker
)


# ----------------------------
# SQLAlchemy base & utilities
# ----------------------------
class Base(DeclarativeBase):
    pass


def build_engine(db_url: str):
    """
    Create SQLAlchemy engine and ensure PRAGMA foreign_keys = ON for SQLite.
    This is the minimal, surgical 'fix' you needed.
    """
    engine = create_engine(db_url, echo=False, future=True)

    if engine.url.get_backend_name() == "sqlite":
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.close()

    return engine


# ----------------------------
# 3NF Schema (SQLite)
# ----------------------------
class University(Base):
    __tablename__ = "university"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    # Optionals for reporting/joins; adjust to your dataset if needed
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    region: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    programmes: Mapped[list["Programme"]] = relationship(
        back_populates="university", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<University id={self.id} name={self.name!r}>"


class Programme(Base):
    __tablename__ = "programme"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    university_id: Mapped[int] = mapped_column(
        ForeignKey("university.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    code: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    __table_args__ = (
        UniqueConstraint("university_id", "name", name="uq_programme_uni_name"),
        Index("ix_programme_name", "name"),
    )

    university: Mapped["University"] = relationship(back_populates="programmes")
    results: Mapped[list["SurveyResult"]] = relationship(
        back_populates="programme", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Programme id={self.id} uni={self.university_id} name={self.name!r}>"


class SurveyYear(Base):
    __tablename__ = "survey_year"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)

    __table_args__ = (
        CheckConstraint("year BETWEEN 2000 AND 2100", name="ck_year_range"),
    )

    results: Mapped[list["SurveyResult"]] = relationship(
        back_populates="year", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<SurveyYear id={self.id} year={self.year}>"


class SurveyResult(Base):
    __tablename__ = "survey_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    programme_id: Mapped[int] = mapped_column(
        ForeignKey("programme.id", ondelete="CASCADE"), nullable=False, index=True
    )
    year_id: Mapped[int] = mapped_column(
        ForeignKey("survey_year.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Employment rates (percentage 0..100)
    employment_overall: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    employment_ft_perm: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))

    # Salaries (non-negative, in the same unit as dataset; medians)
    basic_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    gross_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    __table_args__ = (
        UniqueConstraint("programme_id", "year_id", name="uq_prog_year"),
        # Business logic safeguards.
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

    def __repr__(self) -> str:
        return f"<SurveyResult id={self.id} prog={self.programme_id} year={self.year_id}>"


# ----------------------------
# Data mapping helpers
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
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _clean_year(y) -> Optional[int]:
    try:
        yy = int(y)
        if 2000 <= yy <= 2100:
            return yy
        return None
    except Exception:
        return None


def parse_dataframe(df: pd.DataFrame) -> list[Row]:
    """
    Map your raw dataframe columns to the unified Row model.
    Adjust column names below to your CSV headers if needed.
    """
    # ---- EDIT ME if your column names differ ----
    # Example expected columns (rename here to match your CSV):
    rename_map = {
        "university": "university",
        "programme": "programme",
        "year": "year",
        "employment_overall": "employment_overall",
        "employment_fulltime_permanent": "employment_ft_perm",
        "basic_monthly_median": "basic_monthly_median",
        "gross_monthly_median": "gross_monthly_median",
    }
    # If your CSV uses different headers (e.g., 'school', 'degree', 'overall_employment_rate', etc.),
    # do: df = df.rename(columns={"school":"university", "degree":"programme", ...})

    df2 = df.rename(columns=rename_map).copy()

    required = [
        "university", "programme", "year",
        "employment_overall", "employment_ft_perm",
        "basic_monthly_median", "gross_monthly_median"
    ]
    missing = [c for c in required if c not in df2.columns]
    if missing:
        raise ValueError(f"Missing required columns after rename: {missing}")

    rows: list[Row] = []
    for _, r in df2.iterrows():
        yr = _clean_year(r["year"])
        if yr is None:
            continue
        rows.append(
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
    return rows


# ----------------------------
# Loaders (idempotent)
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
    obj = (
        sess.query(Programme)
        .filter_by(university_id=uni_id, name=name)
        .one_or_none()
    )
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


def upsert_result(sess: Session, prog_id: int, year_id: int, r: Row):
    obj = (
        sess.query(SurveyResult)
        .filter_by(programme_id=prog_id, year_id=year_id)
        .one_or_none()
    )
    if obj is None:
        obj = SurveyResult(
            programme_id=prog_id, year_id=year_id,
            employment_overall=r.emp_overall,
            employment_ft_perm=r.emp_ft_perm,
            basic_monthly_median=r.basic_median,
            gross_monthly_median=r.gross_median,
        )
        sess.add(obj)
    else:
        # Upsert: refresh values
        obj.employment_overall = r.emp_overall
        obj.employment_ft_perm = r.emp_ft_perm
        obj.basic_monthly_median = r.basic_median
        obj.gross_monthly_median = r.gross_median


# ----------------------------
# Audit & ERD export
# ----------------------------
def audit_and_fix_gross_basic(sess: Session, out_csv: str) -> int:
    """
    Export rows where gross < basic. Then clamp gross = basic (soft fix).
    Returns number of audited rows.
    """
    q = (
        sess.query(SurveyResult)
        .filter(
            SurveyResult.gross_monthly_median.isnot(None),
            SurveyResult.basic_monthly_median.isnot(None),
            SurveyResult.gross_monthly_median < SurveyResult.basic_monthly_median,
        )
        .all()
    )
    if not q:
        return 0

    recs = []
    for x in q:
        recs.append({
            "result_id": x.id,
            "programme_id": x.programme_id,
            "year_id": x.year_id,
            "gross": float(x.gross_monthly_median),
            "basic": float(x.basic_monthly_median),
        })
        # Clamp (soft fix)
        x.gross_monthly_median = x.basic_monthly_median

    pd.DataFrame(recs).to_csv(out_csv, index=False)
    return len(q)


def export_mermaid_erd(path: str = "erd.md"):
    """
    Very lightweight Mermaid ER diagram for the report.
    """
    mermaid = """```mermaid
erDiagram
    UNIVERSITY ||--o{ PROGRAMME : has
    PROGRAMME ||--o{ SURVEY_RESULT : has
    SURVEY_YEAR ||--o{ SURVEY_RESULT : has

    UNIVERSITY {
        integer id PK
        string  name  "UNIQUE, NOT NULL"
        string  country
        string  region
    }

    PROGRAMME {
        integer id PK
        integer university_id FK "-> UNIVERSITY.id (ON DELETE CASCADE)"
        string  name  "NOT NULL"
        string  code
    }

    SURVEY_YEAR {
        integer id PK
        integer year "UNIQUE, CHECK 2000..2100"
    }

    SURVEY_RESULT {
        integer id PK
        integer programme_id FK "-> PROGRAMME.id (ON DELETE CASCADE)"
        integer year_id FK "-> SURVEY_YEAR.id (ON DELETE CASCADE)"
        numeric employment_overall  "0..100"
        numeric employment_ft_perm  "0..100"
        numeric basic_monthly_median ">= 0"
        numeric gross_monthly_median ">= basic_monthly_median"
    }
```"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


# ----------------------------
# Main pipeline
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Input CSV file")
    ap.add_argument("--db", default="results.db", help="SQLite DB file path")
    ap.add_argument("--reset", action="store_true", help="Drop & recreate schema")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        raise FileNotFoundError(args.csv)

    engine = build_engine(f"sqlite:///{args.db}")
    SessionLocal = sessionmaker(bind=engine, future=True)

    if args.reset:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # Load CSV
    df = pd.read_csv(args.csv)
    rows = parse_dataframe(df)
    print(f"[INFO] Parsed valid rows: {len(rows)}")

    inserted_u = inserted_p = inserted_y = 0
    with SessionLocal.begin() as sess:
        # Dimension upserts + fact upserts
        cache_uni: Dict[str, int] = {}
        cache_prog: Dict[Tuple[int, str], int] = {}
        cache_year: Dict[int, int] = {}

        for r in rows:
            # University
            if r.university not in cache_uni:
                u = get_or_create_university(sess, r.university)
                if u.id is None:
                    sess.flush()
                cache_uni[r.university] = u.id
                inserted_u += 1 if sess.get(University, u.id) else 0
            u_id = cache_uni[r.university]

            # Programme
            key = (u_id, r.programme)
            if key not in cache_prog:
                p = get_or_create_programme(sess, u_id, r.programme)
                cache_prog[key] = p.id
                inserted_p += 1 if sess.get(Programme, p.id) else 0
            p_id = cache_prog[key]

            # Year
            if r.year not in cache_year:
                y = get_or_create_year(sess, r.year)
                cache_year[r.year] = y.id
                inserted_y += 1 if sess.get(SurveyYear, y.id) else 0
            y_id = cache_year[r.year]

            # Fact
            upsert_result(sess, p_id, y_id, r)

        # Audit & clamp
        n_bad = audit_and_fix_gross_basic(sess, out_csv="audit_gross_lt_basic.csv")
        print(f"[AUDIT] gross < basic rows: {n_bad} (clamped; details -> audit_gross_lt_basic.csv)")

    # Stats
    with SessionLocal() as sess:
        n_u = sess.query(University).count()
        n_p = sess.query(Programme).count()
        n_y = sess.query(SurveyYear).count()
        n_r = sess.query(SurveyResult).count()
    print(f"[COUNTS] university={n_u}, programme={n_p}, year={n_y}, results={n_r}")

    # Export ERD
    export_mermaid_erd("erd.md")
    print("[OK] Mermaid ERD exported -> erd.md")
    print("[OK] Done.")


if __name__ == "__main__":
    main()

