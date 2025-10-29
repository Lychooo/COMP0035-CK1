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


# =========================
# SQLAlchemy base & engine
# =========================
class Base(DeclarativeBase):
    pass


def build_engine(db_url: str):
    engine = create_engine(db_url, echo=False, future=True)
    if engine.url.get_backend_name() == "sqlite":

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.close()

    return engine


# =========================
# 目标 3NF 模式（与你“dim_university → dim_school → dim_degree → fact_employment”一致）
# =========================
class DimUniversity(Base):
    __tablename__ = "dim_university"

    university_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    university_name: Mapped[str] = mapped_column(
        String(200), unique=True, nullable=False
    )
    country: Mapped[Optional[str]] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(100))

    schools: Mapped[List["DimSchool"]] = relationship(
        back_populates="university", cascade="all, delete-orphan"
    )


class DimSchool(Base):
    __tablename__ = "dim_school"

    school_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    school_name: Mapped[str] = mapped_column(String(200), nullable=False)
    university_id: Mapped[int] = mapped_column(
        ForeignKey("dim_university.university_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # 去重：同一所大学中 school_name 唯一
    __table_args__ = (
        UniqueConstraint("university_id", "school_name", name="uq_school_uni_name"),
        Index("ix_school_name", "school_name"),
    )

    university: Mapped["DimUniversity"] = relationship(back_populates="schools")
    degrees: Mapped[List["DimDegree"]] = relationship(
        back_populates="school", cascade="all, delete-orphan"
    )


class DimDegree(Base):
    __tablename__ = "dim_degree"

    degree_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    degree_name: Mapped[str] = mapped_column(String(200), nullable=False)
    school_id: Mapped[int] = mapped_column(
        ForeignKey("dim_school.school_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # 去重：同一 school 中 degree_name 唯一
    __table_args__ = (
        UniqueConstraint("school_id", "degree_name", name="uq_degree_school_name"),
        Index("ix_degree_name", "degree_name"),
    )

    school: Mapped["DimSchool"] = relationship(back_populates="degrees")
    facts: Mapped[List["FactEmployment"]] = relationship(
        back_populates="degree", cascade="all, delete-orphan"
    )


class SurveyYear(Base):
    __tablename__ = "survey_year"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)

    __table_args__ = (
        CheckConstraint("year BETWEEN 2000 AND 2100", name="ck_year_range"),
    )

    facts: Mapped[List["FactEmployment"]] = relationship(
        back_populates="year", cascade="all, delete-orphan"
    )


class FactEmployment(Base):
    __tablename__ = "fact_employment"

    record_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    degree_id: Mapped[int] = mapped_column(
        ForeignKey("dim_degree.degree_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    year_id: Mapped[int] = mapped_column(
        ForeignKey("survey_year.id", ondelete="CASCADE"), nullable=False, index=True
    )

    employment_rate_overall: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    employment_rate_ft_perm: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    basic_monthly_mean: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    basic_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    gross_monthly_mean: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    gross_monthly_median: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    __table_args__ = (
        UniqueConstraint("degree_id", "year_id", name="uq_degree_year"),
        CheckConstraint(
            "(employment_rate_overall IS NULL) OR (employment_rate_overall BETWEEN 0 AND 100)",
            name="ck_emp_overall_pct",
        ),
        CheckConstraint(
            "(employment_rate_ft_perm IS NULL) OR (employment_rate_ft_perm BETWEEN 0 AND 100)",
            name="ck_emp_ft_perm_pct",
        ),
        CheckConstraint(
            "(basic_monthly_mean IS NULL) OR (basic_monthly_mean >= 0)",
            name="ck_basic_mean_nonneg",
        ),
        CheckConstraint(
            "(basic_monthly_median IS NULL) OR (basic_monthly_median >= 0)",
            name="ck_basic_median_nonneg",
        ),
        CheckConstraint(
            "(gross_monthly_mean IS NULL) OR (gross_monthly_mean >= 0)",
            name="ck_gross_mean_nonneg",
        ),
        CheckConstraint(
            "(gross_monthly_median IS NULL) OR (gross_monthly_median >= 0)",
            name="ck_gross_median_nonneg",
        ),
        CheckConstraint(
            "(gross_monthly_median IS NULL) OR "
            "(basic_monthly_median IS NULL) OR "
            "(gross_monthly_median >= basic_monthly_median)",
            name="ck_gross_ge_basic_median",
        ),
        Index("ix_fact_emp_overall", "employment_rate_overall"),
        Index("ix_fact_basic_median", "basic_monthly_median"),
        Index("ix_fact_gross_median", "gross_monthly_median"),
    )

    degree: Mapped["DimDegree"] = relationship(back_populates="facts")
    year: Mapped["SurveyYear"] = relationship(back_populates="facts")


# =========================
# Data model from CSV
# =========================
@dataclass
class Row:
    university: str
    school: str
    degree: str
    year: int
    emp_overall: Optional[float]
    emp_ft_perm: Optional[float]
    basic_mean: Optional[float]
    basic_median: Optional[float]
    gross_mean: Optional[float]
    gross_median: Optional[float]


def _as_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        v = float(str(x).replace(",", "").strip())
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _as_int_year(y) -> Optional[int]:
    try:
        val = int(float(str(y).replace(",", "").strip()))
        return val if 2000 <= val <= 2100 else None
    except Exception:
        return None


def parse_dataframe(df: pd.DataFrame) -> List[Row]:
    """
    Map raw GES CSV to Row (优先使用 school/degree；若无 school 则 school='(N/A)')。
    """
    rename = {
        "university": "university",
        "school": "school",
        "degree": "degree",
        "year": "year",
        "employment_rate_overall": "employment_rate_overall",
        "employment_rate_ft_perm": "employment_rate_ft_perm",
        "basic_monthly_mean": "basic_monthly_mean",
        "basic_monthly_median": "basic_monthly_median",
        "gross_monthly_mean": "gross_monthly_mean",
        "gross_monthly_median": "gross_monthly_median",
    }
    df2 = df.rename(columns={k: v for k, v in rename.items() if k in df.columns}).copy()

    # 容错：如果没有 school/degree，但有 programme_title，就拆到 degree；school 置 '(N/A)'
    if "degree" not in df2.columns and "programme_title" in df2.columns:
        df2["degree"] = df2["programme_title"].astype(str)
    if "school" not in df2.columns:
        df2["school"] = "(N/A)"

    required = [
        "university",
        "school",
        "degree",
        "year",
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
    ]
    missing = [c for c in required if c not in df2.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out: List[Row] = []
    for _, r in df2.iterrows():
        yr = _as_int_year(r["year"])
        if yr is None:
            continue
        out.append(
            Row(
                university=str(r["university"]).strip(),
                school=str(r["school"]).strip(),
                degree=str(r["degree"]).strip(),
                year=yr,
                emp_overall=_as_float(r["employment_rate_overall"]),
                emp_ft_perm=_as_float(r["employment_rate_ft_perm"]),
                basic_mean=_as_float(r["basic_monthly_mean"]),
                basic_median=_as_float(r["basic_monthly_median"]),
                gross_mean=_as_float(r["gross_monthly_mean"]),
                gross_median=_as_float(r["gross_monthly_median"]),
            )
        )
    return out


# =========================
# Upsert helpers
# =========================
def get_or_create_university(sess: Session, name: str) -> DimUniversity:
    obj = sess.query(DimUniversity).filter_by(university_name=name).one_or_none()
    if obj:
        return obj
    obj = DimUniversity(university_name=name)
    sess.add(obj)
    sess.flush()
    return obj


def get_or_create_school(sess: Session, uni_id: int, school_name: str) -> DimSchool:
    obj = (
        sess.query(DimSchool)
        .filter_by(university_id=uni_id, school_name=school_name)
        .one_or_none()
    )
    if obj:
        return obj
    obj = DimSchool(university_id=uni_id, school_name=school_name)
    sess.add(obj)
    sess.flush()
    return obj


def get_or_create_degree(sess: Session, school_id: int, degree_name: str) -> DimDegree:
    obj = (
        sess.query(DimDegree)
        .filter_by(school_id=school_id, degree_name=degree_name)
        .one_or_none()
    )
    if obj:
        return obj
    obj = DimDegree(school_id=school_id, degree_name=degree_name)
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


def upsert_fact(sess: Session, degree_id: int, year_id: int, r: Row) -> None:
    obj = (
        sess.query(FactEmployment)
        .filter_by(degree_id=degree_id, year_id=year_id)
        .one_or_none()
    )
    if obj is None:
        obj = FactEmployment(
            degree_id=degree_id,
            year_id=year_id,
            employment_rate_overall=r.emp_overall,
            employment_rate_ft_perm=r.emp_ft_perm,
            basic_monthly_mean=r.basic_mean,
            basic_monthly_median=r.basic_median,
            gross_monthly_mean=r.gross_mean,
            gross_monthly_median=r.gross_median,
        )
        sess.add(obj)
    else:
        obj.employment_rate_overall = r.emp_overall
        obj.employment_rate_ft_perm = r.emp_ft_perm
        obj.basic_monthly_mean = r.basic_mean
        obj.basic_monthly_median = r.basic_median
        obj.gross_monthly_mean = r.gross_mean
        obj.gross_monthly_median = r.gross_median


# =========================
# ERD (Mermaid)
# =========================
def export_mermaid_erd(path: str = "erd.md") -> None:
    mermaid = """```mermaid
erDiagram
  DIM_UNIVERSITY  ||--o{ DIM_SCHOOL     : has
  DIM_SCHOOL     ||--o{ DIM_DEGREE     : has
  DIM_DEGREE     ||--o{ FACT_EMPLOYMENT: has
  SURVEY_YEAR    ||--o{ FACT_EMPLOYMENT: has

  DIM_UNIVERSITY {
    int    university_id  PK
    string university_name
    string country
    string region
  }

  DIM_SCHOOL {
    int    school_id      PK
    int    university_id  FK
    string school_name
  }

  DIM_DEGREE {
    int    degree_id      PK
    int    school_id      FK
    string degree_name
  }

  SURVEY_YEAR {
    int    id             PK
    int    year
  }

  FACT_EMPLOYMENT {
    int    record_id          PK
    int    degree_id          FK
    int    year_id            FK
    float  employment_rate_overall
    float  employment_rate_ft_perm
    float  basic_monthly_mean
    float  basic_monthly_median
    float  gross_monthly_mean
    float  gross_monthly_median
  }
```"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


# =========================
# Main
# =========================
def main() -> None:
    ap = argparse.ArgumentParser(description="Section 2.1 – 3NF DB build & load")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    default_csv = os.path.join(
        repo_root, "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv"
    )
    default_db = os.path.join(repo_root, "ges.db")

    ap.add_argument("--csv", default=default_csv, help="Input CSV")
    ap.add_argument("--db", default=default_db, help="SQLite DB file")
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
        cache_school: Dict[Tuple[int, str], int] = {}
        cache_degree: Dict[Tuple[int, str], int] = {}
        cache_year: Dict[int, int] = {}

        for r in rows:
            # dims
            if r.university not in cache_uni:
                u = get_or_create_university(sess, r.university)
                cache_uni[r.university] = u.university_id
            uni_id = cache_uni[r.university]

            s_key = (uni_id, r.school)
            if s_key not in cache_school:
                s = get_or_create_school(sess, uni_id, r.school)
                cache_school[s_key] = s.school_id
            school_id = cache_school[s_key]

            d_key = (school_id, r.degree)
            if d_key not in cache_degree:
                d = get_or_create_degree(sess, school_id, r.degree)
                cache_degree[d_key] = d.degree_id
            degree_id = cache_degree[d_key]

            if r.year not in cache_year:
                y = get_or_create_year(sess, r.year)
                cache_year[r.year] = y.id
            year_id = cache_year[r.year]

            # --- PRE-INSERT AUDIT & CLAMP (median 口径) ---
            bm = r.basic_median
            gm = r.gross_median
            if (bm is not None) and (gm is not None) and (gm < bm):
                audit_rows.append(
                    {
                        "university": r.university,
                        "school": r.school,
                        "degree": r.degree,
                        "year": r.year,
                        "gross_before": gm,
                        "basic": bm,
                    }
                )
                r.gross_median = bm  # clamp
            # ---------------------------------------------

            upsert_fact(sess, degree_id, year_id, r)

    if audit_rows:
        pd.DataFrame(audit_rows).to_csv("audit_gross_lt_basic.csv", index=False)
        print(
            f"[AUDIT] gross < basic rows: {len(audit_rows)} -> audit_gross_lt_basic.csv"
        )
    else:
        print("[AUDIT] gross < basic rows: 0")

    with SessionLocal() as sess:
        n_u = sess.query(DimUniversity).count()
        n_s = sess.query(DimSchool).count()
        n_d = sess.query(DimDegree).count()
        n_y = sess.query(SurveyYear).count()
        n_f = sess.query(FactEmployment).count()
    print(
        f"[COUNTS] university={n_u}, school={n_s}, degree={n_d}, year={n_y}, facts={n_f}"
    )

    export_mermaid_erd("erd.md")
    print("[OK] Mermaid ERD exported -> erd.md")
    print("[OK] Done.")


if __name__ == "__main__":
    main()
