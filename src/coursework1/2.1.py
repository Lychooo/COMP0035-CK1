from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import (
    create_engine, String, Integer, SmallInteger, Numeric, ForeignKey,
    UniqueConstraint, CheckConstraint, Index
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session

# ===================== 配置 =====================
DEFAULT_CSV = r"C:\Users\30769\Desktop\comp0035-cw-Lychooo\7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv"
DEFAULT_DB_URL = "sqlite:///ges.db"  # 零配置 SQLite
OUT_DIR = Path("prep_output")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# gross < basic 的处理策略： "clamp"（把 gross 提升到 basic） 或 "drop"（丢弃这些记录）
GROSS_BASIC_STRATEGY = "clamp"

# ===================== ORM 基类 =====================
class Base(DeclarativeBase):
    pass

# ===================== 3NF 表模型 =====================
class University(Base):
    __tablename__ = "university"
    university_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name:         Mapped[str]  = mapped_column(String(200), unique=True, nullable=False)
    short_name:   Mapped[Optional[str]] = mapped_column(String(50), unique=True, nullable=True)
    programmes:   Mapped[list["Programme"]] = relationship(back_populates="university", cascade="all, delete-orphan")

class Programme(Base):
    __tablename__ = "programme"
    programme_id:  Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    university_id: Mapped[int] = mapped_column(ForeignKey("university.university_id", ondelete="CASCADE"), nullable=False)
    name:          Mapped[str] = mapped_column(String(300), nullable=False)

    university: Mapped["University"] = relationship(back_populates="programmes")
    results:    Mapped[list["SurveyResult"]] = relationship(back_populates="programme", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("university_id", "name", name="uq_programme_university_name"),
        Index("ix_programme_university", "university_id"),
    )

class SurveyYear(Base):
    __tablename__ = "survey_year"
    year_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year:    Mapped[int] = mapped_column(SmallInteger, nullable=False, unique=True)
    results: Mapped[list["SurveyResult"]] = relationship(back_populates="year")

    __table_args__ = (
        CheckConstraint("year BETWEEN 2000 AND 2100", name="ck_year_range"),
        Index("ix_year_year", "year"),
    )

class SurveyResult(Base):
    __tablename__ = "survey_result"
    result_id:    Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    programme_id: Mapped[int] = mapped_column(ForeignKey("programme.programme_id", ondelete="CASCADE"), nullable=False)
    year_id:      Mapped[int] = mapped_column(ForeignKey("survey_year.year_id", ondelete="CASCADE"), nullable=False)

    employment_rate_overall: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    employment_rate_ft_perm: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    basic_monthly_mean:      Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    basic_monthly_median:    Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    gross_monthly_mean:      Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    gross_monthly_median:    Mapped[Optional[float]] = mapped_column(Numeric(10, 2))

    programme: Mapped["Programme"] = relationship(back_populates="results")
    year:      Mapped["SurveyYear"] = relationship(back_populates="results")

    __table_args__ = (
        UniqueConstraint("programme_id", "year_id", name="uq_programme_year"),
        # 就业率 0..100
        CheckConstraint("(employment_rate_overall IS NULL) OR (employment_rate_overall BETWEEN 0 AND 100)", name="ck_emp_overall"),
        CheckConstraint("(employment_rate_ft_perm IS NULL) OR (employment_rate_ft_perm BETWEEN 0 AND 100)", name="ck_emp_ft"),
        # 逻辑：ft_perm ≤ overall
        CheckConstraint("""
            (employment_rate_overall IS NULL) OR (employment_rate_ft_perm IS NULL)
            OR (employment_rate_ft_perm <= employment_rate_overall)
        """, name="ck_emp_logic"),
        # 薪资非负
        CheckConstraint("""
            (basic_monthly_mean    IS NULL OR basic_monthly_mean    >= 0) AND
            (basic_monthly_median  IS NULL OR basic_monthly_median  >= 0) AND
            (gross_monthly_mean    IS NULL OR gross_monthly_mean    >= 0) AND
            (gross_monthly_median  IS NULL OR gross_monthly_median  >= 0)
        """, name="ck_salary_nonneg"),
        # 逻辑：gross ≥ basic
        CheckConstraint("""
            (gross_monthly_mean   IS NULL OR basic_monthly_mean   IS NULL OR gross_monthly_mean   >= basic_monthly_mean) AND
            (gross_monthly_median IS NULL OR basic_monthly_median IS NULL OR gross_monthly_median >= basic_monthly_median)
        """, name="ck_salary_logic"),
        Index("ix_result_programme_year", "programme_id", "year_id"),
    )

# ===================== 工具函数 =====================
def to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    d = df.copy()
    for c in cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    return d

def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for c in ["university", "degree"]:
        if c in d.columns:
            d[c] = d[c].astype(str).str.strip()
    return d

def get_or_create(sess: Session, model, **kwargs):
    obj = sess.query(model).filter_by(**kwargs).one_or_none()
    if obj is None:
        obj = model(**kwargs)
        sess.add(obj)
        sess.flush()
    return obj

def write_mermaid_erd(out_dir: Path) -> Path:
    content = r"""```mermaid
erDiagram
    UNIVERSITY ||--o{ PROGRAMME : has
    PROGRAMME  ||--o{ SURVEY_RESULT : has
    SURVEY_YEAR ||--o{ SURVEY_RESULT : in

    UNIVERSITY {
        int     university_id PK
        string  name  "UNIQUE, NOT NULL"
        string  short_name "UNIQUE, NULL"
    }

    PROGRAMME {
        int     programme_id PK
        int     university_id FK
        string  name "NOT NULL"
        "UNIQUE(university_id, name)"
    }

    SURVEY_YEAR {
        int       year_id PK
        smallint  year "UNIQUE, 2000..2100"
    }

    SURVEY_RESULT {
        int            result_id PK
        int            programme_id FK
        int            year_id FK
        numeric(5,2)   employment_rate_overall "0..100"
        numeric(5,2)   employment_rate_ft_perm  "0..100, <= overall"
        numeric(10,2)  basic_monthly_mean   ">= 0"
        numeric(10,2)  basic_monthly_median ">= 0"
        numeric(10,2)  gross_monthly_mean   ">= basic"
        numeric(10,2)  gross_monthly_median ">= basic"
        "UNIQUE(programme_id, year_id)"
    }
```"""
    path = out_dir / "ges_erd.md"
    path.write_text(content, encoding="utf-8-sig")
    return path

# ===================== 主流程 =====================
def run(csv_path: str, db_url: str) -> None:
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.drop_all(engine)   # 开发阶段：先清空
    Base.metadata.create_all(engine)

    # 读取 CSV
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    df = normalize_text(df)

    # 类型转换
    num_cols = [
        "employment_rate_overall","employment_rate_ft_perm",
        "basic_monthly_mean","basic_monthly_median",
        "gross_monthly_mean","gross_monthly_median","year"
    ]
    df = to_numeric(df, num_cols)

    # 只保留所需列 + 主键列不缺失
    needed = {
        "university","degree","year",
        "employment_rate_overall","employment_rate_ft_perm",
        "basic_monthly_mean","basic_monthly_median",
        "gross_monthly_mean","gross_monthly_median",
    }
    df = df[[c for c in df.columns if c in needed]].dropna(subset=["university","degree","year"])

    # 合理范围的软过滤（仍允许 NaN）
    df = df[df["year"].between(2000,2100)]
    for col in ["employment_rate_overall","employment_rate_ft_perm"]:
        if col in df.columns:
            df = df[(df[col].isna()) | (df[col].between(0,100))]
    for col in ["basic_monthly_mean","basic_monthly_median","gross_monthly_mean","gross_monthly_median"]:
        if col in df.columns:
            df = df[(df[col].isna()) | (df[col] >= 0)]

    # ---------- 关键修复：gross < basic 的记录 ----------
    EPS = 1e-6
    mask_mean = (
        df["gross_monthly_mean"].notna() & df["basic_monthly_mean"].notna() &
        (df["gross_monthly_mean"] + EPS < df["basic_monthly_mean"])
    )
    mask_median = (
        df["gross_monthly_median"].notna() & df["basic_monthly_median"].notna() &
        (df["gross_monthly_median"] + EPS < df["basic_monthly_median"])
    )
    bad_mask = mask_mean | mask_median

    if bad_mask.any():
        audit_path = OUT_DIR / "anomaly_gross_lt_basic.csv"
        df.loc[bad_mask].to_csv(audit_path, index=False, encoding="utf-8-sig")
        print(f"   - Anomaly saved (gross < basic): {audit_path.resolve()}  rows={int(bad_mask.sum())}")

        if GROSS_BASIC_STRATEGY.lower() == "clamp":
            # 把 gross 调整到 basic（最小改动）
            df.loc[mask_mean,   "gross_monthly_mean"]   = df.loc[mask_mean,   "basic_monthly_mean"]
            df.loc[mask_median, "gross_monthly_median"] = df.loc[mask_median, "basic_monthly_median"]
        elif GROSS_BASIC_STRATEGY.lower() == "drop":
            df = df[~bad_mask].copy()
        else:
            # 未知策略：默认 clamp
            df.loc[mask_mean,   "gross_monthly_mean"]   = df.loc[mask_mean,   "basic_monthly_mean"]
            df.loc[mask_median, "gross_monthly_median"] = df.loc[mask_median, "basic_monthly_median"]

    # ---------- 入库 ----------
    with Session(engine) as sess:
        uni_cache: Dict[str,int] = {}
        year_cache: Dict[int,int] = {}
        prog_cache: Dict[Tuple[int,str],int] = {}

        for _, r in df.iterrows():
            u = get_or_create(sess, University, name=str(r["university"]))
            y = get_or_create(sess, SurveyYear, year=int(r["year"]))

            # 学校内专业唯一
            p = sess.query(Programme).filter_by(university_id=u.university_id, name=str(r["degree"])).one_or_none()
            if p is None:
                p = Programme(university_id=u.university_id, name=str(r["degree"]))
                sess.add(p); sess.flush()

            # upsert-like：存在则跳过
            exist = sess.query(SurveyResult).filter_by(programme_id=p.programme_id, year_id=y.year_id).one_or_none()
            if exist:
                continue

            sess.add(SurveyResult(
                programme_id=p.programme_id, year_id=y.year_id,
                employment_rate_overall=r.get("employment_rate_overall"),
                employment_rate_ft_perm=r.get("employment_rate_ft_perm"),
                basic_monthly_mean=r.get("basic_monthly_mean"),
                basic_monthly_median=r.get("basic_monthly_median"),
                gross_monthly_mean=r.get("gross_monthly_mean"),
                gross_monthly_median=r.get("gross_monthly_median"),
            ))

        sess.commit()

    erd_path = write_mermaid_erd(OUT_DIR)
    print("✅ Section 2.1 completed.")
    print(f"   - CSV : {Path(csv_path)}")
    print(f"   - DB  : {db_url}")
    print(f"   - ERD : {erd_path.resolve()}")
    print("   - Tables: university, programme, survey_year, survey_result")
    print("   - Notes: gross<basic handled by strategy =", GROSS_BASIC_STRATEGY)

def parse_args():
    ap = argparse.ArgumentParser(description="COMP0035 Coursework 1 - Section 2.1 (3NF + Load)")
    ap.add_argument("--csv", default=DEFAULT_CSV, help="Path to GES CSV")
    ap.add_argument("--db",  default=DEFAULT_DB_URL, help="SQLAlchemy DB URL (e.g., sqlite:///ges.db)")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.csv, args.db)


