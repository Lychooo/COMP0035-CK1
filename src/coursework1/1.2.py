from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ================== Config ==================
# Default CSV path: repository root (override via --csv)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_FILE_PATH = REPO_ROOT / "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (3).csv"

OUTPUT_DIR = Path("./prep_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============== Utilities ==============
def save_table(df: pd.DataFrame, name: str) -> None:
    """Save a table to CSV and Markdown for easy inclusion in the report."""
    df.to_csv(OUTPUT_DIR / f"{name}.csv", index=True)
    with open(OUTPUT_DIR / f"{name}.md", "w", encoding="utf-8") as f:
        f.write(df.to_markdown())


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def clean_base(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning shared by all questions."""
    out = df.copy()
    # Standardize likely numeric columns
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
        if c in out.columns:
            out[c] = to_num(out[c])

    # Drop exact duplicates
    out = out.drop_duplicates()

    # Remove impossible percentages and negative salaries (data quality from 1.1)
    if "employment_rate_overall" in out.columns:
        out = out[
            (out["employment_rate_overall"] >= 0)
            & (out["employment_rate_overall"] <= 100)
        ]
    if "employment_rate_ft_perm" in out.columns:
        out = out[
            (out["employment_rate_ft_perm"] >= 0)
            & (out["employment_rate_ft_perm"] <= 100)
        ]

    for sc in [
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
    ]:
        if sc in out.columns:
            out = out[out[sc].isna() | (out[sc] >= 0)]

    return out


def latest_year(df: pd.DataFrame) -> int | None:
    """Return the latest reasonable survey year (bounded to 2000�?100)."""
    if "year" not in df.columns:
        return None
    yrs = pd.to_numeric(df["year"], errors="coerce").dropna().astype(int)
    yrs = yrs[(yrs >= 2000) & (yrs <= 2100)]
    return int(yrs.max()) if not yrs.empty else None


def ensure_not_empty(df: pd.DataFrame, label: str) -> None:
    if df is None or df.empty:
        raise ValueError(f"No data available for {label} after cleaning/selection.")


# ============== Q1: Latest year, university ranking (bar plots) ==============
def q1_prepare(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Prepare latest-year median by university for salary and employment rate."""
    ly = latest_year(df)
    if ly is None:
        raise ValueError("No valid 'year' column found.")
    dfy = df[df["year"] == ly].copy()

    # Keep only relevant cols
    need_cols = ["university", "gross_monthly_median", "employment_rate_overall"]
    keep = [c for c in need_cols if c in dfy.columns]
    dfy = dfy[keep].dropna()

    # Aggregate by university (median = robust to outliers)
    grp = dfy.groupby("university").median(numeric_only=True)

    # Save table
    save_table(
        grp.sort_values("gross_monthly_median", ascending=False),
        f"q1_latest_{ly}_by_university_median",
    )

    return grp, ly


def q1_plot(grp: pd.DataFrame, ly: int) -> None:
    """Vertical bars with improved label alignment and readability."""
    # Salary bar
    if "gross_monthly_median" in grp.columns:
        ax = grp.sort_values("gross_monthly_median", ascending=False)[
            "gross_monthly_median"
        ].plot(
            kind="bar",
            figsize=(12, 6),
            title=f"Median Gross Monthly Salary by University (Latest year {ly})",
            rot=0,
        )
        # right-align, rotate labels slightly to match bars
        ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right")
        ax.set_xlabel("university")
        ax.set_ylabel("Gross monthly median (SGD)")
        ax.tick_params(axis="x", pad=6)
        fig = ax.get_figure()
        fig.tight_layout()
        fig.savefig(
            OUTPUT_DIR / f"q1_bar_salary_latest_{ly}.png",
            dpi=200,
            bbox_inches="tight",
        )
        plt.close(fig)

    # Employment rate bar
    if "employment_rate_overall" in grp.columns:
        ax = grp.sort_values("employment_rate_overall", ascending=False)[
            "employment_rate_overall"
        ].plot(
            kind="bar",
            figsize=(12, 6),
            title=f"Median Employment Rate by University (Latest year {ly})",
            rot=0,
        )
        ax.set_xticklabels(ax.get_xticklabels(), rotation=30, ha="right")
        ax.set_xlabel("university")
        ax.set_ylabel("Employment rate (%)")
        ax.set_ylim(0, 100)
        ax.tick_params(axis="x", pad=6)
        fig = ax.get_figure()
        fig.tight_layout()
        fig.savefig(
            OUTPUT_DIR / f"q1_bar_emp_latest_{ly}.png",
            dpi=200,
            bbox_inches="tight",
        )
        plt.close(fig)


# ============== Q2: Trend over time by university (lines) ==============
def q2_prepare(df: pd.DataFrame) -> pd.DataFrame:
    """(year, university) median salary for trend lines."""
    need = ["year", "university", "gross_monthly_median"]
    ok = [c for c in need if c in df.columns]
    d = df[ok].dropna()
    ensure_not_empty(d, "Q2 raw")

    # group median for stability
    g = (
        d.groupby(["year", "university"])["gross_monthly_median"]
        .median()
        .unstack("university")
    )
    g = g.sort_index()  # sort by year
    save_table(g, "q2_trend_table_year_univ_salary_median")
    return g


def q2_plot(trend_df: pd.DataFrame) -> None:
    ax = trend_df.plot(
        kind="line",
        marker="o",
        title="Median Gross Monthly Salary Trend by University (Yearly)",
    )
    ax.set_xlabel("year")
    ax.set_ylabel("Gross monthly median (SGD)")
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "q2_trend_lines_salary_by_university.png", dpi=150)
    plt.close(fig)


# ============== Q3: Relationship between employment rate and salary (scatter) ==============
def q3_prepare(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Latest-year per-record data (course granularity) for scatter plot."""
    ly = latest_year(df)
    d = df[df["year"] == ly].copy()
    need = ["employment_rate_overall", "gross_monthly_median"]
    ok = [c for c in need if c in d.columns]
    d = d[ok].dropna()
    ensure_not_empty(d, "Q3 latest-year scatter")
    save_table(d.describe().transpose(), f"q3_latest_{ly}_scatter_numeric_describe")
    return d, ly


def q3_plot(scatter_df: pd.DataFrame, ly: int) -> None:
    ax = scatter_df.plot(
        kind="scatter",
        x="employment_rate_overall",
        y="gross_monthly_median",
        title=f"Employment Rate vs Gross Monthly Median (Latest year {ly})",
    )
    ax.set_xlabel("Employment rate (%)")
    ax.set_ylabel("Gross monthly median (SGD)")
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / f"q3_scatter_emp_vs_salary_latest_{ly}.png", dpi=150)
    plt.close(fig)


# ================== Main ==================
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_FILE_PATH), help="Input CSV file")
    args = parser.parse_args()
    file_path = args.csv

    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(file_path)

    # 0) generic cleaning for all questions
    dfc = clean_base(df)
    save_table(dfc.head(10), "00_clean_preview_head")  # show cleaned structure

    # Q1
    grp, ly = q1_prepare(dfc)
    q1_plot(grp, ly)

    # Q2
    trend = q2_prepare(dfc)
    q2_plot(trend)

    # Q3
    scat, ly2 = q3_prepare(dfc)
    q3_plot(scat, ly2)

    print("✅Section 1.2 Data preparation finished.")
    print(f"   Input : {file_path}")
    print(f"   Output: {OUTPUT_DIR.resolve()}")
    print("   Artifacts: Q1 tables/bars, Q2 trend lines, Q3 scatter + describe")


if __name__ == "__main__":
    main()
