from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ========= Config =========
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_FILE_PATH = REPO_ROOT / "7-GraduateEmploymentSurveyNTUNUSSITSMUSUSSSUTD (2).csv"

OUTPUT_DIR = Path("./eda_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ========= Helper Functions =========
def save_table(df: pd.DataFrame, name: str) -> None:
    """Save a table to CSV and Markdown for easy report inclusion."""
    df.to_csv(OUTPUT_DIR / f"{name}.csv", index=True)
    with open(OUTPUT_DIR / f"{name}.md", "w", encoding="utf-8") as f:
        f.write(df.to_markdown())


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Convert selected columns to numeric if possible."""
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def hist_pair(
    df: pd.DataFrame,
    col1: str,
    col2: str,
    title1: str,
    title2: str,
    xlabel: str,
    filename: str,
) -> None:
    """Plot two histograms side by side for comparison."""
    if col1 not in df.columns or col2 not in df.columns:
        return
    fig, axes = plt.subplots(ncols=2, figsize=(8, 3.2), sharey=True)
    df[col1].plot(kind="hist", bins=20, ax=axes[0], title=title1)
    df[col2].plot(kind="hist", bins=20, ax=axes[1], title=title2)
    axes[0].set_xlabel(xlabel)
    axes[1].set_xlabel(xlabel)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, dpi=150)
    plt.close(fig)


def box_by(df: pd.DataFrame, col: str, by_col: str, title: str, ylabel: str) -> None:
    """Create a boxplot grouped by a categorical column."""
    if col in df.columns and by_col in df.columns:
        ax = df.boxplot(column=col, by=by_col, rot=30)
        plt.title(title)
        plt.suptitle("")  # remove auto suptitle
        plt.ylabel(ylabel)
        plt.xticks(rotation=30, ha="right")
        fig = ax.get_figure()
        fig.tight_layout()
        fig.savefig(
            OUTPUT_DIR / f"box_{col}_by_{by_col}.png", dpi=150, bbox_inches="tight"
        )
        plt.close(fig)


def trend_over_time(
    df: pd.DataFrame,
    y_col: str,
    x_col: str = "year",
    agg: str = "median",
    title_prefix: str = "Median",
) -> None:
    """Plot a trend line of y_col vs. year (aggregated by median or mean)."""
    if x_col not in df.columns or y_col not in df.columns:
        return

    ser_year = pd.to_numeric(df[x_col], errors="coerce")
    ser_y = pd.to_numeric(df[y_col], errors="coerce")
    tmp = pd.DataFrame({x_col: ser_year, y_col: ser_y}).dropna()

    if tmp.empty:
        return

    if agg == "median":
        ts = tmp.groupby(x_col)[y_col].median()
    elif agg == "mean":
        ts = tmp.groupby(x_col)[y_col].mean()
    else:
        raise ValueError("agg must be 'median' or 'mean'")

    ts = ts.sort_index()

    ax = ts.plot(kind="line", marker="o", title=f"{title_prefix} {y_col} over time")
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col.replace("_", " ").title())
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / f"trend_{y_col}_over_time.png", dpi=150)
    plt.close(fig)


# ========= Main Workflow =========
def main() -> None:
    # 1) Load
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_FILE_PATH), help="Input CSV file")
    args = parser.parse_args()
    file_path = args.csv

    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(file_path)

    # 2) Basic structure
    n_rows, n_cols = df.shape
    dtypes = df.dtypes.astype(str)
    overview = pd.DataFrame(
        {
            "value": [
                f"{n_rows} rows",
                f"{n_cols} columns",
                ", ".join(df.columns.astype(str).tolist()),
            ]
        },
        index=["shape_rows", "shape_cols", "columns"],
    )
    save_table(overview, "00_overview")
    save_table(pd.DataFrame({"dtype": dtypes}), "01_dtypes")

    # 3) Missing & duplicates
    save_table(
        df.isnull().sum().sort_values(ascending=False).to_frame("missing_count"),
        "02_missing_by_column",
    )
    save_table(
        pd.DataFrame({"duplicated_rows": [df.duplicated().sum()]}),
        "02b_duplicated_rows",
    )

    # 4) Descriptive statistics
    save_table(df.describe().transpose(), "03_describe_numeric")
    save_table(df.describe(include="all").transpose(), "04_describe_all")
    save_table(
        df.nunique().sort_values(ascending=False).to_frame("nunique"),
        "05_nunique_by_column",
    )

    # 5) Convert likely numeric columns
    likely_numeric = [
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "basic_monthly_mean",
        "basic_monthly_median",
        "gross_monthly_mean",
        "gross_monthly_median",
        "year",
    ]
    df_num = coerce_numeric(df, likely_numeric)

    # 6) Paired histograms (no duplicates)
    hist_pair(
        df_num,
        "employment_rate_overall",
        "employment_rate_ft_perm",
        "Overall Employment Rate Distribution",
        "Full-time Permanent Employment Rate Distribution",
        "Employment Rate (%)",
        "hist_employment_rate_pair.png",
    )
    hist_pair(
        df_num,
        "basic_monthly_mean",
        "basic_monthly_median",
        "Basic Monthly Mean Salary Distribution",
        "Basic Monthly Median Salary Distribution",
        "Salary (SGD)",
        "hist_basic_salary_pair.png",
    )
    hist_pair(
        df_num,
        "gross_monthly_mean",
        "gross_monthly_median",
        "Gross Monthly Mean Salary Distribution",
        "Gross Monthly Median Salary Distribution",
        "Salary (SGD)",
        "hist_gross_salary_pair.png",
    )

    # 7) Boxplots by University (unique)
    box_by(
        df_num,
        "employment_rate_overall",
        "university",
        "Overall Employment Rate by University",
        "Employment Rate (%)",
    )
    box_by(
        df_num,
        "basic_monthly_median",
        "university",
        "Basic Monthly Median Salary by University",
        "Salary (SGD)",
    )

    # 8) Line Charts (Trends)
    trend_over_time(
        df_num,
        "basic_monthly_median",
        "year",
        agg="median",
        title_prefix="Median",
    )
    trend_over_time(
        df_num,
        "employment_rate_overall",
        "year",
        agg="median",
        title_prefix="Median",
    )

    # 9) Simple anomaly checks
    anomalies = {}
    if "employment_rate_overall" in df_num.columns:
        anomalies["employment_rate_overall_out_of_range"] = df_num[
            (df_num["employment_rate_overall"] < 0)
            | (df_num["employment_rate_overall"] > 100)
        ]
    if "employment_rate_ft_perm" in df_num.columns:
        anomalies["employment_rate_ft_perm_out_of_range"] = df_num[
            (df_num["employment_rate_ft_perm"] < 0)
            | (df_num["employment_rate_ft_perm"] > 100)
        ]
    bad_salary_cols = [
        c for c in ["basic_monthly_mean", "gross_monthly_mean"] if c in df_num.columns
    ]
    if bad_salary_cols:
        anomalies["salary_negative"] = df_num[(df_num[bad_salary_cols] < 0).any(axis=1)]

    for name, sub in anomalies.items():
        if not sub.empty:
            save_table(sub, f"06_anomaly_{name}")

    # 10) Top-10 frequencies for categorical columns
    num_cols = df_num.select_dtypes(include="number").columns
    for col in df.columns:
        if col not in num_cols:
            vc = df[col].astype("string").value_counts(dropna=False).head(10)
            save_table(vc.to_frame(name="count"), f"07_top10_freq_{col}")

    # Done
    print("✅ Section 1.1 EDA completed.")
    print(f"   - Input : {file_path}")
    print(f"   - Output: {OUTPUT_DIR.resolve()}")
    print(
        "   - Saved: overview/dtypes/missing/duplicates/describe/nunique/"
        "hist/box/trend/anomalies/top10-freq"
    )


if __name__ == "__main__":
    main()

