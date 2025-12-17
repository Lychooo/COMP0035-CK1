from __future__ import annotations

from typing import Optional, List

from sqlmodel import SQLModel, Field, Session, select


class EmploymentRecord(SQLModel, table=True):
    """
    ORM model mapping to the fact_employment table.
    Each record represents employment outcomes for a given degree and year.
    """

    __tablename__ = "fact_employment"

    # --- Primary key ---
    record_id: int = Field(primary_key=True)

    # --- Foreign keys (kept as plain ints to avoid joins in CW2) ---
    degree_id: int = Field(index=True)
    year_id: int = Field(index=True)

    # --- Employment rates (%) ---
    employment_rate_overall: Optional[float] = None
    employment_rate_ft_perm: Optional[float] = None

    # --- Salary metrics (SGD/month) ---
    basic_monthly_mean: Optional[float] = None
    basic_monthly_median: Optional[float] = None

    gross_monthly_mean: Optional[float] = None
    gross_monthly_median: Optional[float] = None
    gross_mthly_25_percentile: Optional[float] = None
    gross_mthly_75_percentile: Optional[float] = None

    # =========================
    # Query methods (DB-dependent)
    # =========================

    @classmethod
    def get_by_year(cls, session: Session, year_id: int) -> List["EmploymentRecord"]:
        """
        Return all employment records for a given survey year_id.
        """
        if year_id <= 0:
            raise ValueError("year_id must be a positive integer.")
        statement = select(cls).where(cls.year_id == year_id)
        return session.exec(statement).all()

    @classmethod
    def get_by_degree(cls, session: Session, degree_id: int) -> List["EmploymentRecord"]:
        """
        Return all employment records for a given degree_id across all years.
        """
        if degree_id <= 0:
            raise ValueError("degree_id must be a positive integer.")
        statement = select(cls).where(cls.degree_id == degree_id)
        return session.exec(statement).all()

    @classmethod
    def get_latest_for_degree(
        cls, session: Session, degree_id: int
    ) -> Optional["EmploymentRecord"]:
        """
        Return the most recent employment record for a given degree_id
        (based on the largest year_id).
        """
        if degree_id <= 0:
            raise ValueError("degree_id must be a positive integer.")
        statement = (
            select(cls)
            .where(cls.degree_id == degree_id)
            .order_by(cls.year_id.desc())
        )
        return session.exec(statement).first()

    # =========================
    # Pure-logic methods (DB-independent, unit-test friendly)
    # =========================

    @staticmethod
    def _is_rate_valid(rate: Optional[float]) -> bool:
        """
        A valid employment rate should be in [0, 100].
        None is treated as 'unknown' and considered valid here.
        """
        if rate is None:
            return True
        return 0.0 <= rate <= 100.0

    @staticmethod
    def _is_non_negative(value: Optional[float]) -> bool:
        """
        Salary-like values should be >= 0.
        None is treated as 'unknown' and considered valid here.
        """
        if value is None:
            return True
        return value >= 0.0

    def has_valid_employment_rates(self) -> bool:
        """
        Check whether employment_rate_overall and employment_rate_ft_perm are within [0, 100].
        """
        return (
            self._is_rate_valid(self.employment_rate_overall)
            and self._is_rate_valid(self.employment_rate_ft_perm)
        )

    def has_non_negative_salaries(self) -> bool:
        """
        Check whether all salary metrics are non-negative (when present).
        """
        salary_fields = [
            self.basic_monthly_mean,
            self.basic_monthly_median,
            self.gross_monthly_mean,
            self.gross_monthly_median,
            self.gross_mthly_25_percentile,
            self.gross_mthly_75_percentile,
        ]
        return all(self._is_non_negative(v) for v in salary_fields)

    def has_valid_percentile_order(self) -> bool:
        """
        Check percentile ordering:
            gross_25% <= gross_median <= gross_75%
        Only enforced when all three values are present.
        """
        p25 = self.gross_mthly_25_percentile
        med = self.gross_monthly_median
        p75 = self.gross_mthly_75_percentile

        if p25 is None or med is None or p75 is None:
            return True  # not enough info to validate

        return p25 <= med <= p75

    def is_sane_record(self) -> bool:
        """
        Aggregate sanity check for this record.
        This is a pure-logic wrapper to simplify testing.
        """
        return (
            self.has_valid_employment_rates()
            and self.has_non_negative_salaries()
            and self.has_valid_percentile_order()
        )

