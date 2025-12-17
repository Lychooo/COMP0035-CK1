"""
Pytest fixtures for database-backed tests.

This file provides:
- an in-memory SQLite engine
- a SQLModel Session fixture with deterministic seeded data
"""

import pytest
from sqlmodel import SQLModel, Session, create_engine

from coursework2.employment_record import EmploymentRecord


@pytest.fixture()
def engine():
    """Integration fixture: create an in-memory SQLite engine and initialise tables."""
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture()
def session(engine):
    """Integration fixture: provide a Session with seeded rows for repeatable DB tests."""
    with Session(engine) as session:
        records = [
            EmploymentRecord(
                record_id=1,
                degree_id=100,
                year_id=1,
                employment_rate_overall=90.0,
                employment_rate_ft_perm=85.0,
                basic_monthly_mean=3000.0,
                basic_monthly_median=2900.0,
                gross_monthly_mean=3500.0,
                gross_monthly_median=3400.0,
                gross_mthly_25_percentile=3000.0,
                gross_mthly_75_percentile=4000.0,
            ),
            EmploymentRecord(
                record_id=2,
                degree_id=100,
                year_id=2,  # later year_id -> should be "latest"
                employment_rate_overall=91.0,
                employment_rate_ft_perm=86.0,
                basic_monthly_mean=3100.0,
                basic_monthly_median=3000.0,
                gross_monthly_mean=3600.0,
                gross_monthly_median=3500.0,
                gross_mthly_25_percentile=3100.0,
                gross_mthly_75_percentile=4200.0,
            ),
            EmploymentRecord(
                record_id=3,
                degree_id=200,
                year_id=2,
                employment_rate_overall=80.0,
                employment_rate_ft_perm=70.0,
                basic_monthly_mean=2500.0,
                basic_monthly_median=2400.0,
                gross_monthly_mean=2800.0,
                gross_monthly_median=2700.0,
                gross_mthly_25_percentile=2300.0,
                gross_mthly_75_percentile=3100.0,
            ),
        ]
        session.add_all(records)
        session.commit()
        yield session
