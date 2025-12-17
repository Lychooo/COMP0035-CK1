"""
Integration tests for database query methods on EmploymentRecord.

These tests REQUIRE a SQLModel Session (in-memory SQLite) and therefore are not unit tests.
"""

import pytest

from coursework2.employment_record import EmploymentRecord


def test_get_by_year_returns_expected_records(session):
    """Integration test (not unit): query by year_id returns the expected DB rows."""
    rows = EmploymentRecord.get_by_year(session, year_id=2)
    assert len(rows) == 2
    assert {r.record_id for r in rows} == {2, 3}


def test_get_by_degree_returns_expected_records(session):
    """Integration test (not unit): query by degree_id returns the expected DB rows."""
    rows = EmploymentRecord.get_by_degree(session, degree_id=100)
    assert len(rows) == 2
    assert {r.record_id for r in rows} == {1, 2}


def test_get_latest_for_degree_returns_latest_by_year_id(session):
    """Integration test (not unit): latest record chosen by max(year_id) for the degree."""
    latest = EmploymentRecord.get_latest_for_degree(session, degree_id=100)
    assert latest is not None
    assert latest.record_id == 2
    assert latest.year_id == 2


def test_get_by_year_invalid_year_id_raises(session):
    """Integration test (not unit): invalid year_id triggers input validation (ValueError)."""
    with pytest.raises(ValueError):
        EmploymentRecord.get_by_year(session, year_id=0)
