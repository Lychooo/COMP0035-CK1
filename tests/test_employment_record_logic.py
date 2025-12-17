"""
Unit tests for pure-logic methods on EmploymentRecord.

These tests DO NOT use a database Session and therefore are unit tests.
"""

from coursework2.employment_record import EmploymentRecord


def test_has_valid_employment_rates_true():
    """Unit test: rate bounds valid when both rates are within [0, 100]."""
    r = EmploymentRecord(
        record_id=10,
        degree_id=1,
        year_id=1,
        employment_rate_overall=50.0,
        employment_rate_ft_perm=60.0,
    )
    assert r.has_valid_employment_rates() is True


def test_has_valid_employment_rates_invalid_overall_false():
    """Unit test: invalid overall rate (>100) should fail validation."""
    r = EmploymentRecord(
        record_id=11,
        degree_id=1,
        year_id=1,
        employment_rate_overall=120.0,
        employment_rate_ft_perm=60.0,
    )
    assert r.has_valid_employment_rates() is False


def test_has_valid_employment_rates_invalid_ft_false():
    """Unit test: invalid full-time permanent rate (<0) should fail validation."""
    r = EmploymentRecord(
        record_id=12,
        degree_id=1,
        year_id=1,
        employment_rate_overall=60.0,
        employment_rate_ft_perm=-1.0,
    )
    assert r.has_valid_employment_rates() is False


def test_has_valid_employment_rates_none_is_allowed():
    """Unit test: None rates are treated as 'unknown' and considered valid."""
    r = EmploymentRecord(
        record_id=13,
        degree_id=1,
        year_id=1,
        employment_rate_overall=None,
        employment_rate_ft_perm=None,
    )
    assert r.has_valid_employment_rates() is True


def test_has_non_negative_salaries_true():
    """Unit test: salary fields should be valid when all present values are >= 0."""
    r = EmploymentRecord(
        record_id=14,
        degree_id=1,
        year_id=1,
        basic_monthly_mean=1.0,
        basic_monthly_median=1.0,
        gross_monthly_mean=1.0,
        gross_monthly_median=1.0,
        gross_mthly_25_percentile=1.0,
        gross_mthly_75_percentile=2.0,
    )
    assert r.has_non_negative_salaries() is True


def test_has_non_negative_salaries_negative_false():
    """Unit test: any negative salary value should fail validation."""
    r = EmploymentRecord(
        record_id=15,
        degree_id=1,
        year_id=1,
        gross_monthly_median=-0.01,
    )
    assert r.has_non_negative_salaries() is False


def test_has_valid_percentile_order_true():
    """Unit test: percentile order holds when p25 <= median <= p75."""
    r = EmploymentRecord(
        record_id=16,
        degree_id=1,
        year_id=1,
        gross_mthly_25_percentile=2000.0,
        gross_monthly_median=3000.0,
        gross_mthly_75_percentile=4000.0,
    )
    assert r.has_valid_percentile_order() is True


def test_has_valid_percentile_order_false_when_violated():
    """Unit test: percentile order fails when p25 > median (violates p25 <= median)."""
    r = EmploymentRecord(
        record_id=17,
        degree_id=1,
        year_id=1,
        gross_mthly_25_percentile=3500.0,
        gross_monthly_median=3000.0,
        gross_mthly_75_percentile=4000.0,
    )
    assert r.has_valid_percentile_order() is False
