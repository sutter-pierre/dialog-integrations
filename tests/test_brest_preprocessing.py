"""Tests for Brest preprocessing."""

from datetime import datetime

import polars as pl
import pytest

from integrations.co_brest.integration import Integration, compute_save_period_fields
from integrations.co_brest.schema import BrestRawDataSchema


@pytest.fixture
def raw_data():
    """Load test data from data.csv."""
    return pl.read_csv("tests/data/co_brest/data.csv")


@pytest.fixture
def integration():
    """Create Brest integration instance."""
    return Integration.from_organization("co_brest")


def test_validate_raw_data(integration, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = integration.validate_raw_data(raw_data)

    expected_columns = set(BrestRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_casts_booleans(integration, raw_data):
    """Test that preprocessing casts VELO and CYCLO to boolean."""
    schema_columns = list(BrestRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = integration.preprocess_raw_data(df)

    assert preprocessed["VELO"].dtype == pl.Boolean
    assert preprocessed["CYCLO"].dtype == pl.Boolean
    assert all(v in [True, False] for v in preprocessed["VELO"].to_list())


def test_cast_boolean_column_oui_to_true(integration):
    """Test that 'OUI' is cast to True."""
    df = pl.DataFrame({"test_col": ["OUI", "oui", "Oui"]})

    result = df.with_columns(integration.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [True, True, True]


def test_cast_boolean_column_non_to_false(integration):
    """Test that 'NON' is cast to False."""
    df = pl.DataFrame({"test_col": ["NON", "non", "Non"]})

    result = df.with_columns(integration.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [False, False, False]


def test_cast_boolean_column_null_to_false(integration):
    """Test that null values are filled with False."""
    df = pl.DataFrame({"test_col": ["OUI", None, "NON"]})

    result = df.with_columns(integration.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [True, False, False]


def test_compute_save_period_fields():
    """Test that compute_save_period_fields creates all required fields."""

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15, 10, 30, 45), datetime(2024, 1, 1)],
            "NOARR": ["A", "B"],
        }
    )

    result = compute_save_period_fields(df)

    # Check all period fields exist
    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    assert "period_start_time" in result.columns
    assert "period_end_time" in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    # Check values
    assert result["period_start_date"][0] == "2023-06-15T10:30:45Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True


def test_compute_save_period_fields_filters_null_dt_mat():
    """Test that rows with null DT_MAT are filtered out."""
    from datetime import datetime

    from integrations.co_brest.integration import compute_save_period_fields

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15), None, datetime(2024, 1, 1)],
            "NOARR": ["A", "B", "C"],
        }
    )

    result = compute_save_period_fields(df)

    assert result.height == 2
    assert result["NOARR"].to_list() == ["A", "C"]
