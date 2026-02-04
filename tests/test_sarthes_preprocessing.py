"""Tests for Sarthes preprocessing."""

import polars as pl
import pytest

from integrations.dp_sarthes.integration import Integration
from integrations.dp_sarthes.schema import SarthesRawDataSchema
from integrations.dp_sarthes.integration import compute_start_date



@pytest.fixture
def raw_data():
    """Load test data from data.csv."""
    # CSV has index column, read and drop it
    df = pl.read_csv("tests/data/dp_sarthes/data.csv")
    # Drop the index column (first column which has no name or is numeric)
    if df.columns[0] in ["", "column_1"] or df.columns[0].isdigit():
        df = df.drop(df.columns[0])
    return df


@pytest.fixture
def integration():
    """Create Sarthes integration instance."""
    return Integration.from_organization("dp_sarthes")


def test_validate_raw_data(integration, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = integration.validate_raw_data(raw_data)

    expected_columns = set(SarthesRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_is_identity(integration, raw_data):
    """Test that Sarthes has no preprocessing (identity function)."""
    schema_columns = list(SarthesRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = integration.preprocess_raw_data(df)

    assert preprocessed.columns == df.columns
    assert preprocessed.height == df.height


def test_compute_start_date_uses_annee():
    """Test that compute_start_date uses annee when present."""
    df = pl.DataFrame(
        {
            "annee": [2023.0, 2024.0],
            "date_modif": ["2023-05-15T10:00:00Z", "2024-08-20T14:30:00Z"],
        }
    )

    result = compute_start_date(df)

    assert result["period_start_date"][0] == "2023-01-01T00:00:00Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"


def test_compute_start_date_falls_back_to_date_modif():
    """Test that compute_start_date uses date_modif when annee is null."""
    from integrations.dp_sarthes.integration import compute_start_date

    df = pl.DataFrame(
        {
            "annee": [None, 2024.0],
            "date_modif": ["2023-05-15T10:00:00Z", "2024-08-20T14:30:00Z"],
        }
    )

    result = compute_start_date(df)

    assert result["period_start_date"][0] == "2023-05-15T10:00:00Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"


def test_compute_start_date_creates_all_period_fields():
    """Test that compute_start_date creates all required period fields."""
    df = pl.DataFrame(
        {
            "annee": [2023.0],
            "date_modif": ["2023-05-15T10:00:00Z"],
        }
    )

    result = compute_start_date(df)

    # Check all period fields exist
    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    assert "period_start_time" in result.columns
    assert "period_end_time" in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    # Check values
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True
    assert result["period_end_date"][0] is None
