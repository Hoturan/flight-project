"""Tests for schema definitions."""

from src.utils.schema import (
    OPENSKY_STATE_COLUMNS,
    RAW_STATE_SCHEMA,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
)


def test_opensky_state_columns_count():
    """OpenSky returns 17 positional fields per state vector."""
    assert len(OPENSKY_STATE_COLUMNS) == 17


def test_raw_state_schema_fields():
    """Raw schema should contain all 17 OpenSky columns."""
    field_names = {f.name for f in RAW_STATE_SCHEMA.fields}
    expected = set(OPENSKY_STATE_COLUMNS)
    assert expected.issubset(field_names)


def test_bronze_schema_has_metadata():
    """Bronze schema must include ingest_ts, source_time, batch_id."""
    field_names = {f.name for f in BRONZE_SCHEMA.fields}
    assert "ingest_ts" in field_names
    assert "source_time" in field_names
    assert "batch_id" in field_names


def test_silver_schema_types():
    """Silver schema should have typed timestamps."""
    time_field = next(f for f in SILVER_SCHEMA.fields if f.name == "time_position")
    assert "Timestamp" in str(time_field.dataType)

    icao_field = next(f for f in SILVER_SCHEMA.fields if f.name == "icao24")
    assert icao_field.nullable is False