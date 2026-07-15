"""DLT Bronze layer: reads from bronze.raw_states with data-quality expectations.

This file is executed by the Databricks Delta Live Tables pipeline.
It registers the bronze streaming table and applies DQ constraints
that quarantine invalid rows.
"""

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

from src.utils.schema import BRONZE_SCHEMA

CATALOG = "flight_cat"  # overridden by pipeline configuration


@dlt.table(
    name="raw_states",
    comment="Raw OpenSky state vectors with ingestion metadata",
    table_properties={"delta.logRetentionDuration": "interval 14 days"},
    partition_cols=["ingest_ts"],
)
@dlt.expect_all_or_drop("icao24_not_null", "icao24 IS NOT NULL")
@dlt.expect_all_or_drop("time_position_positive", "time_position > 0")
@dlt.expect_all_or_drop("origin_country_not_null", "origin_country IS NOT NULL")
def raw_states():
    """Streaming read from the bronze Delta table written by the ingestion job."""
    return (
        dlt.read_stream(f"{CATALOG}.bronze.raw_states")
        .withColumn("time_position_ts", col("time_position").cast(TimestampType()))
    )