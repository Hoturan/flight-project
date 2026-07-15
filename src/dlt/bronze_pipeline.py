"""DLT Bronze layer: reads from the externally-populated bronze.raw_states table.

The ingestion job (flight-ingest-streaming) writes to flight_cat.bronze.raw_states.
This DLT table reads from that external table and applies DQ expectations.
The DLT table is named 'bronze.dlt_bronze_states' to avoid a naming cycle.
"""

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

CATALOG = "flight_cat"


@dlt.table(
    name="bronze.dlt_bronze_states",
    comment="DLT bronze layer — reads from externally-ingested bronze.raw_states",
    table_properties={"delta.logRetentionDuration": "interval 14 days"},
)
@dlt.expect_all_or_drop({
    "icao24_not_null": "icao24 IS NOT NULL",
    "time_position_positive": "time_position > 0",
    "origin_country_not_null": "origin_country IS NOT NULL",
})
def dlt_bronze_states():
    """Streaming read from the bronze Delta table written by the ingestion job."""
    return (
        dlt.read_stream(f"{CATALOG}.bronze.raw_states")
        .withColumn("time_position_ts", col("time_position").cast(TimestampType()))
    )
