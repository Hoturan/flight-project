"""DLT Silver layer: typed, deduplicated, watermarked state vectors.

Reads from the Bronze DLT table, casts types, drops duplicates,
applies range-based DQ expectations, and writes to silver.clean_states.
"""

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

CATALOG = "flight_cat"


@dlt.table(
    name="clean_states",
    comment="Typed, deduplicated, watermarked OpenSky state vectors",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    },
)
@dlt.expect_all_or_drop("longitude_range", "longitude BETWEEN -180 AND 180")
@dlt.expect_all_or_drop("latitude_range", "latitude BETWEEN -90 AND 90")
@dlt.expect_all_or_drop("altitude_floor", "baro_altitude >= -1000 OR baro_altitude IS NULL")
@dlt.expect_all_or_drop("velocity_nonneg", "velocity >= 0 OR velocity IS NULL")
def clean_states():
    """Transform bronze → silver with typing, dedup, watermark."""
    return (
        dlt.read_stream("raw_states")
        .withWatermark("time_position_ts", "10 minutes")
        .dropDuplicates(["icao24", "time_position"])
        .select(
            col("icao24"),
            col("callsign"),
            col("origin_country"),
            col("time_position").cast(TimestampType()).alias("time_position"),
            col("last_contact").cast(TimestampType()).alias("last_contact"),
            col("longitude").cast("double"),
            col("latitude").cast("double"),
            col("baro_altitude").cast("double"),
            col("on_ground").cast("boolean"),
            col("velocity").cast("double"),
            col("true_track").cast("double"),
            col("vertical_rate").cast("double"),
            col("geo_altitude").cast("double"),
            col("squawk"),
            col("spi").cast("boolean"),
            col("position_source").cast("long"),
            col("ingest_ts"),
        )
    )