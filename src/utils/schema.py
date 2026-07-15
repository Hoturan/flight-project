"""Schema definitions for OpenSky state vectors.

The OpenSky `states/all` endpoint returns a `states` array where each element
is a positional array of 17 fields.  This module provides the canonical
typed schema used across Bronze / Silver / Gold layers.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Positional order returned by the OpenSky REST API (index → name)
OPENSKY_STATE_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]

# Schema of the raw JSON `states` array element (all strings/nullable on ingest)
RAW_STATE_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", DoubleType(), True),
        StructField("last_contact", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", ArrayType(DoubleType()), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", LongType(), True),
    ]
)

# Schema of the Bronze Delta table (raw + ingestion metadata)
BRONZE_SCHEMA = StructType(
    RAW_STATE_SCHEMA.fields
    + [
        StructField("ingest_ts", TimestampType(), False),
        StructField("source_time", LongType(), False),  # response `time` field
        StructField("batch_id", LongType(), False),
    ]
)

# Schema of the Silver Delta table (typed + cleaned)
SILVER_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), False),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", TimestampType(), False),
        StructField("last_contact", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", LongType(), True),
        StructField("ingest_ts", TimestampType(), False),
    ]
)