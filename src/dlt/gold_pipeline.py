"""DLT Gold layer: curated data products.

Produces three gold tables:
  - current_flights: latest state vector per icao24
  - airport_congestion: rolling aggregations near major airports
  - anomaly_feed: outlier state vectors for ops alerting & ML
"""

import dlt
from pyspark.sql.functions import col, count, avg, max as spark_max, expr, window

CATALOG = "flight_cat"


@dlt.table(
    name="current_flights",
    comment="Latest state vector per aircraft (icao24)",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def current_flights():
    """Pick the most recent row per icao24 from silver."""
    return (
        dlt.read_stream("clean_states")
        .groupBy("icao24")
        .agg(
            spark_max("time_position").alias("time_position"),
            spark_max("ingest_ts").alias("ingest_ts"),
        )
    )


@dlt.table(
    name="airport_congestion",
    comment="Rolling counts of aircraft near major airports (windowed)",
)
def airport_congestion():
    """Count aircraft within rough bounding boxes of major airports per 1-min window."""
    airport_box = expr(
        """
        CASE
            WHEN latitude BETWEEN 40.6 AND 40.8 AND longitude BETWEEN -74.2 AND -73.9 THEN 'JFK'
            WHEN latitude BETWEEN 51.1 AND 51.2 AND longitude BETWEEN -0.6 AND 0.3  THEN 'LHR'
            WHEN latitude BETWEEN 49.0 AND 49.1 AND longitude BETWEEN 2.4  AND 2.7  THEN 'CDG'
            WHEN latitude BETWEEN 40.3 AND 40.5 AND longitude BETWEEN -3.7 AND -3.4 THEN 'MAD'
            ELSE NULL
        END
        """
    )

    return (
        dlt.read_stream("clean_states")
        .withColumn("airport", airport_box)
        .filter("airport IS NOT NULL")
        .withWatermark("time_position", "10 minutes")
        .groupBy(
            window("time_position", "1 minute"),
            col("airport"),
        )
        .agg(
            count("*").alias("flight_count"),
            avg("baro_altitude").alias("avg_altitude"),
            spark_max("velocity").alias("max_velocity"),
        )
    )


@dlt.table(
    name="anomaly_feed",
    comment="Outlier state vectors (altitude or velocity beyond thresholds)",
)
def anomaly_feed():
    """Flag anomalous readings for operational alerting and ML feature engineering."""
    return (
        dlt.read_stream("clean_states")
        .filter(
            "baro_altitude > 15000 OR velocity > 400 OR vertical_rate > 25 OR vertical_rate < -25"
        )
        .select(
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "longitude",
            "latitude",
            "baro_altitude",
            "velocity",
            "vertical_rate",
            "ingest_ts",
        )
    )
