"""ML feature refresh job: builds ml_features.flight_features from silver.

This job produces a feature table stub suitable for MLflow Feature Store
or direct model training. It computes per-aircraft rolling features
from clean_states.

CLI args:
    --catalog  Unity Catalog catalog name (default: flight_cat)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, max as spark_max, window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ML feature table refresh job")
    p.add_argument("--catalog", default="flight_cat")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    silver_df = spark.table(f"{args.catalog}.silver.clean_states")

    # Aggregate features per icao24 over a 5-minute tumbling window
    features = (
        silver_df
        .withWatermark("time_position", "10 minutes")
        .groupBy(
            window("time_position", "5 minutes").alias("feat_window"),
            col("icao24"),
        )
        .agg(
            avg("velocity").alias("avg_velocity"),
            spark_max("velocity").alias("max_velocity"),
            avg("baro_altitude").alias("avg_altitude"),
            spark_max("baro_altitude").alias("max_altitude"),
            avg("vertical_rate").alias("avg_vertical_rate"),
            count("*").alias("position_count"),
        )
        .select(
            col("icao24"),
            col("feat_window.start").alias("feature_ts"),
            "avg_velocity",
            "max_velocity",
            "avg_altitude",
            "max_altitude",
            "avg_vertical_rate",
            "position_count",
        )
    )

    (
        features.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{args.catalog}.ml_features.flight_features")
    )
    print(f"Refreshed {args.catalog}.ml_features.flight_features")


if __name__ == "__main__":
    main()