"""Gold refresh job: materialize / upsert gold tables from silver.

Executed as a Databricks Job on a schedule.  Reads from silver.clean_states
and MERGEs into gold.current_flights (latest per icao24).

CLI args:
    --catalog  Unity Catalog catalog name (default: flight_cat)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, row_number
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold table refresh job")
    p.add_argument("--catalog", default="flight_cat")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    # --- current_flights: latest row per icao24 ---
    silver_df = spark.table(f"{args.catalog}.silver.clean_states")

    w = Window.partitionBy("icao24").orderBy(col("time_position").desc())
    latest = (
        silver_df.withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    latest.write.format("delta").mode("overwrite").saveAsTable(
        f"{args.catalog}.gold.current_flights"
    )
    print(f"Refreshed {args.catalog}.gold.current_flights")


if __name__ == "__main__":
    main()