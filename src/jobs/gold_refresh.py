"""Gold Refresh job: materialize / upsert gold tables from silver.

Executed as a Databricks Job on a schedule.  Reads from silver.clean_states
and MERGEs into gold.current_flights (latest per icao24).

If silver.clean_states doesn't exist yet (DLT pipeline hasn't run), 
the job exits gracefully with a message.

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

    # Check if silver.clean_states exists
    try:
        silver_df = spark.table(f"{args.catalog}.silver.clean_states")
    except Exception:
        print(f"Table {args.catalog}.silver.clean_states not found — run the DLT pipeline first.")
        return

    row_count = silver_df.count()
    if row_count == 0:
        print(f"{args.catalog}.silver.clean_states is empty — nothing to refresh.")
        return

    # current_flights: latest row per icao24
    w = Window.partitionBy("icao24").orderBy(col("time_position").desc())
    latest = (
        silver_df.withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    latest.write.format("delta").mode("overwrite").saveAsTable(
        f"{args.catalog}.gold.current_flights"
    )
    print(f"Refreshed {args.catalog}.gold.current_flights with {latest.count()} rows")


if __name__ == "__main__":
    main()
