"""Batch ingestion job for OpenSky state vectors.

Fetches a single snapshot from the OpenSky REST API and writes it to
bronze.raw_states. Designed to run as a scheduled Databricks Job
(notebook_task) on Free Edition serverless compute.

Usage:
    exec(open("src/ingestion/opensky_stream.py").read())
    # or via notebook wrapper with dbutils.widgets
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from src.utils.opensky_client import OpenSkyClient
from src.utils.schema import RAW_STATE_SCHEMA


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OpenSky batch ingestion")
    p.add_argument("--catalog", default="flight_cat")
    p.add_argument("--poll-interval", type=int, default=10)
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    client = OpenSkyClient()

    # Fetch a single snapshot from OpenSky
    payload = client.fetch_states()
    source_time = payload.get("time", int(time.time()))
    rows = client.flatten_states(payload)

    if not rows:
        print("No states returned from OpenSky, skipping.")
        return

    # Build DataFrame from flattened rows
    df = spark.createDataFrame(rows, schema=RAW_STATE_SCHEMA)

    # Add ingestion metadata
    df = df.withColumn("ingest_ts", current_timestamp()) \
           .withColumn("source_time", lit(source_time)) \
           .withColumn("batch_id", lit(int(time.time())))

    target = f"{args.catalog}.bronze.raw_states"
    df.write.format("delta").mode("append").saveAsTable(target)

    print(f"Wrote {df.count()} rows to {target} (source_time={source_time})")


if __name__ == "__main__":
    main()
