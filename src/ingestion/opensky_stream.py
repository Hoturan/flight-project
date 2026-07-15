"""Structured Streaming ingestion job for OpenSky state vectors.

This script runs as a Databricks Job.  It uses a `rate` source to drive
micro-batches at a configurable interval, calls the OpenSky REST API
inside `foreachBatch`, and appends raw rows to `bronze.raw_states`.

Usage (inside Databricks):
    spark-submit src/ingestion/opensky_stream.py --catalog flight_cat --poll-interval 10

CLI args:
    --catalog        Unity Catalog catalog name (default: flight_cat)
    --poll-interval  Seconds between polls (default: 10)
"""

import argparse
import time
import uuid
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

from src.utils.opensky_client import OpenSkyClient
from src.utils.schema import OPENSKY_STATE_COLUMNS, RAW_STATE_SCHEMA, BRONZE_SCHEMA


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OpenSky Structured Streaming ingestion")
    p.add_argument("--catalog", default="flight_cat")
    p.add_argument("--poll-interval", type=int, default=10)
    return p.parse_args()


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def fetch_and_write(batch_df, batch_id: int):
    """foreachBatch callback: fetch OpenSky snapshot, flatten, append to bronze."""
    spark = SparkSession.getOrCreate()
    client = OpenSkyClient()

    payload = client.fetch_states()
    source_time = payload.get("time", int(time.time()))
    rows = client.flatten_states(payload)

    if not rows:
        print(f"Batch {batch_id}: no states returned, skipping.")
        return

    # Build a DataFrame from the flattened rows using the raw schema
    df = spark.createDataFrame(rows, schema=RAW_STATE_SCHEMA)

    # Add ingestion metadata
    df = df.withColumn("ingest_ts", current_timestamp()) \
           .withColumn("source_time", lit(source_time)) \
           .withColumn("batch_id", lit(batch_id))

    target = f"{args.catalog}.bronze.raw_states"
    df.write.format("delta").mode("append").saveAsTable(target)

    print(f"Batch {batch_id}: wrote {df.count()} rows to {target} (source_time={source_time})")


def main():
    global args
    args = parse_args()

    spark = get_spark()

    # Use a rate source with 1 row per poll-interval to drive micro-batches
    rate_df = spark.readStream.format("rate").option("rowsPerSecond", 1 / args.poll_interval).load()

    # The rate source provides (timestamp, value); we use it only as a trigger.
    # foreachBatch fetches from OpenSky and writes to bronze.
    query = (
        rate_df.writeStream.foreachBatch(fetch_and_write)
        .outputMode("append")
        .trigger(processingTime=f"{args.poll_interval} seconds")
        .option("checkpointLocation", "/tmp/opensky/checkpoint")
        .queryName("opensky_ingestion")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()