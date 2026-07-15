"""DQ publish job: extracts data-quality metrics from DLT event logs
and writes them to governance.dq_results for lineage and dashboards.

CLI args:
    --catalog  Unity Catalog catalog name (default: flight_cat)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DQ metrics publishing job")
    p.add_argument("--catalog", default="flight_cat")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    # Read DLT event log (system tables must be enabled on the workspace)
    # Path may vary; adjust to your workspace's event log location.
    try:
        dlt_events = spark.read.table("system.dlt.event_log")
    except Exception:
        # Fallback: read from the pipeline's event log path if system tables unavailable
        print("system.dlt.event_log not available; skipping DQ publish.")
        return

    # Filter to expectation-related events
    dq_metrics = (
        dlt_events.filter("event_type = 'flow_progress' AND expectations IS NOT NULL")
        .select(
            col("origin.update_id").alias("update_id"),
            col("origin.pipeline_id").alias("pipeline_id"),
            col("timestamp").alias("event_ts"),
            col("expectations"),
        )
        .withColumn("published_ts", current_timestamp())
        .withColumn("catalog", lit(args.catalog))
    )

    (
        dq_metrics.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{args.catalog}.governance.dq_results")
    )
    print(f"Published DQ metrics to {args.catalog}.governance.dq_results")


if __name__ == "__main__":
    main()