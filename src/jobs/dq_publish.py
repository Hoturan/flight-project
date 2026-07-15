"""DQ publish job: extracts data-quality metrics from DLT event logs
and writes them to governance.dq_results for lineage and dashboards.

On Databricks Free Edition, system.dlt.event_log may not be available.
In that case, this job writes a simple row count summary instead.

CLI args:
    --catalog  Unity Catalog catalog name (default: flight_cat)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, count


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DQ metrics publishing job")
    p.add_argument("--catalog", default="flight_cat")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    # Try to read DLT event log (may not exist on Free Edition)
    try:
        dlt_events = spark.read.table("system.dlt.event_log")
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
    except Exception:
        # Fallback: write a simple summary of row counts per layer
        print("system.dlt.event_log not available — writing simple row count summary instead.")
        layers = ["bronze.raw_states", "silver.clean_states", "gold.current_flights"]
        rows = []
        for layer in layers:
            try:
                cnt = spark.table(f"{args.catalog}.{layer}").count()
                rows.append((layer, cnt))
            except Exception:
                rows.append((layer, -1))  # table doesn't exist yet

        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        schema = StructType([
            StructField("layer", StringType(), False),
            StructField("row_count", LongType(), False),
            StructField("published_ts", TimestampType(), False),
        ])
        from pyspark.sql.functions import current_timestamp as now
        dq_metrics = spark.createDataFrame(
            [(r[0], r[1], None) for r in rows], schema
        ).withColumn("published_ts", now())

    (
        dq_metrics.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{args.catalog}.governance.dq_results")
    )
    print(f"Published DQ metrics to {args.catalog}.governance.dq_results")


if __name__ == "__main__":
    main()
