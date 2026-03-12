"""
gpu_etl.py
----------
GPU (NVIDIA RAPIDS-accelerated Spark) ETL pipeline for the Spark RAPIDS benchmark.

Business logic is IDENTICAL to cpu_etl.py — only the SparkSession differs.
Do not modify the pipeline logic in one file without mirroring the change.

The SparkSession is configured for an NVIDIA A10G GPU (24 GB VRAM) on a
g5.4xlarge instance (16 vCPU, 64 GB RAM) running on AWS.  Switch GPU_TYPE
env var to T4 or A100 to activate alternative tuning profiles.

Usage
-----
  python gpu_etl.py --data-dir /tmp/rapids-demo/retail --output-dir /tmp/results/gpu
  GPU_TYPE=A10G python gpu_etl.py --scale 10 \\
      --data-dir /tmp/rapids-demo/retail \\
      --output-dir /tmp/results/gpu \\
      --timing-path /tmp/results/gpu_timing.json
"""

import argparse
import os
import sys

# Allow imports from the demo/ root when running as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.spark_config import get_gpu_session
from utils.timing import TimingHarness


# ─── Pipeline ─────────────────────────────────────────────────────────────────
# NOTE: run_etl() is intentionally identical to the version in cpu_etl.py.
#       Any pipeline logic change must be applied to both files.

def run_etl(spark: SparkSession, data_dir: str, output_dir: str,
            harness: TimingHarness) -> None:
    """
    Execute the full retail ETL pipeline.

    Parameters
    ----------
    spark : SparkSession
    data_dir : str
        Root directory containing the six Parquet table sub-directories.
    output_dir : str
        Destination for output Parquet files.
    harness : TimingHarness
        Timing harness to record per-stage wall-clock durations.
    """

    # ── Stage 1: Load ──────────────────────────────────────────────────────────
    with harness.stage("load"):
        sales_df     = spark.read.parquet(os.path.join(data_dir, "sales"))
        stock_df     = spark.read.parquet(os.path.join(data_dir, "stock"))
        supplier_df  = spark.read.parquet(os.path.join(data_dir, "supplier"))
        market_df    = spark.read.parquet(os.path.join(data_dir, "market"))
        logistics_df = spark.read.parquet(os.path.join(data_dir, "logistics"))
        customer_df  = spark.read.parquet(os.path.join(data_dir, "customer"))
        _ = sales_df.count()

    # ── Stage 2: Clean ─────────────────────────────────────────────────────────
    with harness.stage("clean"):
        sales_df     = sales_df.dropna().dropDuplicates()
        stock_df     = stock_df.dropna().dropDuplicates()
        supplier_df  = supplier_df.dropna().dropDuplicates()
        market_df    = market_df.dropna().dropDuplicates()
        logistics_df = logistics_df.dropna().dropDuplicates()
        customer_df  = customer_df.dropna().dropDuplicates()

        sales_df     = sales_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        stock_df     = (stock_df
                        .withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
                        .withColumn("location",     F.trim(F.upper(F.col("location")))))
        supplier_df  = supplier_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        market_df    = market_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        logistics_df = logistics_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))

        sales_df    = sales_df.withColumn("date_of_sale",   F.col("date_of_sale").cast("date"))
        stock_df    = stock_df.withColumn("date_received",  F.col("date_received").cast("date"))
        supplier_df = supplier_df.withColumn("date_ordered", F.col("date_ordered").cast("date"))

        # Rename supplier.price to avoid ambiguity with sales.price after join
        supplier_df = supplier_df.withColumnRenamed("price", "supplier_price")

        customer_df = customer_df.filter(F.col("gender").isin("male", "female"))
        _ = sales_df.count()

    # ── Stage 3: Join ──────────────────────────────────────────────────────────
    with harness.stage("join"):
        joined = (
            sales_df
            .join(stock_df,     "product_name", "leftouter")
            .join(supplier_df,  "product_name", "leftouter")
            .join(market_df,    "product_name", "leftouter")
            .join(logistics_df, "product_name", "leftouter")
            .join(customer_df,  "customer_id",  "leftouter")
        )
        _ = joined.count()

    # ── Stage 4: Feature Engineering ───────────────────────────────────────────
    with harness.stage("feature_eng"):
        featured = (
            joined
            .withColumn(
                "total_cost",
                F.coalesce(F.col("shipping_cost"), F.lit(0.0)) +
                F.coalesce(F.col("transportation_cost"), F.lit(0.0)) +
                F.coalesce(F.col("warehouse_cost"), F.lit(0.0)),
            )
            .withColumn(
                "margin",
                F.col("price") - F.col("total_cost"),
            )
            .withColumn(
                "price_vs_competitor",
                F.col("price") / F.coalesce(F.col("competitor_price"), F.col("price")),
            )
            .withColumn(
                "perishable",
                F.when(F.col("shelf_life") <= 30, F.lit("highly_perishable"))
                 .when(F.col("shelf_life") <= 90, F.lit("perishable"))
                 .otherwise(F.lit("non_perishable")),
            )
            .withColumn(
                "sales_status",
                F.when(F.col("quantity_sold") > 7, F.lit("strong"))
                 .when(F.col("quantity_sold") > 4, F.lit("moderate"))
                 .otherwise(F.lit("weak")),
            )
            .withColumn(
                "age_bucket",
                F.when(F.col("age") < 30, F.lit("18-29"))
                 .when(F.col("age") < 45, F.lit("30-44"))
                 .when(F.col("age") < 60, F.lit("45-59"))
                 .otherwise(F.lit("60+")),
            )
        )
        _ = featured.count()

    # ── Stage 5: Window Functions ──────────────────────────────────────────────
    with harness.stage("window"):
        product_date_window = (
            Window
            .partitionBy("product_name")
            .orderBy("date_of_sale")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        product_date_lag_window = (
            Window.partitionBy("product_name").orderBy("date_of_sale")
        )

        windowed = (
            featured
            .withColumn("rolling_avg_qty", F.avg("quantity_sold").over(product_date_window))
            .withColumn("rolling_revenue", F.sum(
                F.col("price") * F.col("quantity_sold")).over(product_date_window))
            .withColumn("prev_qty_sold", F.lag("quantity_sold", 1).over(product_date_lag_window))
            .withColumn("next_qty_sold", F.lead("quantity_sold", 1).over(product_date_lag_window))
            .withColumn(
                "qty_delta",
                F.col("quantity_sold") - F.coalesce(F.col("prev_qty_sold"), F.col("quantity_sold")),
            )
        )
        _ = windowed.count()

    # ── Stage 6: Aggregations ──────────────────────────────────────────────────
    with harness.stage("aggregate"):
        by_product_location = (
            windowed
            .groupBy("product_name", "location")
            .agg(
                F.sum(F.col("price") * F.col("quantity_sold")).alias("total_revenue"),
                F.avg("price").alias("avg_price"),
                F.sum("quantity_ordered").alias("total_ordered"),
                F.sum("quantity_sold").alias("total_sold"),
                F.avg("margin").alias("avg_margin"),
                F.avg("rolling_avg_qty").alias("avg_rolling_qty"),
                F.stddev("price").alias("price_stddev"),
                F.countDistinct("customer_id").alias("unique_customers"),
            )
            .sort(F.desc("total_revenue"))
        )

        by_product = (
            windowed
            .groupBy("product_name", "perishable")
            .agg(
                F.sum("quantity_in_stock").alias("total_stock"),
                F.avg("demand_forecast").alias("avg_demand_forecast"),
                F.avg("price_vs_competitor").alias("avg_price_vs_competitor"),
                F.sum(F.col("price") * F.col("quantity_sold")).alias("total_sales_value"),
                F.avg("total_cost").alias("avg_supply_chain_cost"),
            )
        )

        by_age_sales = (
            windowed
            .groupBy("age_bucket", "sales_status", "gender")
            .agg(
                F.count("*").alias("txn_count"),
                F.sum(F.col("price") * F.col("quantity_sold")).alias("revenue"),
                F.avg("purchase_history").alias("avg_purchase_history"),
                F.avg("price").alias("avg_price"),
            )
            .sort("age_bucket", "sales_status")
        )

        _ = by_product_location.count()
        _ = by_product.count()
        _ = by_age_sales.count()

    # ── Stage 7: Write ─────────────────────────────────────────────────────────
    with harness.stage("write"):
        by_product_location.write.mode("overwrite").parquet(
            os.path.join(output_dir, "by_product_location")
        )
        by_product.write.mode("overwrite").parquet(
            os.path.join(output_dir, "by_product")
        )
        by_age_sales.write.mode("overwrite").parquet(
            os.path.join(output_dir, "by_age_sales")
        )


# ─── Entry point ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    default_storage = os.environ.get("DATA_STORAGE", "/tmp/rapids-demo")
    p = argparse.ArgumentParser(
        description="GPU ETL benchmark (Spark RAPIDS accelerated)"
    )
    p.add_argument("--data-dir",    default=os.path.join(default_storage, "retail"),
                   help="Root directory containing the six Parquet tables")
    p.add_argument("--output-dir",  default=os.path.join(default_storage, "results", "gpu"),
                   help="Output directory for aggregated Parquet results")
    p.add_argument("--scale",       type=int, default=10,
                   help="ROW_SCALE used for the run (stored in timing JSON only)")
    p.add_argument("--timing-path", default=None,
                   help="Path for timing JSON output (auto-generated if omitted)")
    p.add_argument("--event-log-dir", default=None,
                   help="Enable Spark event logging to this directory")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    timing_path = args.timing_path or os.path.join(
        args.output_dir, f"timing_gpu_scale{args.scale}.json"
    )

    spark = get_gpu_session(
        app_name="SparkRAPIDS-GPU-Benchmark",
        event_log_dir=args.event_log_dir,
    )
    harness = TimingHarness(run_id=f"gpu_scale{args.scale}", mode="gpu", scale=args.scale)

    try:
        run_etl(spark, args.data_dir, args.output_dir, harness)
    finally:
        harness.save(timing_path)
        harness.print_summary()
        spark.stop()
