"""
cpu_etl.py
----------
CPU (vanilla Spark) ETL pipeline for the Spark RAPIDS benchmark demo.

Business logic is IDENTICAL to gpu_etl.py — only the SparkSession differs.
Do not modify the pipeline logic in one file without mirroring the change.

Pipeline stages:
  1. load        – read six Parquet tables
  2. clean       – deduplicate, cast types, standardize strings
  3. join        – six-table left-outer join on product_name / customer_id
  4. feature_eng – derived columns (total_cost, margin, perishability bucket,
                   sales status bucket)
  5. window      – rolling avg, lag, lead window functions per product
  6. aggregate   – groupBy product+location, product, age_bucket
  7. write       – write final aggregates to Parquet

Usage
-----
  python cpu_etl.py --data-dir /tmp/rapids-demo/retail --output-dir /tmp/results/cpu
  python cpu_etl.py --scale 10 --data-dir /tmp/rapids-demo/retail \\
                    --output-dir /tmp/results/cpu --timing-path /tmp/results/cpu_timing.json
"""

import argparse
import os
import sys

# Allow imports from the demo/ root when running as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.spark_config import get_cpu_session
from utils.timing import TimingHarness


# ─── Pipeline ─────────────────────────────────────────────────────────────────

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
        # Force evaluation so load time is isolated from compute time
        _ = sales_df.count()

    # ── Stage 2: Clean ─────────────────────────────────────────────────────────
    with harness.stage("clean"):
        # Drop nulls and duplicates across all tables
        sales_df     = sales_df.dropna().dropDuplicates()
        stock_df     = stock_df.dropna().dropDuplicates()
        supplier_df  = supplier_df.dropna().dropDuplicates()
        market_df    = market_df.dropna().dropDuplicates()
        logistics_df = logistics_df.dropna().dropDuplicates()
        customer_df  = customer_df.dropna().dropDuplicates()

        # Standardize string columns to UPPER TRIM (product names join cleanly)
        sales_df     = sales_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        stock_df     = (stock_df
                        .withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
                        .withColumn("location",     F.trim(F.upper(F.col("location")))))
        supplier_df  = supplier_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        market_df    = market_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))
        logistics_df = logistics_df.withColumn("product_name", F.trim(F.upper(F.col("product_name"))))

        # Cast date strings to date type
        sales_df    = sales_df.withColumn("date_of_sale",   F.col("date_of_sale").cast("date"))
        stock_df    = stock_df.withColumn("date_received",  F.col("date_received").cast("date"))
        supplier_df = supplier_df.withColumn("date_ordered", F.col("date_ordered").cast("date"))

        # Rename supplier.price to avoid ambiguity with sales.price after join
        supplier_df = supplier_df.withColumnRenamed("price", "supplier_price")

        # Filter invalid gender values in customer dim
        customer_df = customer_df.filter(F.col("gender").isin("male", "female"))

        # Force clean stage materialisation
        _ = sales_df.count()

    # ── Stage 3: Join ──────────────────────────────────────────────────────────
    # Six-table left-outer join – the primary GPU-friendly workload
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
            # Total supply-chain cost per unit
            .withColumn(
                "total_cost",
                F.coalesce(F.col("shipping_cost"), F.lit(0.0)) +
                F.coalesce(F.col("transportation_cost"), F.lit(0.0)) +
                F.coalesce(F.col("warehouse_cost"), F.lit(0.0)),
            )
            # Margin = sale price – total cost
            .withColumn(
                "margin",
                F.col("price") - F.col("total_cost"),
            )
            # Price pressure vs competitor
            .withColumn(
                "price_vs_competitor",
                F.col("price") / F.coalesce(F.col("competitor_price"), F.col("price")),
            )
            # Perishability bucket (case/when)
            .withColumn(
                "perishable",
                F.when(F.col("shelf_life") <= 30, F.lit("highly_perishable"))
                 .when(F.col("shelf_life") <= 90, F.lit("perishable"))
                 .otherwise(F.lit("non_perishable")),
            )
            # Sales performance bucket
            .withColumn(
                "sales_status",
                F.when(F.col("quantity_sold") > 7, F.lit("strong"))
                 .when(F.col("quantity_sold") > 4, F.lit("moderate"))
                 .otherwise(F.lit("weak")),
            )
            # Customer age bucket
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
    # Rolling stats and lag/lead – GPU accelerates window execution significantly
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
            # 30-day rolling average of quantity sold
            .withColumn("rolling_avg_qty", F.avg("quantity_sold").over(product_date_window))
            # Rolling cumulative revenue
            .withColumn("rolling_revenue", F.sum(
                F.col("price") * F.col("quantity_sold")).over(product_date_window))
            # Previous and next sale quantities (for trend detection)
            .withColumn("prev_qty_sold", F.lag("quantity_sold", 1).over(product_date_lag_window))
            .withColumn("next_qty_sold", F.lead("quantity_sold", 1).over(product_date_lag_window))
            # Day-over-day quantity change
            .withColumn(
                "qty_delta",
                F.col("quantity_sold") - F.coalesce(F.col("prev_qty_sold"), F.col("quantity_sold")),
            )
        )
        _ = windowed.count()

    # ── Stage 6: Aggregations ──────────────────────────────────────────────────
    # Heavy groupBy + multi-metric agg – the other primary GPU workload
    with harness.stage("aggregate"):
        # 6a: Product × location performance summary
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

        # 6b: Product-level inventory & demand rollup
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

        # 6c: Age-bucket × sales-status customer analytics
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

        # Materialise all three aggregations
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
    p = argparse.ArgumentParser(description="CPU ETL benchmark (vanilla Spark)")
    p.add_argument("--data-dir",    default=os.path.join(default_storage, "retail"),
                   help="Root directory containing the six Parquet tables")
    p.add_argument("--output-dir",  default=os.path.join(default_storage, "results", "cpu"),
                   help="Output directory for aggregated Parquet results")
    p.add_argument("--scale",       type=int, default=10,
                   help="ROW_SCALE used for the run (stored in timing JSON only)")
    p.add_argument("--timing-path", default=None,
                   help="Path for timing JSON output (auto-generated if omitted)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    timing_path = args.timing_path or os.path.join(
        args.output_dir, f"timing_cpu_scale{args.scale}.json"
    )

    spark = get_cpu_session("SparkRAPIDS-CPU-Benchmark")
    harness = TimingHarness(run_id=f"cpu_scale{args.scale}", mode="cpu", scale=args.scale)

    try:
        run_etl(spark, args.data_dir, args.output_dir, harness)
    finally:
        harness.save(timing_path)
        harness.print_summary()
        spark.stop()
