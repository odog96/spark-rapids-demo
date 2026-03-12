# Spark RAPIDS: GPU-Accelerated ETL on Cloudera AI

## What Is This?

This repo demonstrates how NVIDIA GPU acceleration (via [Apache Spark RAPIDS](https://nvidia.github.io/spark-rapids/))
can dramatically speed up large-scale Spark ETL pipelines — and under what conditions that
acceleration is actually *cost-effective*.

Running Spark on a GPU instance costs roughly 2× more per hour than a comparable CPU instance.
The question this demo answers is: **does the GPU finish the job fast enough to make it cheaper
overall?** The answer depends heavily on the types of operations in your pipeline and the volume
of data.

---

## What Are We Trying to Show?

Spark RAPIDS works by replacing Spark's CPU-based SQL/DataFrame execution engine with
GPU-accelerated operators — no code changes to your pipeline logic are required. The same
PySpark code runs on both CPU and GPU; only the SparkSession configuration changes.

This demo makes that concrete by:

1. **Running an identical 7-stage ETL pipeline** on CPU and GPU against the same dataset
2. **Timing each stage individually** so you can see *which operations* benefit most from GPU
3. **Calculating actual cost per job** (not just speed) using real AWS on-demand pricing
4. **Using NVIDIA's Qualification Tool** to predict GPU speedup from a CPU event log — before
   you ever run on GPU — and then validating that prediction against the actual result

### Where GPUs Shine in ETL

GPU acceleration delivers the largest gains on operations that are **data-parallel and
compute-intensive** — meaning the GPU can process many rows simultaneously:

| Operation | GPU Benefit | Why |
|---|---|---|
| Multi-table joins | **High** | Hash join build/probe phases parallelise across thousands of GPU cores |
| GroupBy aggregations | **High** | Parallel reduction over large row counts |
| Window functions (rolling avg, lag/lead) | **High** | Partition-level parallelism across many products/keys |
| Sorting | **Medium–High** | GPU radix sort outperforms CPU merge sort at scale |
| Type casting, arithmetic, case/when | **Medium** | Vectorised column operations |
| Simple filters on small data | **Low** | Overhead of GPU transfer not worth it at small scale |
| Python UDFs | **None** | UDFs run on CPU; GPU can't accelerate them |

The ETL pipeline in this demo is deliberately constructed to exercise the high-benefit
categories: it runs a 6-table join, three heavy aggregations, and rolling window functions
— exactly the workload profile where GPU acceleration pays off.

---

## The Cost Story

```
CPU baseline  m5.4xlarge : $0.768 / hr   (16 vCPU, 64 GB RAM)
GPU           g5.4xlarge : $1.624 / hr   (16 vCPU, 64 GB RAM, NVIDIA A10G 24 GB)

Break-even speedup: 2.1×

If the GPU completes the job in less than 1/2.1 of the CPU time, the GPU job costs less.
At 10× data scale, the retail ETL typically achieves 3–6× speedup → 30–60% cheaper per job.
```

The benchmark reports both the raw speedup and a cost-effectiveness verdict for every run.

---

## Environment

| Property | Value |
|---|---|
| Cloud | AWS Public Cloud (us-east-1) |
| GPU Instance | g5.4xlarge — NVIDIA A10G 24 GB VRAM, 16 vCPU, 64 GB RAM |
| Platform | Cloudera AI (CAI) Workbench 2.0.53, CDP Public Cloud 7.3.1 |
| Python | 3.10 |
| Spark | 3.3.0 (Cloudera Runtime Hotfix 1, CDE 1.24) |
| RAPIDS JAR | rapids-4-spark 24.08.0 (last release supporting Spark 3.3.x) |
| CUDA | 12.5 |

---

## Project Structure

```
demo/
├── utils/
│   ├── spark_config.py        # SparkSession factory — CPU and GPU profiles
│   └── timing.py              # TimingHarness, per-stage timing, comparison report
│
├── datagen/
│   ├── retail_datagen.py      # Generates the 6-table retail dataset (see below)
│   └── banking_datagen.py     # Generates a banking fraud dataset (standalone)
│
├── benchmark/
│   ├── cpu_etl.py             # 7-stage ETL pipeline — vanilla Spark
│   ├── gpu_etl.py             # Identical pipeline — RAPIDS-accelerated session
│   └── run_benchmark.py       # Orchestrator: --mode cpu|gpu|both --scale N
│
├── qualification/
│   ├── run_cpu_with_logging.py # Runs CPU ETL with Spark event logging enabled
│   ├── run_qualification.py    # Feeds event log to RAPIDS Qualification Tool
│   └── validate.py             # Runs GPU ETL; compares actual vs predicted speedup
│
├── agent-studio/
│   └── DESIGN.md              # Design for a 4-agent AI workflow over the benchmark
│
└── requirements.txt           # Python dependencies
```

Reference folders (original exploration notebooks, preserved):
```
spark-rapids-etl/
spark-rapids-qualification-tool/
spark-rapids-ml/
```

---

## Quick Start

### 1. Prerequisites

**Run all benchmark scripts from a CPU-only CAI session.**
A GPU session holds exclusive access to the GPU. The benchmark scripts launch Spark
as subprocesses, and each subprocess needs to claim the GPU independently. If you run
from a GPU session you will get resource allocation conflicts.

```bash
# Install Python dependencies (once per environment)
pip install -r demo/requirements.txt

# Download the RAPIDS accelerator JAR (once — persists on the project filesystem)
mkdir -p /home/cdsw/jars
wget -P /home/cdsw/jars \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0-cuda12.jar
```

### 2. Set Environment Variables

Set these in **CAI Workbench → Project Settings → Advanced → Environment Variables**
so they are available in every session automatically:

| Variable | Example | Description |
|---|---|---|
| `DATA_STORAGE` | `s3a://your-bucket/rapids-demo` | Root path for all data and results. Use `/tmp/rapids-demo` for local testing. |
| `GPU_TYPE` | `A10G` | GPU tuning profile. Options: `A10G` (default), `T4`, `A100` |
| `ROW_SCALE` | `10` | Data volume multiplier (see Scale Guide below) |
| `RAPIDS_JAR` | `/home/cdsw/jars/rapids-4-spark_2.12-24.08.0-cuda12.jar` | Override JAR path if needed |

### 3. Generate the Dataset

```bash
# Standard demo scale (10M sales rows, ~2-3 min to generate)
python demo/datagen/retail_datagen.py --scale 10

# Or use the benchmark orchestrator to generate + run in one step:
python demo/benchmark/run_benchmark.py --mode both --scale 10 --generate-data
```

### 4. Run the CPU vs GPU Benchmark

```bash
# Full comparison — runs CPU then GPU, prints speedup and cost verdict
python demo/benchmark/run_benchmark.py --mode both --scale 10

# CPU only (quick smoke test before committing GPU time)
python demo/benchmark/run_benchmark.py --mode cpu --scale 1

# GPU only
python demo/benchmark/run_benchmark.py --mode gpu --scale 10
```

### 5. Run the Qualification Tool Flow (optional)

This flow demonstrates NVIDIA's pre-migration analysis: predict GPU speedup from a
CPU event log *before* you ever run on GPU, then validate the prediction.

```bash
# Step 1: Run CPU ETL with Spark event logging
python demo/qualification/run_cpu_with_logging.py --scale 10

# Step 2: Feed the event log to the RAPIDS Qualification Tool
python demo/qualification/run_qualification.py \
    --event-log-dir $DATA_STORAGE/event-logs \
    --platform onprem

# Step 3: Run GPU ETL and compare actual vs predicted speedup
python demo/qualification/validate.py \
    --cpu-timing $DATA_STORAGE/results/cpu-logged/timing_cpu_logged_scale10.json \
    --data-dir   $DATA_STORAGE/retail \
    --scale 10
```

---

## Scale Guide

"Scale" in this demo is a **row-count multiplier** applied to the base dataset size.
Scale 1× = ~1 million sales rows, which is a quick smoke test. Scale 10× = ~10 million
rows, which is the recommended demo size. Scale 50× = ~50 million rows, producing the
most dramatic speedup numbers.

The reason scale matters for GPU comparison: **GPU overhead is fixed, benefit is
proportional to data volume.** At very small scales the GPU setup cost dominates and
you may see little or no speedup. At larger scales the parallel processing advantage
dominates and speedup grows. The break-even point for this workload is typically around
5–10 million sales rows.

| Scale | Sales rows | Approx. total data | CPU runtime (m5.4xlarge) | GPU runtime (A10G) |
|---|---|---|---|---|
| 1× | 1 M | ~0.5 GB | ~1 min | ~45 s |
| 10× | 10 M | ~5 GB | ~8–12 min | ~2–3 min |
| 50× | 50 M | ~25 GB | ~50–70 min | ~8–12 min |

---

## The ETL Pipeline

Both `cpu_etl.py` and `gpu_etl.py` run the **same 7-stage pipeline** — the only
difference between the two files is the SparkSession configuration. This makes it
easy to diff the files and confirm there is no business logic difference.

```
Stage 1: Load          Read 6 Parquet tables from storage
Stage 2: Clean         Deduplicate, cast dates, standardise strings, filter invalid rows
Stage 3: Join          6-table left-outer join (product_name × customer_id)
Stage 4: Feature Eng   Derived columns: total_cost, margin, price_vs_competitor,
                       perishable bucket, sales_status bucket, age_bucket
Stage 5: Window        Rolling avg qty, cumulative revenue, lag/lead, day-over-day delta
                       — partitioned by product_name, ordered by date_of_sale
Stage 6: Aggregate     3 groupBy summaries:
                         (a) product × location → revenue, margin, inventory
                         (b) product × perishable → stock, demand, cost
                         (c) age_bucket × sales_status × gender → customer analytics
Stage 7: Write         3 Parquet outputs to $DATA_STORAGE/results/
```

Each stage is individually timed so you can pinpoint exactly which operations
benefit most from GPU acceleration in your environment.

---

## The Dataset

> All data is **100% synthetically generated** using PySpark native functions
> (`spark.range()` + `F.rand()` with fixed seeds). No external data source is required.
> Output is fully reproducible across runs.

### Data Model — 6-Table Retail Supply Chain

`sales` is the central fact table. The four product dimension tables join to it on
`product_name`; the customer dimension joins on `customer_id`.

```
                     ┌──────────────────────────────────────────────┐
                     │                  sales  (fact)               │
                     │  sales_id  │ product_name │ price            │
                     │  quantity_sold │ date_of_sale │ customer_id  │
                     └────────┬──────────────────────────┬──────────┘
                              │  product_name FK          │ customer_id FK
              ┌───────────────┼───────────────┐           ▼
              ▼               ▼               ▼   ┌───────────────────┐
           stock          supplier         market │    customer       │
        product_name    product_name    product_name│ customer_id     │
        shelf_life      quantity_ordered competitor_price  age        │
        quantity_in_stock supplier_price demand_forecast   gender     │
        location        date_ordered    sales_trend  purchase_history │
        date_received                              └───────────────────┘
              │
              ▼
          logistics
        product_name
        shipping_cost
        transportation_cost
        warehouse_cost
```

**Why these tables?** The join pattern (4 product dims + 1 customer dim to 1 fact table)
mirrors a typical retail data warehouse star schema. It produces a workload with high
join cardinality (50,000 distinct products across all tables), multiple aggregation
dimensions, and window functions over time — the exact mix that stresses CPU Spark and
benefits most from GPU acceleration.

### Table Details

| Table | Scale 1× rows | Role | Key columns used in ETL |
|---|---|---|---|
| sales | 1 M | Fact — one row per transaction | price, quantity_sold, date_of_sale |
| stock | 50 K | Inventory snapshot per product/location | shelf_life, quantity_in_stock, location |
| supplier | 50 K | Purchase orders from suppliers | quantity_ordered |
| market | 500 K | Competitor pricing & demand signals | competitor_price, demand_forecast |
| logistics | 500 K | Supply chain cost breakdown | shipping_cost, transportation_cost, warehouse_cost |
| customer | 1 K | Customer dimension | age, gender, purchase_history |

---

## SparkSession Configuration

`demo/utils/spark_config.py` provides factory functions `get_cpu_session()` and
`get_gpu_session()` with tuning profiles for A10G, T4, and A100.

Key A10G settings and their rationale:

| Config | Value | Why |
|---|---|---|
| `spark.executor.memory` | 16 GB | Leaves headroom within 64 GB instance RAM |
| `spark.executor.memoryOverhead` | 4 GB | JVM off-heap buffer for shuffle and RAPIDS |
| `spark.rapids.memory.pinnedPool.size` | 4 GB | Pinned host memory speeds CPU↔GPU transfers |
| `spark.rapids.sql.concurrentGpuTasks` | 2 | A10G can safely run 2 concurrent GPU tasks |
| `spark.sql.files.maxPartitionBytes` | 256 MB | Balanced partition size for 16 cores |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 512 MB | AQE post-shuffle target partition size |
| `spark.dynamicAllocation.enabled` | false | Required — RAPIDS needs fixed executor count |

Switch GPU profiles by setting `GPU_TYPE=T4` or `GPU_TYPE=A100` — no code changes needed.

---

## Agent Studio Workflow (Design)

`demo/agent-studio/DESIGN.md` contains a design for automating the full
qualification-to-validation cycle using a 4-agent AI workflow:

1. **Log Reader** — parses and validates Spark event logs
2. **Qualification Runner** — invokes `spark_rapids qualification`, extracts prediction
3. **Code Modifier** — generates a GPU-optimised script from a CPU script + recommendations
4. **Benchmark Runner** — executes both scripts, produces the final comparison report

---

## References

- [NVIDIA RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/)
- [RAPIDS Qualification Tool](https://github.com/NVIDIA/spark-rapids-tools)
- [Cloudera AI Workbench Documentation](https://docs.cloudera.com/machine-learning/cloud/)
- [g5.4xlarge instance specs (AWS)](https://aws.amazon.com/ec2/instance-types/g5/)
- [RAPIDS release compatibility matrix](https://nvidia.github.io/spark-rapids/docs/supported_ops.html)
