# Spark RAPIDS Demo – Cloudera AI

Production-ready Spark RAPIDS benchmark and qualification demo for
**IQVIA** on **Cloudera AI (CAI) Workbench** with NVIDIA GPU acceleration.

---

## Target Environment

| Property | Value |
|---|---|
| Cloud | AWS Public Cloud |
| Instance | g5.4xlarge (NVIDIA A10G 24 GB VRAM, 16 vCPU, 64 GB RAM) |
| Cloudera Runtime | CDP Public Cloud 7.3.1 |
| CAI Workbench | 2.0.53 |
| Python | 3.10 |
| Spark | 3.3.0 (DEX CDE 1.24 Runtime Hotfix 1) |

---

## Project Structure

```
CAI_Rapids_Articles/
│
├── demo/                          ← NEW: production demo code
│   ├── utils/
│   │   ├── spark_config.py        # SparkSession factory (CPU + GPU profiles)
│   │   └── timing.py              # TimingHarness + comparison utilities
│   │
│   ├── datagen/
│   │   ├── retail_datagen.py      # Parameterized 6-table retail Parquet datagen
│   │   └── banking_datagen.py     # Banking fraud Parquet datagen
│   │
│   ├── benchmark/
│   │   ├── cpu_etl.py             # 7-stage ETL pipeline (vanilla Spark)
│   │   ├── gpu_etl.py             # Identical logic, RAPIDS-accelerated session
│   │   └── run_benchmark.py       # Orchestrator: --mode cpu|gpu|both --scale N
│   │
│   ├── qualification/
│   │   ├── run_cpu_with_logging.py # CPU ETL + Spark event log
│   │   ├── run_qualification.py    # RAPIDS Qualification Tool + CSV parse
│   │   └── validate.py             # GPU ETL + actual vs predicted speedup
│   │
│   └── agent-studio/
│       └── DESIGN.md              # 4-agent workflow design + YAML template
│
├── spark-rapids-etl/              ← Original reference (preserved)
├── spark-rapids-qualification-tool/ ← Original reference (preserved)
├── spark-rapids-ml/               ← Original reference (preserved)
├── CLAUDE.md                      # Project status and working notes
└── README.md                      # This file
```

---

## Quick Start

### 1. Environment Setup

```bash
# Install Python dependencies (run once per session/image)
pip install -r demo/requirements.txt

# Download the RAPIDS accelerator JAR (run once, persists on project filesystem)
mkdir -p /home/cdsw/jars
wget -P /home/cdsw/jars \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0-cuda12.jar

# Required environment variables (set in CAI Workbench Project Settings → Advanced → Environment)
export DATA_STORAGE="s3a://iqvia-cbpoc-buk-5762ce93/rapids-demo"  # or /tmp/rapids-demo for local
export GPU_TYPE="A10G"                    # A10G (default), T4, A100
export ROW_SCALE="10"                     # 1, 10, or 50
```

> **Important:** Run benchmark scripts from a **CPU-only CAI session**.
> A GPU session already holds the GPU; the benchmark subprocesses need to
> claim it themselves and will conflict if you run from a GPU session.

### 2. Generate Demo Data

```bash
cd demo/datagen

# Retail analytics dataset (6 tables, 10x scale = 10M sales rows)
python retail_datagen.py --scale 10 --output-dir /tmp/rapids-demo/retail

# Banking fraud dataset (optional second dataset)
python banking_datagen.py --scale 10 --output-dir /tmp/rapids-demo/banking
```

**Scale guide:**

| Scale | Sales rows | Runtime (A10G) | Use case |
|---|---|---|---|
| 1x | 1 M | ~30s | Quick smoke test |
| 10x | 10 M | ~2-3 min | Standard demo |
| 50x | 50 M | ~8-10 min | "Wow" numbers |

### 3. Run the CPU vs GPU Benchmark

```bash
cd demo/benchmark

# Run both CPU and GPU, compare results:
python run_benchmark.py --mode both --scale 10

# CPU only (quick smoke test):
python run_benchmark.py --mode cpu --scale 1

# Generate data + run benchmark in one command:
python run_benchmark.py --mode both --scale 10 --generate-data

# Custom paths:
python run_benchmark.py \
  --mode both \
  --scale 10 \
  --data-dir /tmp/rapids-demo/retail \
  --output-dir /tmp/results
```

### 4. Run the Qualification Tool Flow

```bash
cd demo/qualification

# Step 1: Run CPU ETL with Spark event logging
python run_cpu_with_logging.py --scale 10

# Step 2: Run qualification tool, parse prediction
python run_qualification.py \
  --event-log-dir /tmp/rapids-demo/event-logs \
  --platform onprem

# Step 3: Run GPU ETL, compare actual vs predicted speedup
python validate.py \
  --cpu-timing /tmp/rapids-demo/results/cpu-logged/timing_cpu_logged_scale10.json \
  --data-dir /tmp/rapids-demo/retail \
  --scale 10
```

---

## Architecture

### ETL Pipeline (7 Stages)

```
Load → Clean → Join (6-table) → Feature Engineering → Window Functions
                                                              ↓
                                            Write ← Aggregations (3x groupBy)
```

**GPU-friendly operations:**
- 6-table left-outer joins (`product_name`, `customer_id`)
- Rolling avg, cumulative sum, lag/lead window functions
- 3 heavy groupBy aggregations (product×location, product×perishable, age×sales_status)
- Derived column expressions (case/when bucketing, arithmetic)
- Sort by total_revenue DESC

### SparkSession Configs (A10G)

| Config | Value | Rationale |
|---|---|---|
| `spark.rapids.memory.pinnedPool.size` | 4g | Pinned host memory for GPU transfers |
| `spark.rapids.sql.concurrentGpuTasks` | 2 | A10G handles 2 concurrent tasks |
| `spark.executor.memoryOverhead` | 4g | Extra JVM overhead headroom |
| `spark.executor.memory` | 16g | Primary executor heap |
| `spark.sql.files.maxPartitionBytes` | 256m | Balanced partition size |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 512m | AQE advisory size |

### Cost Story

```
CPU baseline  (m5.4xlarge): $0.768/hr
GPU           (g5.4xlarge): $1.624/hr
Break-even speedup:          2.1x

Any GPU speedup above 2.1x means the GPU job is cheaper per unit of work.
At 10x scale, typical speedup on retail ETL: 3-6x → GPU is ~30-60% cheaper per job.
```

---

## Agent Studio Workflow

See [`demo/agent-studio/DESIGN.md`](demo/agent-studio/DESIGN.md) for the full
4-agent workflow design:

1. **Log Reader** – validates and parses Spark event logs
2. **Qualification Runner** – runs `spark_rapids qualification`, predicts speedup
3. **Code Modifier** – generates GPU-optimized script from CPU script + recommendations
4. **Benchmark Runner** – executes both, produces final comparison report

---

## Datasets

> **All data is 100% synthetically generated** — no external data pull, no real
> customer data. `retail_datagen.py` uses `spark.range()` + `F.rand()` with fixed
> seeds, so output is fully reproducible across runs.

### Retail Analytics — Data Model

The 6 tables model a simplified retail supply chain.
`sales` is the central fact table; all four product dimension tables
join to it on `product_name`, and the customer dimension joins on `customer_id`.

```
┌─────────────────────────────────────────────────────────────────┐
│                         sales  (fact)                           │
│  sales_id │ product_name │ price │ quantity_sold │ date_of_sale │
│           │ customer_id  │                                      │
└──────┬────────────┬───────────────────────────────────┬─────────┘
       │ product_name FK (joins all 4 dims below)       │ customer_id FK
       │                                                ▼
       ├──────────► stock          ┌──────────────────────────────┐
       │            product_name   │         customer             │
       │            shelf_life     │  customer_id │ age │ gender  │
       │            quantity_in_stock│  purchase_history          │
       │            location       └──────────────────────────────┘
       │            date_received
       │
       ├──────────► supplier
       │            product_name
       │            quantity_ordered
       │            supplier_price  ← renamed from "price" to avoid join ambiguity
       │            date_ordered
       │
       ├──────────► market
       │            product_name
       │            competitor_price
       │            demand_forecast
       │            sales_trend
       │
       └──────────► logistics
                    product_name
                    shipping_cost
                    transportation_cost
                    warehouse_cost
```

**Join design:** All 50,000 product names appear in every table, so joins are
"hot" (no skew, high match rate). This maximises GPU utilisation during the
join stage and produces realistic-looking aggregation results.

**Scale factors:**

| Table | Scale 1x | Scale 10x | Scale 50x |
|---|---|---|---|
| sales | 1 M | 10 M | 50 M |
| stock | 50 K | 500 K | 2.5 M |
| supplier | 50 K | 500 K | 2.5 M |
| market | 500 K | 5 M | 25 M |
| logistics | 500 K | 5 M | 25 M |
| customer | 1 K | 10 K | 50 K |

### Banking Fraud (standalone, not used by benchmark ETL)

| Scale | Rows | Fraud rate |
|---|---|---|
| 1x | 1 M | ~10% |
| 10x | 10 M | ~10% |

Columns: age, 8 financial balances, geo coordinates, transaction_amount, fraud_trx (0/1)

---

## Development Notes

- All paths use environment variables with `/tmp/rapids-demo` defaults – no hardcoded paths
- `cpu_etl.py` and `gpu_etl.py` contain identical `run_etl()` functions – diff the
  SparkSession only
- `run_benchmark.py` launches each ETL as a subprocess for clean JVM isolation
- `utils/spark_config.py` has GPU profiles for A10G, T4, and A100 – switch via `GPU_TYPE`
- Original `spark-rapids-etl/`, `spark-rapids-qualification-tool/`, `spark-rapids-ml/`
  folders are preserved for reference

---

## References

- [NVIDIA RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/)
- [spark-rapids-user-tools (Qualification Tool)](https://github.com/NVIDIA/spark-rapids-tools)
- [Cloudera AI Workbench Documentation](https://docs.cloudera.com/machine-learning/cloud/)
- [g5.4xlarge instance specs](https://aws.amazon.com/ec2/instance-types/g5/)
