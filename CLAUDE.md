# CLAUDE.md – Project Status & Working Notes

## Project
**CAI_Rapids_Articles** – Spark RAPIDS CPU vs GPU benchmark demo
Cloudera AI (CAI) Workbench 2.0.53, CDP Public Cloud 7.3.1, Spark 3.3.0 DEX CDE 1.24

## Target Environment
- Instance: g5.4xlarge (NVIDIA A10G 24 GB VRAM, 16 vCPU, 64 GB RAM)
- AWS Public Cloud, us-east-1
- Python 3.10, Spark 3.3.0

## Current Status (2026-03-12)
All six sections completed in initial build:

### ✅ Section 1 – Repo Restructure
Created `demo/` with clean sub-directories. Original folders preserved as reference.

### ✅ Section 2 – Parameterized Datagen (`demo/datagen/`)
- `retail_datagen.py` – six-table retail dataset using native PySpark (no external libs)
- `banking_datagen.py` – banking fraud dataset, adapted from `spark-rapids-etl/01_datagen.py`
- Scale factor controlled by `--scale` arg or `ROW_SCALE` env var
- Output: Parquet to configurable path (local or S3)
- Default scale: 10x (10M sales rows), recommended demo; 50x for "wow" numbers

### ✅ Section 3 – CPU vs GPU Benchmark (`demo/benchmark/`)
- `cpu_etl.py` – vanilla SparkSession, 7-stage pipeline
- `gpu_etl.py` – identical logic, RAPIDS SparkSession (A10G tuned)
- `run_benchmark.py` – orchestrator: `--mode cpu|gpu|both --scale N`
- Pipeline stages: load, clean, join (6-table), feature_eng, window, aggregate, write
- Timing harness wraps each stage; outputs JSON + summary table

### ✅ Section 4 – Qualification Tool Flow (`demo/qualification/`)
- `run_cpu_with_logging.py` – CPU ETL + Spark event logging; writes manifest.json
- `run_qualification.py` – runs `spark_rapids qualification`, parses output CSV
- `validate.py` – runs GPU ETL, compares actual vs qualification-predicted speedup

### ✅ Section 5 – Agent Studio Design (`demo/agent-studio/DESIGN.md`)
- 4-agent workflow: Log Reader, Qualification Runner, Code Modifier, Benchmark Runner
- Full YAML workflow structure included
- LLM prompt template for Code Modifier agent included

### ✅ Section 6 – SparkSession Configs (`demo/utils/spark_config.py`)
- `get_cpu_session()` / `get_gpu_session()` factory functions
- Per-GPU tuning profiles: A10G (default), T4, A100
- A10G-specific settings as specified in brief
- Cost reference block: m5.4xlarge $0.768/hr, g5.4xlarge $1.624/hr, break-even 2.1x
- Shared timing utilities in `utils/timing.py`

## Key Decisions Made

1. **Datagen uses native PySpark** (spark.range + F.rand) instead of dbldatagen to
   eliminate external dependencies. The retail tables use a shared product-name
   space (50K distinct products) so join keys are "hot" and joins are realistic.

2. **ETL logic is copy-duplicated between cpu_etl.py and gpu_etl.py** (not shared)
   to make it easy for a customer to diff the two files and see only the
   SparkSession change. A comment at the top of each file enforces parity.

3. **run_benchmark.py uses subprocess** to invoke cpu_etl.py / gpu_etl.py so each
   run gets a fresh JVM/Spark context without contention.

4. **Scale defaults**: ROW_SCALE=10 (10M sales). This is tuned to run in ~2-3min
   on an A10G. Scale=50 runs in ~5-10min and produces more dramatic speedup numbers.

5. **Qualification tool platform**: defaults to `onprem` for CAI Workbench local GPU.

## Open Questions — RESOLVED (2026-03-12)

- [x] **Qualification tool version**: `spark-rapids-user-tools 26.2.0` installed.
      Required fix: upgrade `matplotlib>=3.9` (older builds crash with NumPy 2.x).
      See `demo/requirements.txt`.

- [x] **getGpusResources.sh path**: confirmed at
      `/runtime-addons/spark330-24.1-h1-ga3qav/opt/spark/examples/src/main/scripts/getGpusResources.sh`
      Updated in `demo/utils/spark_config.py`.

- [x] **DATA_STORAGE / Kerberos**: `spark.kerberos.access.hadoopFileSystems` IS needed
      for `s3a://` paths on CDP. Already handled conditionally in `_storage_config()`.
      No code change needed. Set `DATA_STORAGE=s3a://<your-bucket>/rapids-demo`
      in CAI project environment variables.

- [x] **RAPIDS jar version**: correct version is **24.08.0-cuda12** (not 25.08.0).
      RAPIDS dropped Spark 3.3.x support after 24.x. CUDA 12.5 is on the g5.4xlarge image.
      JAR downloaded to `/home/cdsw/jars/rapids-4-spark_2.12-24.08.0-cuda12.jar`.
      `spark.jars` config added to `spark_config.py`. Override path via `RAPIDS_JAR` env var.

## Next Steps (if needed)

- Run from a **CPU-only CAI session** (GPU session holds the GPU; benchmark subprocesses
  need to claim it themselves)
- Set `DATA_STORAGE=s3a://<your-bucket>/rapids-demo` in project env vars
  (or use `/tmp/rapids-demo` for local testing)
- Run `demo/datagen/retail_datagen.py --scale 10` to generate test data
- Run `demo/benchmark/run_benchmark.py --mode cpu --scale 1` for a quick CPU smoke-test
- Run full `--mode both --scale 10` for the customer demo
- Add a Jupyter notebook wrapper if the customer prefers notebook-first demo
- Wire up Agent Studio workflow YAML once the production schema is confirmed

## File Map
```
demo/
  utils/
    spark_config.py     # SparkSession factory, GPU profiles, cost reference
    timing.py           # TimingHarness, compare_runs, print_comparison
  datagen/
    retail_datagen.py   # 6-table retail Parquet datagen
    banking_datagen.py  # banking fraud Parquet datagen
  benchmark/
    cpu_etl.py          # CPU ETL pipeline (7 stages)
    gpu_etl.py          # GPU ETL pipeline (identical logic, RAPIDS session)
    run_benchmark.py    # Orchestrator (--mode cpu|gpu|both --scale N)
  qualification/
    run_cpu_with_logging.py  # CPU ETL + event log → manifest.json
    run_qualification.py     # spark_rapids qualification + CSV parse
    validate.py              # GPU ETL + actual vs predicted comparison
  agent-studio/
    DESIGN.md           # 4-agent workflow design + YAML template
```

Original reference folders (DO NOT DELETE):
  spark-rapids-etl/
  spark-rapids-qualification-tool/
  spark-rapids-ml/
