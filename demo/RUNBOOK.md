# Demo Runbook

Operational guidance for running the Spark RAPIDS CPU vs GPU benchmark demo.

---

## Session Type: CPU vs GPU

### Rule of thumb

| What you want to run | Session to use |
|---|---|
| Datagen only | CPU or GPU session |
| `--mode cpu` benchmark only | CPU or GPU session |
| `--mode gpu` or `--mode both` | **GPU session required** |

### Why

CAI Workbench runs each session as a Kubernetes pod. A **GPU session** has the NVIDIA
device mounted into the pod (`NVIDIA_VISIBLE_DEVICES=<gpu-uuid>`). A **CPU session** has
`NVIDIA_VISIBLE_DEVICES=void` — the GPU device does not exist inside that pod at all.
CUDA cannot function regardless of what JARs are installed.

The benchmark scripts launch Spark via `subprocess.run()`. That subprocess runs inside the
**same container** as the session, so it inherits whatever GPU access the session has. In
`local[*]` mode (used when `DATA_STORAGE` is a local path like `/tmp`), Spark runs
everything in a single JVM — there are no separate executor pods and no GPU scheduling
conflict between the session and the benchmark process.

**Practical consequence:** you can run the full CPU + GPU benchmark from a single GPU
session without any resource conflicts:

```bash
python demo/benchmark/run_benchmark.py --mode both --scale 10
```

---

## Scale: Datagen and Benchmark Must Match

The `--scale` argument to `retail_datagen.py` controls **how many rows are written** to
disk. The `--scale` argument to `run_benchmark.py` controls **which results directory is
used** for output — it does NOT filter or resize the data. The ETL scripts always read
everything that is in `$DATA_STORAGE/retail/`.

This means: **the data on disk must match the scale you pass to the benchmark.**

### The data directory has no scale in its path

Both `retail_datagen.py --scale 1` and `retail_datagen.py --scale 10` write to the same
location:

```
$DATA_STORAGE/retail/sales/
$DATA_STORAGE/retail/stock/
...
```

Running datagen at a new scale **overwrites** whatever was there before.

### What happens if the scales don't match

If you generate at scale 10 but benchmark at scale 1:
- The ETL reads all 10M rows (scale 10 data)
- The results are labelled `scale1` in filenames and timing JSON
- Your benchmark output is mislabelled — it shows scale 1 timing for scale 10 data

If you generate at scale 1 but benchmark at scale 10:
- Same problem in reverse — scale 10 results for scale 1 data

### Correct workflow

Always regenerate data when changing scale:

```bash
# Generate at the scale you want to benchmark
python demo/datagen/retail_datagen.py --scale 10

# Then benchmark at the same scale
python demo/benchmark/run_benchmark.py --mode both --scale 10
```

Or in one step using `--generate-data`:

```bash
python demo/benchmark/run_benchmark.py --mode both --scale 10 --generate-data
```

### Switching from scale 10 to scale 5 (or any scale change)

There is no "scale 5 subset" of scale 10 data — you must regenerate:

```bash
# Regenerate at the new scale (overwrites the scale 10 data)
python demo/datagen/retail_datagen.py --scale 5

# Now benchmark at scale 5
python demo/benchmark/run_benchmark.py --mode both --scale 5
```

If you want to keep both scale 10 and scale 5 data available simultaneously, point each
run at a separate directory using `--output-dir` / `--data-dir` and set `DATA_STORAGE`
accordingly:

```bash
# Generate scale 10 into a dedicated path
DATA_STORAGE=/tmp/rapids-demo-scale10 python demo/datagen/retail_datagen.py --scale 10

# Generate scale 5 into a different path
DATA_STORAGE=/tmp/rapids-demo-scale5 python demo/datagen/retail_datagen.py --scale 5

# Benchmark each independently
DATA_STORAGE=/tmp/rapids-demo-scale10 python demo/benchmark/run_benchmark.py --mode both --scale 10
DATA_STORAGE=/tmp/rapids-demo-scale5  python demo/benchmark/run_benchmark.py --mode both --scale 5
```

---

## Quick Reference: Full Flow (Single GPU Session)

```bash
# 1. Set storage path (once per session, or set in CAI project env vars)
export DATA_STORAGE=/tmp/rapids-demo

# 2. Generate data
python demo/datagen/retail_datagen.py --scale 10

# 3. Run CPU benchmark
python demo/benchmark/run_benchmark.py --mode cpu --scale 10

# 4. Run GPU benchmark
python demo/benchmark/run_benchmark.py --mode gpu --scale 10

# — or steps 3+4 combined —
python demo/benchmark/run_benchmark.py --mode both --scale 10
```
