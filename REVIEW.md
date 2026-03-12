# Code Review – demo/ Python Files
_Reviewed: 2026-03-12_

---

## Summary

One **critical runtime bug** was found and fixed (ambiguous `price` column).
All other files are logically correct and the qualification flow chains cleanly end-to-end.

---

## Files Reviewed

### `demo/utils/spark_config.py`
**Status: CORRECT**

- `get_cpu_session()` and `get_gpu_session()` factory pattern is sound.
- GPU profile configs merge correctly: `_RAPIDS_CORE_CONFIGS` → `GPU_PROFILES[gpu]` → `_storage_config()` → `extra_configs` (highest priority wins at each step).
- `_storage_config()` conditionally sets `spark.kerberos.access.hadoopFileSystems` only for `s3a://` paths — correct.
- `get_session_for_mode()` convenience wrapper routes correctly.
- `spark.dynamicAllocation.enabled=false` is correct for GPU sessions (required by RAPIDS).
- `spark.executor.resource.gpu.discoveryScript` path `/home/cdsw/getGpusResources.sh` — needs verification (open question #2).
- `spark.rapids.shims-provider-override` — may be ignored or error on newer RAPIDS builds; see open question #4.

---

### `demo/utils/timing.py`
**Status: CORRECT**

- `TimingHarness` context manager correctly records wall-clock time per stage.
- `total_elapsed_s` sums stage durations (not wall-clock end-to-end), which is the right choice for benchmarking individual stages.
- `compare_runs()` guards against `gpu_t == 0` before division. Correct.
- `print_comparison()` column format strings are consistent.
- No issues.

---

### `demo/benchmark/cpu_etl.py` and `demo/benchmark/gpu_etl.py`

#### Business Logic Parity
**Status: IDENTICAL (after fix applied)**

Careful line-by-line comparison of `run_etl()` in both files confirms:
- All 7 stages (load, clean, join, feature_eng, window, aggregate, write) are present and identical.
- All `withColumn`, `groupBy`, `agg`, `sort`, `join`, `Window` calls are character-for-character identical.
- Minor cosmetic comment differences only (expected — cpu file has more explanatory comments, gpu file stripped them).

#### Critical Bug Fixed: Ambiguous `price` column
**BUG (FIXED):** `retail_datagen.py` generates a `supplier` table with a `price` column.
`sales` also has a `price` column.  After `sales_df.join(supplier_df, "product_name", "leftouter")`,
the resulting DataFrame has **two columns both named `price`**.

Every downstream reference to `F.col("price")` (feature_eng, window, aggregate stages) would
throw `AnalysisException: Ambiguous reference to fields price`.

**Fix applied in both files** — clean stage, after date casts:
```python
# Rename supplier.price to avoid ambiguity with sales.price after join
supplier_df = supplier_df.withColumnRenamed("price", "supplier_price")
```

`supplier_price` is not referenced anywhere in the ETL pipeline logic (only
`quantity_ordered` from the supplier table is used), so this rename has no
downstream impact beyond resolving the ambiguity.

#### Other Notes
- `cpu_etl.py` `__main__`: `get_cpu_session(...)` — correct.
- `gpu_etl.py` `__main__`: `get_gpu_session(..., event_log_dir=args.event_log_dir)` — correct.
- `--timing-path` auto-generation uses different suffixes (`timing_cpu_scaleN.json` vs `timing_gpu_scaleN.json`) — correct.

---

### `demo/benchmark/run_benchmark.py`
**Status: CORRECT**

- `run_cpu()` passes `--timing-path` pointed at `{output_dir}/timing_cpu_scaleN.json`.
- `run_gpu()` passes `--timing-path` pointed at `{output_dir}/timing_gpu_scaleN.json`.
- `save_comparison()` calls `compare_runs(cpu_timing, gpu_timing)` with those exact paths. ✓
- Subprocess invocation uses `sys.executable` (correct — same Python environment).
- `check=True` propagates subprocess failures. ✓
- `run_datagen()` passes `--scale` and `--output-dir` to `retail_datagen.py`. ✓
- Banner string format: the `║` lines for `data` and `out` paths are width 43; very long paths
  will overflow and misalign the box. Cosmetic only, not a runtime issue.

---

### `demo/datagen/retail_datagen.py`
**Status: CORRECT (schemas verified against ETL)**

Schema produced vs. what ETL expects:

| Table | datagen columns | ETL uses |
|-------|----------------|----------|
| sales | sales_id, **product_name**, **price**, **quantity_sold**, **date_of_sale**, **customer_id** | all 6 used |
| stock | **product_name**, **shelf_life**, contains_promotion, **quantity_in_stock**, **location**, **date_received** | all except `contains_promotion` |
| supplier | sup_id, **product_name**, **quantity_ordered**, price→`supplier_price`, **date_ordered** | `quantity_ordered` used; `supplier_price` not used (renamed to avoid ambiguity) |
| market | **product_name**, **competitor_price**, sales_trend, **demand_forecast** | `competitor_price`, `demand_forecast` used |
| logistics | **product_name**, **shipping_cost**, **transportation_cost**, **warehouse_cost** | all 3 cost cols used |
| customer | **customer_id**, customer_name, **age**, **gender**, **purchase_history**, contact_info | `age`, `gender`, `purchase_history` used |

All columns used by the ETL are present in the datagen output. ✓

Customer ID join key alignment:
- `customer` table: `c_0` … `c_{n_customer-1}` (deterministic, from `id`)
- `sales` table: `c_0` … `c_{n_sales//1000 - 1}` (from `rand * n_sales//1000`)
- At scale 10x: customer has 10,000 IDs; sales references 0–9,999. ✓

---

### `demo/datagen/banking_datagen.py`
**Status: CORRECT (standalone, not used by ETL)**

- Not consumed by `cpu_etl.py` / `gpu_etl.py` — independent dataset.
- `drop("id")` removes the internal range column before writing. ✓
- Fraud label ~10% positive rate correctly modelled with `rand < 0.10`. ✓
- No issues.

---

### `demo/qualification/run_cpu_with_logging.py`
**Status: CORRECT**

- Imports `run_etl` from `benchmark/cpu_etl.py` via `sys.path.insert`. ✓
- Passes `extra_configs={"spark.eventLog.enabled": "true", ...}` to `get_cpu_session()`. ✓
- `manifest.json` writes `event_log_dir`, `data_dir`, `scale`, `cpu_timing_path`. ✓
- Written after `spark.stop()` (correct — Spark flushes event log on stop). ✓

---

### `demo/qualification/run_qualification.py`
**Status: CORRECT**

- CLI call uses `file://` prefix with `os.path.abspath()` — required by the tool for local paths. ✓
- CSV search uses `os.walk` with `"qualification_summary"` substring match — handles tool
  output directory nesting across versions. ✓
- Column name fallback list (`"Estimated GPU Speedup"`, `"estimatedGpuSpeedup"`, `"GPU Speedup"`)
  covers known tool version variants. ✓
- Saves `qualification_parsed.json` for consumption by `validate.py`. ✓

---

### `demo/qualification/validate.py`
**Status: CORRECT**

- Path chaining:
  - Reads CPU timing from `--cpu-timing` (default matches `run_cpu_with_logging.py` output). ✓
  - Reads qual JSON from `--qualification-json` (default matches `run_qualification.py` output). ✓
  - Writes `timing_gpu_scaleN.json` then calls `compare_runs(args.cpu_timing, gpu_timing_path)`. ✓
- `print_validation_report()` handles `predicted_speedup=None` gracefully. ✓
- `gpu_data = harness.to_dict()` used for gpu_total instead of re-reading the JSON — correct
  (the file was just written, no race condition). ✓

---

## Qualification Flow — End-to-End Path Chain

```
run_cpu_with_logging.py
  writes → $DATA_STORAGE/event-logs/            (event logs)
  writes → $DATA_STORAGE/results/cpu-logged/timing_cpu_logged_scale10.json
  writes → $DATA_STORAGE/results/cpu-logged/manifest.json

run_qualification.py --event-log-dir $DATA_STORAGE/event-logs
  reads  ← $DATA_STORAGE/event-logs/            ✓ matches above
  writes → $DATA_STORAGE/results/qualification/qualification_parsed.json

validate.py
  reads  ← $DATA_STORAGE/results/cpu-logged/timing_cpu_logged_scale10.json  ✓
  reads  ← $DATA_STORAGE/results/qualification/qualification_parsed.json      ✓
  writes → $DATA_STORAGE/results/gpu-validate/validation_report.json
```

All default paths chain correctly end-to-end. ✓

---

## Changes Made

| File | Change |
|------|--------|
| `demo/benchmark/cpu_etl.py` | Added `supplier_df.withColumnRenamed("price", "supplier_price")` in clean stage |
| `demo/benchmark/gpu_etl.py` | Same fix (mirrored to maintain parity) |
