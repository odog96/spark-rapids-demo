"""
spark_config.py
---------------
Centralized SparkSession factory for the Spark RAPIDS benchmark demo.

Supports CPU (vanilla) and GPU (RAPIDS-accelerated) sessions, with
per-GPU-type tuning profiles that can be swapped via the GPU_TYPE env var.

Environment variables (all optional, sensible defaults provided):
  DATA_STORAGE       Base path for data (local or s3a://bucket/prefix)
  CONNECTION_NAME    CML Data Connection name (only needed for CML-managed sessions)
  PROJECT_OWNER      Username / project owner (used in output paths)
  GPU_TYPE           One of: A10G (default), T4, A100

Cost reference (AWS on-demand, us-east-1, 2024):
  CPU baseline  m5.4xlarge  : $0.768 /hr  (16 vCPU, 64 GB RAM)
  GPU           g5.4xlarge  : $1.624 /hr  (16 vCPU, 64 GB RAM, NVIDIA A10G 24 GB)
  Break-even speedup: ~2.1x  (GPU job cheaper per unit work above this threshold)
"""

import os
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Per-GPU tuning profiles
# ---------------------------------------------------------------------------
# Each profile maps Spark config key -> value, targeting a specific GPU type.
# Swap GPU_TYPE env var to switch profiles without changing any benchmark code.

GPU_PROFILES = {
    "A10G": {
        # NVIDIA A10G – 24 GB VRAM, paired with g5.4xlarge (16 vCPU, 64 GB RAM)
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "4g",
        "spark.rapids.memory.pinnedPool.size": "4g",
        "spark.rapids.sql.concurrentGpuTasks": "2",
        "spark.sql.files.maxPartitionBytes": "256m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "512m",
        # GPU resource scheduling
        "spark.executor.resource.gpu.amount": "1",
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.dynamicAllocation.enabled": "false",
    },
    "T4": {
        # NVIDIA T4 – 16 GB VRAM, typically paired with g4dn instance family
        "spark.executor.memory": "12g",
        "spark.executor.memoryOverhead": "3g",
        "spark.rapids.memory.pinnedPool.size": "2g",
        "spark.rapids.sql.concurrentGpuTasks": "1",
        "spark.sql.files.maxPartitionBytes": "128m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
        "spark.executor.resource.gpu.amount": "1",
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.dynamicAllocation.enabled": "false",
    },
    "A100": {
        # NVIDIA A100 – 40/80 GB VRAM, typically p4d/p4de instance family
        "spark.executor.memory": "24g",
        "spark.executor.memoryOverhead": "6g",
        "spark.rapids.memory.pinnedPool.size": "8g",
        "spark.rapids.sql.concurrentGpuTasks": "4",
        "spark.sql.files.maxPartitionBytes": "512m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1g",
        "spark.executor.resource.gpu.amount": "1",
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.dynamicAllocation.enabled": "false",
    },
}

# RAPIDS plugin configs shared across all GPU types
#
# JAR: rapids-4-spark_2.12-24.08.0-cuda12.jar
#   - 24.08.0 is the last RAPIDS release to support Spark 3.3.x
#   - cuda12 variant required (CUDA 12.5 toolkit on g5.4xlarge CAI image)
#   - Download: https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/
#               24.08.0/rapids-4-spark_2.12-24.08.0-cuda12.jar
#   - Place at: /home/cdsw/jars/rapids-4-spark_2.12-24.08.0-cuda12.jar
#     (or override via RAPIDS_JAR env var)
_RAPIDS_JAR = os.environ.get(
    "RAPIDS_JAR",
    "/home/cdsw/jars/rapids-4-spark_2.12-24.08.0-cuda12.jar",
)

_RAPIDS_CORE_CONFIGS = {
    "spark.plugins": "com.nvidia.spark.SQLPlugin",
    "spark.jars": _RAPIDS_JAR,
    # Force the 330cdh shim so RAPIDS accepts Cloudera hotfix build strings like
    # 3.3.0.1.24.7216.0-76 that don't match the exact version patterns baked into
    # the JAR's auto-detection logic.
    "spark.rapids.shims-provider-override": (
        "com.nvidia.spark.rapids.shims.spark330cdh.SparkShimServiceProvider"
    ),
    "spark.rapids.sql.enabled": "true",
    "spark.rapids.sql.incompatibleOps.enabled": "true",
    "spark.rapids.sql.variableFloatAgg.enabled": "true",
    "spark.rapids.sql.castFloatToString.enabled": "true",
    "spark.rapids.sql.castStringToFloat.enabled": "true",
    "spark.rapids.sql.format.csv.enabled": "true",
    "spark.rapids.sql.format.csv.read.enabled": "true",
    "spark.rapids.sql.format.json.read.enabled": "true",
    "spark.rapids.sql.castStringToTimestamp.enabled": "true",
    "spark.rapids.sql.castDecimalToString.enabled": "true",
    "spark.kryo.registrator": "com.nvidia.spark.rapids.GpuKryoRegistrator",
    # Performance
    "spark.locality.wait": "0",
    "spark.sql.adaptive.enabled": "true",
    # GPU executor discovery script (confirmed path on CAI Workbench Spark 3.3.0 runtime)
    "spark.executor.resource.gpu.discoveryScript": "/runtime-addons/spark330-24.1-h1-ga3qav/opt/spark/examples/src/main/scripts/getGpusResources.sh",
    "spark.executor.resource.gpu.vendor": "nvidia.com",
}

# CPU session tuning (no GPU, no RAPIDS)
#
# NOTE: In local[*] mode (used when DATA_STORAGE is a local path), the Spark
# driver and executor share the same JVM process, so only spark.driver.memory
# applies. Set SPARK_DRIVER_MEMORY to override — use 4g or less for sessions
# with 8 GB RAM, 8g+ for sessions with 16 GB+ RAM.
_SPARK_DRIVER_MEM = os.environ.get("SPARK_DRIVER_MEMORY", "10g")

_CPU_CONFIGS = {
    "spark.executor.memory": "16g",       # ignored in local mode
    "spark.executor.memoryOverhead": "2g", # ignored in local mode
    "spark.executor.cores": "4",           # ignored in local mode
    "spark.driver.memory": _SPARK_DRIVER_MEM,
    "spark.dynamicAllocation.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "256m",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
}


def _storage_config() -> dict:
    """Build S3/HDFS access configs from env vars (no-op for local paths)."""
    storage = os.environ.get("DATA_STORAGE", "")
    if storage.startswith("s3a://"):
        return {"spark.kerberos.access.hadoopFileSystems": storage}
    return {}


def _master_config() -> dict:
    """
    Return {"spark.master": "local[*]"} when running against a local data path.

    On distributed clusters (YARN, CDE, k8s) data lives in S3/HDFS, so we
    don't override the master and let spark-defaults.conf take effect.
    For local /tmp testing the executors must run in-process so they share
    the driver's filesystem — otherwise writes go to ephemeral executor pods
    and only the _SUCCESS marker survives.
    """
    storage = os.environ.get("DATA_STORAGE", "/tmp/rapids-demo")
    if storage.startswith("s3a://") or storage.startswith("hdfs://"):
        return {}
    return {"spark.master": os.environ.get("SPARK_MASTER", "local[*]")}


def get_cpu_session(app_name: str = "SparkRAPIDS-CPU-Benchmark",
                    extra_configs: dict = None) -> SparkSession:
    """
    Return a vanilla (CPU-only) SparkSession for benchmarking.

    Parameters
    ----------
    app_name : str
        Spark application name shown in the UI.
    extra_configs : dict, optional
        Additional key/value pairs merged on top of the CPU defaults.

    Returns
    -------
    SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    configs = {**_master_config(), **_CPU_CONFIGS, **_storage_config()}
    if extra_configs:
        configs.update(extra_configs)
    for k, v in configs.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def get_gpu_session(app_name: str = "SparkRAPIDS-GPU-Benchmark",
                    gpu_type: str = None,
                    event_log_dir: str = None,
                    extra_configs: dict = None) -> SparkSession:
    """
    Return a RAPIDS-accelerated SparkSession tuned for the target GPU.

    Parameters
    ----------
    app_name : str
        Spark application name shown in the UI.
    gpu_type : str, optional
        Override for GPU_TYPE env var.  Must be one of A10G, T4, A100.
    event_log_dir : str, optional
        If set, enables Spark event logging to this directory.
    extra_configs : dict, optional
        Additional key/value pairs merged last (highest priority).

    Returns
    -------
    SparkSession
    """
    gpu = (gpu_type or os.environ.get("GPU_TYPE", "A10G")).upper()
    if gpu not in GPU_PROFILES:
        raise ValueError(f"Unknown GPU_TYPE '{gpu}'. Choose from: {list(GPU_PROFILES)}")

    profile = GPU_PROFILES[gpu]
    configs = {
        **_master_config(),
        **_RAPIDS_CORE_CONFIGS,
        **profile,
        **_storage_config(),
        "spark.driver.memory": _SPARK_DRIVER_MEM,
        "spark.sql.adaptive.enabled": "true",
    }

    if event_log_dir:
        configs["spark.eventLog.enabled"] = "true"
        configs["spark.eventLog.dir"] = event_log_dir

    if extra_configs:
        configs.update(extra_configs)

    builder = SparkSession.builder.appName(app_name)
    for k, v in configs.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def get_session_for_mode(mode: str, app_name: str = None,
                         event_log_dir: str = None,
                         extra_configs: dict = None) -> SparkSession:
    """
    Convenience wrapper: returns a CPU or GPU session based on `mode`.

    Parameters
    ----------
    mode : str
        'cpu' or 'gpu'
    app_name : str, optional
    event_log_dir : str, optional
        GPU sessions support event logging; ignored for CPU.
    extra_configs : dict, optional

    Returns
    -------
    SparkSession
    """
    mode = mode.lower()
    if mode == "cpu":
        name = app_name or "SparkRAPIDS-CPU-Benchmark"
        return get_cpu_session(name, extra_configs)
    elif mode == "gpu":
        name = app_name or "SparkRAPIDS-GPU-Benchmark"
        return get_gpu_session(name, event_log_dir=event_log_dir,
                               extra_configs=extra_configs)
    else:
        raise ValueError(f"mode must be 'cpu' or 'gpu', got '{mode}'")
