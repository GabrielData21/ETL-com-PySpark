"""
SparkSession builder with Delta Lake support and local optimizations.
"""
import os
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from config.settings import SPARK_CONFIG, BASE_DIR
from src.common.logger import get_logger

logger = get_logger("SparkSessionManager")

# Ensure compatible Java LTS (Java 17) is used if available
local_java = Path.home() / ".local/share/jvm/jdk-17"
if local_java.exists():
    os.environ["JAVA_HOME"] = str(local_java)
    os.environ["PATH"] = f"{local_java}/bin:{os.environ.get('PATH', '')}"


def get_spark_session(
    app_name: Optional[str] = None,
    enable_delta: bool = True,
    shuffle_partitions: Optional[str] = None,
) -> SparkSession:
    """
    Creates or retrieves a SparkSession configured for Medallion architecture with Delta Lake.

    Args:
        app_name: Spark application name (defaults to settings.py).
        enable_delta: Enables Delta Lake extensions and catalog (default: True).
        shuffle_partitions: Number of shuffle partitions for joins and aggregations.

    Returns:
        Active SparkSession.
    """
    name = app_name or SPARK_CONFIG["appName"]
    partitions = shuffle_partitions or SPARK_CONFIG["shuffle_partitions"]
    warehouse_dir = str(BASE_DIR / "spark-warehouse")

    builder = (
        SparkSession.builder.appName(name)
        .master(SPARK_CONFIG["master"])
        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.repl.eagerEval.enabled", "true")
    )

    if enable_delta:
        # Delta Lake catalog and extension configuration
        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.jars.packages",
                SPARK_CONFIG["delta_package"],
            )
        )

        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
            logger.info("Delta Lake successfully configured via delta-spark.")
        except ImportError:
            logger.warning(
                "Module 'delta' not found via pip. "
                "Using standard Spark Maven package configurations."
            )

    spark = builder.getOrCreate()

    # Reduce JVM Spark log noise to WARN
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession started [v{spark.version}] - App: {name}")
    return spark
