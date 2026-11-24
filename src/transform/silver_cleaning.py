"""
Transformation Module (Bronze -> Silver): Data cleaning and standardization.
Reads the Bronze Delta table, drops unnecessary raw columns, standardizes column names
to English snake_case, casts data types with decimal precision, and deduplicates records.
"""
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import DoubleType, IntegerType, StringType

from config.settings import (
    BRONZE_DIR,
    SILVER_DIR,
    DROP_COLUMNS,
    RENAME_COLUMNS,
    NUMERIC_SCORE_COLUMNS,
    INTEGER_CODE_COLUMNS,
    STRING_COLUMNS,
    TABLE_FORMAT,
)
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Transform-Silver")


def clean_and_transform_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Applies Silver layer transformation rules:
    1. Drop raw/unused columns (answers, answer keys, survey questions).
    2. Rename columns to standardized English snake_case.
    3. Cast types (replace comma with dot for Double score columns).
    4. Deduplicate by registration_id.
    5. Filter out invalid/empty records.
    """
    logger.info("Starting Silver transformations and cleaning...")

    # 1. Drop unused raw columns
    cols_to_drop = [c for c in DROP_COLUMNS if c in df_bronze.columns]
    logger.info(f"Dropping {len(cols_to_drop)} raw columns...")
    df = df_bronze.drop(*cols_to_drop)

    # 2. Rename columns to clean English snake_case
    cols_to_rename = {k: v for k, v in RENAME_COLUMNS.items() if k in df.columns}
    logger.info(f"Renaming {len(cols_to_rename)} columns...")
    df = df.withColumnsRenamed(cols_to_rename)

    # 3. Cast data types
    logger.info("Casting data types (String, Integer, Double)...")

    # Strings
    for c in STRING_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)).cast(StringType()))

    # Integer codes / flags
    for c in INTEGER_CODE_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    # Scores (replace comma with dot if needed, cast to Double)
    for c in NUMERIC_SCORE_COLUMNS:
        if c in df.columns:
            df = df.withColumn(
                c,
                regexp_replace(col(c), ",", ".").cast(DoubleType()),
            )

    # 4. Data quality & deduplication
    if "registration_id" in df.columns:
        logger.info("Applying quality check: removing null/empty registrations and duplicates...")
        initial_count = df.count()
        df = df.filter(col("registration_id").isNotNull() & (trim(col("registration_id")) != ""))
        df = df.dropDuplicates(["registration_id"])
        final_count = df.count()
        logger.info(f"Deduplication completed: {initial_count} -> {final_count} rows.")

    return df


def process_bronze_to_silver(
    spark: SparkSession,
    bronze_input_dir: Optional[Path] = None,
    silver_output_dir: Optional[Path] = None,
    table_format: str = TABLE_FORMAT,
    mode: str = "overwrite",
) -> DataFrame:
    """
    Executes the Bronze to Silver transformation pipeline.

    Args:
        spark: Active SparkSession.
        bronze_input_dir: Path to the Bronze Delta table.
        silver_output_dir: Destination path for the Silver Delta table.
        table_format: Storage table format ('delta' or 'parquet').
        mode: Write mode ('overwrite' or 'append').

    Returns:
        Silver DataFrame.
    """
    bronze_path = bronze_input_dir or (BRONZE_DIR / "enem_raw")
    silver_path = silver_output_dir or (SILVER_DIR / "enem_cleaned")

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"Bronze table not found at: {bronze_path}. "
            "Please run the Bronze ingestion step first."
        )

    logger.info(f"=== PROCESSING SILVER LAYER (Source: {bronze_path}) ===")

    # Read Bronze Delta table
    logger.info(f"Reading Bronze table [{table_format.upper()}]...")
    df_bronze = spark.read.format(table_format).load(str(bronze_path))

    # Apply cleaning and schema transformations
    df_silver = clean_and_transform_silver(df_bronze)

    # Write Silver table partitioned by year and exam_state
    logger.info(f"Saving Silver table [{table_format.upper()}]: {silver_path}")
    writer = df_silver.write.format(table_format).mode(mode)

    partition_cols = [p for p in ["year", "exam_state"] if p in df_silver.columns]
    if partition_cols:
        logger.info(f"Partitioning Silver table by: {partition_cols}")
        writer = writer.partitionBy(*partition_cols)

    writer.save(str(silver_path))

    logger.info(f"Silver layer generated successfully at: {silver_path}")
    return df_silver


if __name__ == "__main__":
    spark_session = get_spark_session(app_name="Silver-Cleaning")
    process_bronze_to_silver(spark_session)
