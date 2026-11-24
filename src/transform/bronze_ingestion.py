"""
Transformation Module (Landing -> Bronze): Ingestion into the Bronze layer.
Reads the raw CSV from the Landing Zone and writes it as a Delta Lake table,
preserving 100% of the raw schema and adding metadata columns.
"""
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name

from config.settings import (
    LANDING_DIR,
    BRONZE_DIR,
    TABLE_FORMAT,
)
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Transform-Bronze")


def add_audit_metadata(df: DataFrame) -> DataFrame:
    """
    Enriches the DataFrame with ingestion metadata:
    - _ingestion_timestamp: Timestamp when the row was ingested into Bronze.
    - _source_file: Original file path of the record.
    """
    return (
        df.withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )


def ingest_landing_to_bronze(
    spark: SparkSession,
    csv_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    table_format: str = TABLE_FORMAT,
    mode: str = "overwrite",
) -> DataFrame:
    """
    Reads CSV from the Landing Zone and writes to the Bronze Delta table.

    Args:
        spark: Active SparkSession.
        csv_path: Path to the CSV file in Landing Zone.
        output_dir: Destination directory for the Bronze table.
        table_format: Table storage format ('delta' or 'parquet').
        mode: Write mode ('overwrite' or 'append').

    Returns:
        Bronze DataFrame.
    """
    input_file = csv_path or (LANDING_DIR / "enem_2020" / "MICRODADOS_ENEM_2020.csv")
    target_path = output_dir or (BRONZE_DIR / "enem_raw")

    if not input_file.exists():
        raise FileNotFoundError(
            f"Source file not found in Landing Zone: {input_file}. "
            "Please run the extraction step first."
        )

    logger.info(f"=== INGESTING INTO BRONZE LAYER (Source: {input_file}) ===")

    # Read raw CSV with semicolon delimiter and ISO-8859-1 encoding
    logger.info("Reading raw CSV with delimiter ';' and encoding ISO-8859-1...")
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("inferSchema", "false")
        .load(str(input_file))
    )

    total_rows = df_raw.count()
    total_cols = len(df_raw.columns)
    logger.info(f"Raw data loaded: {total_rows} rows and {total_cols} columns.")

    # Add ingestion metadata
    df_bronze = add_audit_metadata(df_raw)

    # Save as Delta Lake table partitioned by NU_ANO if present
    logger.info(f"Writing Bronze table [{table_format.upper()}] to: {target_path}")

    writer = df_bronze.write.format(table_format).mode(mode)

    if "NU_ANO" in df_bronze.columns:
        writer = writer.partitionBy("NU_ANO")

    writer.save(str(target_path))

    logger.info(f"Bronze layer completed successfully! Saved to: {target_path}")
    return df_bronze


if __name__ == "__main__":
    spark_session = get_spark_session(app_name="Bronze-Ingestion")
    ingest_landing_to_bronze(spark_session)
