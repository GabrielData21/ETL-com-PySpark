"""
CLI entry point to orchestrate the ENEM ETL pipeline.
Medallion Architecture: Landing Zone -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta).
"""
import argparse
import sys
import time
from typing import Optional

from config.settings import TABLE_FORMAT, LANDING_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR
from src.common.logger import get_logger
from src.extract.download_landing import extract_pipeline

logger = get_logger("ETL-Orchestrator")


def run_pipeline(
    step: str = "all",
    use_sample: bool = False,
    table_format: str = TABLE_FORMAT,
    remove_zip: bool = False,
) -> None:
    """
    Runs the selected step(s) of the Medallion pipeline.

    Args:
        step: Pipeline step ('extract', 'bronze', 'silver', 'gold', or 'all').
        use_sample: If True, uses the sample dataset for fast local testing.
        table_format: Lakehouse table format ('delta' or 'parquet').
        remove_zip: If True, removes the downloaded .zip file after extraction.
    """
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("STARTING ENEM ETL PIPELINE (MEDALLION LAKEHOUSE WITH DELTA LAKE)")
    logger.info(f"Step: [{step.upper()}] | Sample Mode: [{use_sample}] | Format: [{table_format.upper()}]")
    logger.info("=" * 70)

    # 1. EXTRACTION STEP (LANDING ZONE)
    if step in ("extract", "all"):
        logger.info("\n>>> [STEP 1/4] EXTRACT -> LANDING ZONE")
        extract_pipeline(use_sample=use_sample, remove_zip=remove_zip)

    # Initialize Spark for data processing steps
    if step in ("bronze", "silver", "gold", "all"):
        try:
            from src.common.spark_session import get_spark_session
            from src.transform.bronze_ingestion import ingest_landing_to_bronze
            from src.transform.silver_cleaning import process_bronze_to_silver
            from src.load.gold_aggregation import process_silver_to_gold
        except ImportError as e:
            logger.error(
                "Failed to import PySpark or Delta Lake dependencies. "
                "Ensure your virtual environment is active and dependencies are installed."
            )
            raise e

        spark = get_spark_session(
            app_name=f"ETL-ENEM-Medallion-{step.capitalize()}",
            enable_delta=(table_format.lower() == "delta"),
        )

        try:
            # 2. BRONZE STEP (LANDING -> BRONZE)
            if step in ("bronze", "all"):
                logger.info("\n>>> [STEP 2/4] TRANSFORM -> BRONZE LAYER (RAW LAKEHOUSE)")
                df_bronze = ingest_landing_to_bronze(spark, table_format=table_format)
                logger.info("Bronze layer sample:")
                df_bronze.select("NU_INSCRICAO", "NU_ANO", "SG_UF_PROVA", "_ingestion_timestamp").show(5, truncate=False)

            # 3. SILVER STEP (BRONZE -> SILVER)
            if step in ("silver", "all"):
                logger.info("\n>>> [STEP 3/4] TRANSFORM -> SILVER LAYER (CLEANED & CONFORMED)")
                df_silver = process_bronze_to_silver(spark, table_format=table_format)
                logger.info("Silver layer sample:")
                df_silver.select("registration_id", "year", "exam_state", "score_essay", "score_math").show(5, truncate=False)

            # 4. GOLD STEP (SILVER -> GOLD)
            if step in ("gold", "all"):
                logger.info("\n>>> [STEP 4/4] LOAD -> GOLD LAYER (CURATED DATA MARTS)")
                gold_tables = process_silver_to_gold(spark, table_format=table_format)

                logger.info("Preview of 'enem_performance_by_state':")
                gold_tables["enem_performance_by_state"].show(10, truncate=False)

        finally:
            spark.stop()
            logger.info("SparkSession stopped.")

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"PIPELINE COMPLETED SUCCESSFULLY! Total time: {elapsed:.2f} seconds.")
    logger.info("Generated Artifacts:")
    logger.info(f" - Landing Zone : {LANDING_DIR}")
    logger.info(f" - Bronze Lake  : {BRONZE_DIR / 'enem_raw'}")
    logger.info(f" - Silver Lake  : {SILVER_DIR / 'enem_cleaned'}")
    logger.info(f" - Gold Marts   : {GOLD_DIR}")
    logger.info("=" * 70)


def parse_arguments() -> argparse.Namespace:
    """Configures and parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="ENEM Microdata ETL Pipeline with PySpark, Delta Lake, and Medallion Architecture."
    )
    parser.add_argument(
        "--step",
        choices=["extract", "bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline step to run (default: all).",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Run using local sample dataset (instant testing without downloading ~3GB).",
    )
    parser.add_argument(
        "--format",
        choices=["delta", "parquet"],
        default="delta",
        help="Lakehouse table format (default: delta).",
    )
    parser.add_argument(
        "--remove-zip",
        action="store_true",
        help="Delete downloaded ZIP file after extraction to save disk space.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    try:
        run_pipeline(
            step=args.step,
            use_sample=args.sample,
            table_format=args.format,
            remove_zip=args.remove_zip,
        )
    except Exception as e:
        logger.exception(f"Pipeline execution failed: {e}")
        sys.exit(1)
