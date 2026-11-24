"""
Load Module (Silver -> Gold): Curated Business Layer & Aggregations.
Reads the Silver Delta table and generates business data marts
for BI dashboards, reporting, and downstream analysis.
"""
from pathlib import Path
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    round as spark_round,
    when,
    coalesce,
    stddev,
    lit,
)

from config.settings import (
    SILVER_DIR,
    GOLD_DIR,
    TABLE_FORMAT,
)
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("Load-Gold")


def build_gold_performance_by_state(df_silver: DataFrame) -> DataFrame:
    """
    Builds the academic performance data mart grouped by State (exam_state) and Year.
    Calculates average scores across subjects and participation metrics.
    """
    logger.info("Building Data Mart: Performance by State...")

    df_state = (
        df_silver.groupBy("year", "exam_state")
        .agg(
            count("registration_id").alias("total_enrolled"),
            count(
                when(
                    (col("presence_natural_sciences") == 1)
                    & (col("presence_humanities") == 1)
                    & (col("presence_languages") == 1)
                    & (col("presence_math") == 1),
                    True,
                )
            ).alias("total_present_all_days"),
            spark_round(avg("score_natural_sciences"), 2).alias("avg_natural_sciences"),
            spark_round(avg("score_humanities"), 2).alias("avg_humanities"),
            spark_round(avg("score_languages"), 2).alias("avg_languages"),
            spark_round(avg("score_math"), 2).alias("avg_math"),
            spark_round(avg("score_essay"), 2).alias("avg_essay"),
            spark_round(stddev("score_essay"), 2).alias("stddev_essay"),
            spark_round(
                (
                    coalesce(avg("score_natural_sciences"), lit(0.0))
                    + coalesce(avg("score_humanities"), lit(0.0))
                    + coalesce(avg("score_languages"), lit(0.0))
                    + coalesce(avg("score_math"), lit(0.0))
                    + coalesce(avg("score_essay"), lit(0.0))
                )
                / 5.0,
                2,
            ).alias("overall_avg_score"),
        )
        .orderBy("year", col("overall_avg_score").desc())
    )

    return df_state


def build_gold_performance_by_school(df_silver: DataFrame) -> DataFrame:
    """
    Builds the comparative data mart by School Type (Public vs Private) and Gender.
    """
    logger.info("Building Data Mart: Performance by School Type...")

    # Map readable descriptions for school types
    df_school = df_silver.withColumn(
        "school_type_description",
        when(col("school_type") == 1, "Not Specified")
        .when(col("school_type") == 2, "Public")
        .when(col("school_type") == 3, "Private")
        .otherwise("Other"),
    )

    df_mart = (
        df_school.groupBy("year", "exam_state", "school_type_description", "gender")
        .agg(
            count("registration_id").alias("total_candidates"),
            spark_round(avg("score_essay"), 2).alias("avg_essay"),
            spark_round(avg("score_math"), 2).alias("avg_math"),
            spark_round(avg("score_natural_sciences"), 2).alias("avg_natural_sciences"),
            spark_round(avg("score_humanities"), 2).alias("avg_humanities"),
            spark_round(avg("score_languages"), 2).alias("avg_languages"),
        )
        .orderBy("year", "exam_state", "school_type_description")
    )

    return df_mart


def build_gold_fact_candidates(df_silver: DataFrame) -> DataFrame:
    """
    Builds the student-level curated fact table with composite average score
    and performance classification tier.
    """
    logger.info("Building Fact Table: Curated Candidates...")

    avg_score_expr = (
        coalesce(col("score_natural_sciences"), lit(0.0))
        + coalesce(col("score_humanities"), lit(0.0))
        + coalesce(col("score_languages"), lit(0.0))
        + coalesce(col("score_math"), lit(0.0))
        + coalesce(col("score_essay"), lit(0.0))
    ) / 5.0

    df_fact = (
        df_silver.withColumn("average_score", spark_round(avg_score_expr, 2))
        .withColumn(
            "performance_tier",
            when(col("average_score") >= 750, "Excellent")
            .when(col("average_score") >= 600, "Good")
            .when(col("average_score") >= 450, "Average")
            .otherwise("Below Average"),
        )
    )

    return df_fact


def process_silver_to_gold(
    spark: SparkSession,
    silver_input_dir: Optional[Path] = None,
    gold_output_dir: Optional[Path] = None,
    table_format: str = TABLE_FORMAT,
    mode: str = "overwrite",
) -> Dict[str, DataFrame]:
    """
    Executes the Silver to Gold pipeline, generating Delta Lake data marts.

    Args:
        spark: Active SparkSession.
        silver_input_dir: Path to the Silver Delta table.
        gold_output_dir: Root directory for the Gold layer.
        table_format: Storage table format ('delta' or 'parquet').
        mode: Write mode ('overwrite' or 'append').

    Returns:
        Dictionary of generated Gold DataFrames.
    """
    silver_path = silver_input_dir or (SILVER_DIR / "enem_cleaned")
    gold_root = gold_output_dir or GOLD_DIR

    if not silver_path.exists():
        raise FileNotFoundError(
            f"Silver table not found at: {silver_path}. "
            "Please run the Silver transformation step first."
        )

    logger.info(f"=== PROCESSING GOLD LAYER (Source: {silver_path}) ===")

    # Read Silver Delta table
    df_silver = spark.read.format(table_format).load(str(silver_path))

    # Generate Data Marts
    gold_marts = {
        "enem_performance_by_state": (
            build_gold_performance_by_state(df_silver),
            ["year"],
        ),
        "enem_performance_by_school": (
            build_gold_performance_by_school(df_silver),
            ["year"],
        ),
        "enem_fact_candidates": (
            build_gold_fact_candidates(df_silver),
            ["year", "exam_state"],
        ),
    }

    result_dfs = {}

    for table_name, (df_mart, partitions) in gold_marts.items():
        table_path = gold_root / table_name
        logger.info(f"Writing Gold table [{table_name}] to: {table_path}...")

        writer = df_mart.write.format(table_format).mode(mode)
        valid_partitions = [p for p in partitions if p in df_mart.columns]
        if valid_partitions:
            writer = writer.partitionBy(*valid_partitions)

        writer.save(str(table_path))
        result_dfs[table_name] = df_mart
        logger.info(f"Gold table [{table_name}] saved successfully ({df_mart.count()} rows).")

    logger.info("All Gold layer tables were generated and saved successfully!")
    return result_dfs


if __name__ == "__main__":
    spark_session = get_spark_session(app_name="Gold-Aggregation")
    process_silver_to_gold(spark_session)
