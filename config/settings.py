"""
Centralized settings for the ENEM ETL pipeline using PySpark and Delta Lake.
"""
from pathlib import Path

# Base project directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Medallion Architecture + Landing Zone directories
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Sample dataset (for quick local testing without downloading ~3GB)
SAMPLE_DATA_PATH = BASE_DIR / "sample_data" / "sample_enem_2020.csv"

# Official INEP URL for ENEM 2020 microdata download
INEP_ENEM_2020_URL = "https://download.inep.gov.br/microdados/microdados_enem_2020.zip"

# Default table format for the Lakehouse ('delta' or 'parquet')
TABLE_FORMAT = "delta"

# Spark configuration
SPARK_CONFIG = {
    "appName": "ETL-ENEM-Medallion-DeltaLake",
    "master": "local[*]",
    "driver_memory": "4g",
    "shuffle_partitions": "4",  # Optimized for local development
    "delta_package": "io.delta:delta-spark_2.12:3.2.0",
}

# Raw columns to drop in the Silver layer (test answers, answer keys, survey questions)
DROP_COLUMNS = (
    # Exam answers
    "TX_RESPOSTAS_CN",
    "TX_RESPOSTAS_CH",
    "TX_RESPOSTAS_LC",
    "TX_RESPOSTAS_MT",
    # Answer keys
    "TX_GABARITO_CN",
    "TX_GABARITO_CH",
    "TX_GABARITO_LC",
    "TX_GABARITO_MT",
    # Socioeconomic questionnaire (Q001 to Q025)
    "Q001", "Q002", "Q003", "Q004", "Q005",
    "Q006", "Q007", "Q008", "Q009", "Q010",
    "Q011", "Q012", "Q013", "Q014", "Q015",
    "Q016", "Q017", "Q018", "Q019", "Q020",
    "Q021", "Q022", "Q023", "Q024", "Q025",
)

# Column renaming map: Raw INEP columns -> Clean English snake_case
RENAME_COLUMNS = {
    "NU_INSCRICAO": "registration_id",
    "NU_ANO": "year",
    "TP_FAIXA_ETARIA": "age_group",
    "TP_SEXO": "gender",
    "TP_COR_RACA": "race",
    "TP_ST_CONCLUSAO": "high_school_status",
    "TP_ANO_CONCLUIU": "high_school_completion_year",
    "TP_ESCOLA": "school_type",
    "TP_ENSINO": "education_type",
    "IN_TREINEIRO": "is_trainee",
    "SG_UF_PROVA": "exam_state",
    "TP_PRESENCA_CN": "presence_natural_sciences",
    "TP_PRESENCA_CH": "presence_humanities",
    "TP_PRESENCA_LC": "presence_languages",
    "TP_PRESENCA_MT": "presence_math",
    "NU_NOTA_CN": "score_natural_sciences",
    "NU_NOTA_CH": "score_humanities",
    "NU_NOTA_LC": "score_languages",
    "NU_NOTA_MT": "score_math",
    "TP_LINGUA": "foreign_language",
    "TP_STATUS_REDACAO": "essay_status",
    "NU_NOTA_COMP1": "essay_comp1",
    "NU_NOTA_COMP2": "essay_comp2",
    "NU_NOTA_COMP3": "essay_comp3",
    "NU_NOTA_COMP4": "essay_comp4",
    "NU_NOTA_COMP5": "essay_comp5",
    "NU_NOTA_REDACAO": "score_essay",
}

# Numeric score columns (cast to Double to preserve decimal precision)
NUMERIC_SCORE_COLUMNS = [
    "score_natural_sciences",
    "score_humanities",
    "score_languages",
    "score_math",
    "essay_comp1",
    "essay_comp2",
    "essay_comp3",
    "essay_comp4",
    "essay_comp5",
    "score_essay",
]

# Integer code / flag columns
INTEGER_CODE_COLUMNS = [
    "year",
    "age_group",
    "race",
    "high_school_status",
    "high_school_completion_year",
    "school_type",
    "education_type",
    "is_trainee",
    "presence_natural_sciences",
    "presence_humanities",
    "presence_languages",
    "presence_math",
    "foreign_language",
    "essay_status",
]

# String columns
STRING_COLUMNS = [
    "registration_id",
    "gender",
    "exam_state",
]
