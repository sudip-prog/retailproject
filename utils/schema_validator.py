from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from utils.logger import get_logger

logger = get_logger("schema_validator")


def validate_schema(df: DataFrame, expected_schema: dict) -> bool:
    df_fields = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    #expected_fields = {k: v for k, v in expected_schema.items()}
    expected_fields = {field.name: field.dataType.simpleString() for field in expected_schema.fields}

    missing_cols = set(expected_fields.keys()) - set(df_fields.keys())
    mismatched_types = {col: (df_fields[col], expected_fields[col]) for col in df_fields if col in expected_fields and df_fields[col] != expected_fields[col]}

    if missing_cols or mismatched_types:
        logger.error(f"Schema validation failed. Missing columns: {missing_cols}, Mismatched types: {mismatched_types}")
        return False
    logger.info("Schema validation passed.")
    return True
