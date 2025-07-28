from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit
from utils.logger import get_logger

logger = get_logger("data_quality")

def run_data_quality_checks(df: DataFrame, primary_keys: list[str]) -> dict:
    results = {}

    # Check Null 
    nulls = {col_name: df.filter(col(col_name).isNull()).count() for col_name in df.columns}
    results["null_counts"] = nulls

    # Checking duplicate primary key 
    if primary_keys:
        pk_dupes = df.groupBy(primary_keys).count().filter(col("count") > 1).count()
        results["duplicate_primary_keys"] = pk_dupes

    # Total row count
    results["row_count"] = df.count()

    logger.info(f"Data Quality Results: {results}")
    return results
