from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit
from utils.schema_validator import validate_schema
from utils.scd1 import apply_scd1
from utils.data_quality import run_data_quality_checks
from schema_metadata import silver_table_metadata

logger = get_logger("customers_silver")

def process_customers():
    spark = SparkSession.builder.appName("CustomersSilver").getOrCreate()

    table_conf = silver_table_metadata["customers"]
    source_path = table_conf["source_path"]
    target_path = table_conf["target_path"]
    schema = table_conf["schema"]
    primary_keys = table_conf["primary_keys"]

    try:
        df = spark.read.format("delta").load(source_path)
        logger.info("Loaded customers data")

        if not validate_schema(df, schema):
            log_audit(spark, "customers", "failed", "Schema validation failed")
            return

        dq_metrics = run_data_quality_checks(df, primary_keys)

        apply_scd1(spark, df, target_path, primary_keys)
        log_audit(spark, "customers", "success", "Loaded with SCD1", dq_metrics.get("row_count", 0))

    except Exception as e:
        logger.exception("Error processing customers table")
        log_audit(spark, "customers", "failed", str(e))

# if __name__ == "__main__":
#     process_customers()
