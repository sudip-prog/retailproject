from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.schema_validator import validate_schema
from utils.audit_logger import log_audit
from utils.data_quality import run_data_quality_checks
from utils.scd2 import apply_scd2
from schema.schema_metadata import TABLE_SCHEMAS

logger = get_logger("products_silver")

def process_products():
    try:
        spark = SparkSession.builder.appName("ProductsSilver").getOrCreate()
        table_config = TABLE_SCHEMAS['products']

        logger.info("Reading bronze data...")
        df = spark.read.format("delta").load(table_config['bronze_path'])

        logger.info("Validating schema...")
        df = validate_schema(df, table_config['columns'])

        logger.info("Applying SCD2 logic...")
        df_final = apply_scd2(df, table_config['silver_path'], table_config['primary_keys'])

        logger.info("Running data quality checks...")
        dq_results = run_data_quality_checks(df_final, table_config)

        logger.info("Writing to silver table...")
        df_final.write.format("delta").mode("overwrite").save(table_config['silver_path'])

        log_audit(table_name="products", status="success", dq_results=dq_results)

    except Exception as e:
        logger.error(f"Error in processing products: {str(e)}", exc_info=True)
        log_audit(table_name="products", status="failed", error=str(e))
