from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.schema_validator import validate_schema
from utils.audit_logger import log_audit
from utils.data_quality import run_data_quality_checks
from utils.scd2 import apply_scd2
from utils.schema_metadata import silver_table_metadata

logger = get_logger("products_silver")

def process_products():
    try:
        spark = SparkSession.builder.appName("ProductsSilver").getOrCreate()
        table_conf = silver_table_metadata["products"]
        source_path = table_conf["source_path"]
        target_path = table_conf["target_path"]
        schema = table_conf["schema"]
        primary_keys = table_conf["primary_keys"]
        
        logger.info("Reading bronze data...")
        df = spark.read.format("delta").load(table_config['bronze_path'])

        logger.info("Removing unwanted columns...")
        df.drop("_rescued_data","_ingest_time","_source_file")

        logger.info("Validating schema...")

        if not validate_schema(df, schema):
            log_audit(spark, "products", "failed", "Schema validation failed")
            return

        
        dq_metrics = run_data_quality_checks(df, primary_keys)

        logger.info("Applying SCD2 logic and writing data to silver tables")

        apply_scd2(spark, df, target_path, primary_keys)
        
        log_audit(spark, "products", "success", "Loaded with SCD2", dq_metrics.get("row_count", 0))

        
    except Exception as e:
        logger.error(f"Error in processing products: {str(e)}", exc_info=True)
        log_audit(spark, "products", "failed", str(e))
