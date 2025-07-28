from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit
from utils.schema_validator import validate_schema
from schema_metadata import SCHEMA_METADATA

logger = get_logger("customers_gold")

def run_customers_gold(spark):
    try:
        logger.info("Starting customers_gold transformation")

        # Load Silver data
        customers_df = spark.read.format("delta").table("retail_catalog.silver.customers")

        # Example Transformation: Count of customers by city
        customers_gold_df = customers_df.groupBy("city").count().withColumnRenamed("count", "customer_count")

        customers_gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_catalog.gold.customers_by_city")

        log_audit(spark, "customers_gold", "SUCCESS", "Gold transformation successful")
        logger.info("customers_gold transformation completed")
    
    except Exception as e:
        logger.error(f"Error in customers_gold: {str(e)}")
        log_audit(spark, "customers_gold", "FAILED", str(e))
        raise
