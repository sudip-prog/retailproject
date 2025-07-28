from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit

logger = get_logger("products_gold")

def run_products_gold(spark):
    try:
        logger.info("Starting products_gold transformation")

        products_df = spark.read.format("delta").table("retail_catalog.silver.products")

        # Sample KPI: Count of active products per category
        gold_df = products_df.filter("is_active = true").groupBy("category").count().withColumnRenamed("count", "active_products")

        gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_catalog.gold.active_products_by_category")

        log_audit(spark, "products_gold", "SUCCESS", "Products Gold transformation successful")
        logger.info("products_gold transformation complete")

    except Exception as e:
        logger.error(f"Error in products_gold: {str(e)}")
        log_audit(spark, "products_gold", "FAILED", str(e))
        raise
