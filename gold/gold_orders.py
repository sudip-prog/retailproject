from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit
from pyspark.sql.functions import col, to_date, countDistinct

logger = get_logger("orders_gold")

def run_orders_gold(spark):
    try:
        logger.info("Starting orders_gold transformation")

        orders_df = spark.read.format("delta").table("retail_catalog.silver.orders")

        # KPI: Daily order count and distinct customer count
        gold_df = orders_df.withColumn("order_date", to_date("order_date")) \
            .groupBy("order_date") \
            .agg(
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("order_id").alias("total_orders")
            )

        gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_catalog.gold.daily_orders")

        log_audit(spark, "orders_gold", "SUCCESS", "Orders Gold transformation successful")
        logger.info("orders_gold transformation complete")

    except Exception as e:
        logger.error(f"Error in orders_gold: {str(e)}")
        log_audit(spark, "orders_gold", "FAILED", str(e))
        raise
