from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit
from pyspark.sql.functions import col, to_date, sum as _sum

logger = get_logger("transactions_gold")

def run_transactions_gold(spark):
    try:
        logger.info("Starting transactions_gold transformation")

        transactions_df = spark.read.format("delta").table("retail_catalog.silver.transactions")

        gold_df = transactions_df.withColumn("tx_date", to_date("transaction_date")) \
            .groupBy("tx_date") \
            .agg(
                _sum("amount").alias("total_revenue"),
                _sum("quantity").alias("total_quantity")
            )

        gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_catalog.gold.daily_revenue")

        log_audit(spark, "transactions_gold", "SUCCESS", "Transactions Gold transformation successful")
        logger.info("transactions_gold transformation complete")

    except Exception as e:
        logger.error(f"Error in transactions_gold: {str(e)}")
        log_audit(spark, "transactions_gold", "FAILED", str(e))
        raise
