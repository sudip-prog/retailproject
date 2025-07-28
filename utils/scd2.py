from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import get_logger

logger = get_logger("scd2")

def apply_scd2(spark, df: DataFrame, target_path: str, keys: list[str]):
    current_df = df.withColumn("is_current", lit(True)).withColumn("updated_at", current_timestamp())

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)

        cond = " AND ".join([f"target.{key} = source.{key}" for key in keys])
        updates = { "is_current": lit(False), "updated_at": current_timestamp() }

        delta_table.alias("target") \
            .merge(current_df.alias("source"), cond + " AND target.is_current = true") \
            .whenMatchedUpdate(set=updates) \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("SCD2 merge completed.")
    else:
        current_df.write.format("delta").mode("overwrite").save(target_path)
        logger.info("Initial load for SCD2 completed.")
