from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from utils.logger import get_logger

logger = get_logger("scd1")

#Added SCD1 function for all tables check later if I can use SQL like merge functions 

def apply_scd1(spark, df: DataFrame, target_path: str, keys: list[str]):
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = DeltaTable.forPath(spark, target_path)
        updates = {col: f"source.{col}" for col in df.columns if col not in keys}
        cond = " AND ".join([f"target.{key} = source.{key}" for key in keys])

        target_table.alias("target") \
            .merge(df.alias("source"), cond) \
            .whenMatchedUpdate(set=updates) \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("SCD1 merge completed.")
    else:
        df.write.format("delta").mode("overwrite").save(target_path)
        logger.info("Initial load for SCD1 completed.")
