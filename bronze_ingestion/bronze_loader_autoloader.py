# bronze_loader_autoloader.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def load_bronze_table(
    spark: SparkSession,
    table_name: str,
    raw_base_path: str,
    bronze_base_path: str,
    checkpoint_base_path: str,
    quarantine_base_path: str,
    file_format: str
):
    raw_path = f"{raw_base_path}/{table_name}"
    bronze_path = f"{bronze_base_path}/{table_name}"
    checkpoint_path = f"{checkpoint_base_path}/{table_name}"
    quarantine_path = f"{quarantine_base_path}/{table_name}"

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(raw_path)
        .withColumn("_ingest_time", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(bronze_path)
    )

    print(f"Started streaming load for {table_name}")
