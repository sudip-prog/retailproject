from pyspark.sql import SparkSession
from utils.logger import get_logger
from utils.audit_logger import log_audit
from utils.schema_validator import validate_schema
from utils.data_quality import run_data_quality_checks
from delta.tables import DeltaTable
from utils.schema_metadata import silver_table_metadata
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType


logger = get_logger("orders_silver")

def process_orders():
    spark = SparkSession.builder.appName("OrdersSilver").getOrCreate()

    table_conf = silver_table_metadata["orders"]
    source_path = table_conf["source_path"]
    target_path = table_conf["target_path"]
    schema = table_conf["schema"]
    primary_keys = table_conf["primary_keys"]

    KAFKA_BROKER = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
    KAFKA_TOPIC = "orders"
    checkpoint_path = "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/checkpoints"

    try:

        kafka_options = {
            "kafka.bootstrap.servers" : KAFKA_BROKER, 
            "topic": KAFKA_TOPIC, 
            "subscribe": KAFKA_TOPIC,
            "kafka.security.protocol":"SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "checkpointLocation": checkpoint_path, 
            "kafka.sasl.jaas.config":"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="EFDJSWUZVCTJ4TZI" password="pyyiWLLrgySmD+8CA+CKaT4TMIBNPzf+hj9N43mlNezeNYdMX/9qPdmn8FAqHzjb";""",
            "mergeSchema": True,
            "startingOffsets" : "earliest"
        }
        
        logger.info("Loading orders data")

        df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

        processed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        if not validate_schema(df, schema):
            log_audit(spark, "orders", "failed", "Schema validation failed")
            return

        
        #df = spark.read.format("json").load(source_path)
        logger.info("Loaded orders data")

        #dq_metrics = run_data_quality_checks(processed_df, primary_keys)

        query = processed_df.writeStream \
        .format("delta")\
        .foreachBatch(data_quality_streaming(primary_keys))
        .outputMode("append")\
        .options(**kafka_options)\
        .trigger(once=True)\
        .toTable("retail_catalog.silver.orders")


        query.awaitTermination()

        log_audit(spark, "orders", "success", "Orders table loaded successfully", dq_metrics.get("row_count", 0))

    except Exception as e:
        logger.exception("Error processing orders table")
        log_audit(spark, "orders", "failed", str(e))

# if __name__ == "__main__":
#     process_orders()
