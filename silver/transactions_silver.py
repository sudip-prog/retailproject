from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

KAFKA_BROKER = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
KAFKA_TOPIC = "transactions"
checkpoint_path = "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/checkpoints_transactions"

def process_transactions():

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
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Deserialize and process (example for JSON data)
    processed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    if not validate_schema(processed_df, schema):
            log_audit(spark, "transactions", "failed", "Schema validation failed")
            return


    query = processed_df.writeStream \
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation",checkpoint_path)\
    .trigger(availableNow=True)\
    .toTable("retail_catalog.silver.transactions")


    query.awaitTermination()
