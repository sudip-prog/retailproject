from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

silver_table_metadata = {
    "customers": {
        "source_path": "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/bronze/customers/",
        "target_path": "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/customers/",
        "primary_keys": ["customer_id"],
        "scd_type": "scd1",
        "cdc_enabled": False,
        "schema": StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("join_date", DateType(), True)
            #StructField("customer_state", StringType(), True)
            #StructField("ingest_ts", TimestampType(), True)
        ])
    },

    "products": {
        "source_path": "abfss://bronze@<your-container-name>.dfs.core.windows.net/products/",
        "target_path": "abfss://silver@<your-container-name>.dfs.core.windows.net/products/",
        "primary_keys": ["product_id"],
        "scd_type": "scd2",
        "cdc_enabled": True,
        "schema": StructType([
            StructField("product_id", StringType(), False),
            StructField("product_category_name", StringType(), True),
            StructField("product_name_lenght", IntegerType(), True),
            StructField("product_description_lenght", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", DoubleType(), True),
            StructField("product_length_cm", DoubleType(), True),
            StructField("product_height_cm", DoubleType(), True),
            StructField("product_width_cm", DoubleType(), True),
            StructField("ingest_ts", TimestampType(), True)
        ])
    },

    "orders": {
        "source_path": "abfss://bronze@<your-container-name>.dfs.core.windows.net/orders/",
        "target_path": "abfss://silver@<your-container-name>.dfs.core.windows.net/orders/",
        "primary_keys": ["order_id"],
        "scd_type": None,
        "cdc_enabled": True,
        "schema": StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", DateType(), True),
            StructField("status", StringType(), True)
            # StructField("order_delivered_carrier_date", TimestampType(), True),
            # StructField("order_delivered_customer_date", TimestampType(), True),
            # StructField("order_estimated_delivery_date", TimestampType(), True),
            # StructField("ingest_ts", TimestampType(), True)
        ])
    },

    "transactions": {
        "source_path": "abfss://streaming@<your-container-name>.dfs.core.windows.net/transactions/",
        "target_path": "abfss://silver@<your-container-name>.dfs.core.windows.net/transactions/",
        "primary_keys": ["transaction_id"],
        "scd_type": None,
        "cdc_enabled": False,
        "schema": StructType([
            StructField("transaction_id", StringType(), False),
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_value", DoubleType(), True),
            StructField("transaction_timestamp", TimestampType(), True),
            StructField("ingest_ts", TimestampType(), True)
        ])
    }
}
