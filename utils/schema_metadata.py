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
        ])
    },

    "products": {
        "source_path": "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/bronze/products/",
        "target_path": "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/products/",
        "primary_keys": ["product_id"],
        "scd_type": "scd2",
        "cdc_enabled": True,
        "schema": StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True)
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
        ])
    },

    "transactions": {
        "source_path": "abfss://streaming@<your-container-name>.dfs.core.windows.net/transactions/",
        "target_path": "abfss://silver@<your-container-name>.dfs.core.windows.net/transactions/",
        "primary_keys": ["transaction_id"],
        "scd_type": None,
        "cdc_enabled": False,
        "schema":  StructType([
        StructField("transaction_id", StringType()),
        StructField("order_id", StringType()),
        StructField("product_id", StringType()), 
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("transaction_date", DateType())])
    }
}
