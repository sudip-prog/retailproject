# controller_bronze.py

from pyspark.sql import SparkSession
from config.config_bronze import (
    RAW_BASE_PATH,
    BRONZE_BASE_PATH,
    CHECKPOINT_BASE_PATH,
    QUARANTINE_BASE_PATH,
    TABLES_TO_LOAD,
)
from bronze_loader_autoloader import load_bronze_table

spark = SparkSession.builder.appName("BronzeLoader").getOrCreate()
# spark.conf.set(
#     f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
#     ADLS_ACCOUNT_KEY
# )

for table in TABLES_TO_LOAD:
    try:
        load_bronze_table(
            spark=spark,
            table_name=table["table_name"],
            raw_base_path=RAW_BASE_PATH,
            bronze_base_path=BRONZE_BASE_PATH,
            checkpoint_base_path=CHECKPOINT_BASE_PATH,
            quarantine_base_path=QUARANTINE_BASE_PATH,
            file_format=table["file_format"]
        )
    except Exception as e:
        print(f"Failed to load {table['table_name']}: {str(e)}")
