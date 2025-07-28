# config_bronze.py

import os
from dotenv import load_dotenv

load_dotenv()

ADLS_ACCOUNT = os.getenv("ADLS_ACCOUNT")
#ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")

RAW_BASE_PATH = f"abfss://retailstream360@{ADLS_ACCOUNT}.dfs.core.windows.net/raw"
BRONZE_BASE_PATH = f"abfss://retailstream360@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze"
CHECKPOINT_BASE_PATH = f"abfss://retailstream360@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze/checkpoints/"
QUARANTINE_BASE_PATH = f"abfss://retailstream360@{ADLS_ACCOUNT}.dfs.core.windows.net/quarantine"

TABLES_TO_LOAD = [
    {"table_name": "customers", "file_format": "csv"},
    {"table_name": "products", "file_format": "csv"}]
#    {"table_name": "orders", "file_format": "json"},
#    {"table_name": "transactions", "file_format": "json"}

