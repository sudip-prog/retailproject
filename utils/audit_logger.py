from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from utils.logger import get_logger

logger = get_logger("audit_logger")

def log_audit(spark: SparkSession, table_name: str, status: str, message: str = "", record_count: int = 0):
    audit_data = [(datetime.now(), table_name, status, message, record_count)]
    columns = ["timestamp", "table_name", "status", "message", "record_count"]
    df = spark.createDataFrame(audit_data, columns)
    audit_path = "abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/audit_logs/"
    df.write.mode("append").format("delta").save(audit_path)
    logger.info(f"Audit log written for {table_name} with status {status}")
