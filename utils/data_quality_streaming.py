def run_streaming_data_quality_with_pk(primary_keys):
    def run_streaming_data_quality(batch_df: DataFrame, batch_id: int):
        results = {}

        # Null count
        nulls = {col_name: batch_df.filter(col(col_name).isNull()).count() for col_name in batch_df.columns}
        results["null_counts"] = nulls

        # Duplicate PKs
        pk_dupes = batch_df.groupBy("id").count().filter(col("count") > 1).count()
        results["duplicate_primary_keys"] = pk_dupes

        # Row count
        results["row_count"] = batch_df.count()

        logger.info(f"Batch {batch_id} - Data Quality Results: {results}")
    return run_streaming_data_quality