from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, countDistinct, min as _min, month, year, current_date

# Initialize Spark session
spark = SparkSession.builder.appName("GoldKPIs").getOrCreate()

# Replace these with actual data loading from Silver tables

customers_df = spark.read.format("delta").load("abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/customers/")
products_df = spark.read.format("delta").load("abfss://retailstream360@storageaccountsudip1.dfs.core.windows.net/silver/products/")
orders_df = spark.read.table("retail_catalog.silver.orders")
transactions_df = spark.read.table("retail_catalog.silver.transactions")

# --- Sales & Revenue KPIs ---

transactions_df = transactions_df.withColumn("total_price", col("price") * col("quantity"))
total_revenue_df = transactions_df.agg(_sum("total_price").alias("total_revenue"))

revenue_by_product_df = transactions_df.groupBy("product_id").agg(_sum("total_price").alias("revenue"))

revenue_by_category_df = transactions_df.join(products_df, "product_id").groupBy("category").agg(_sum("total_price").alias("revenue"))

revenue_by_day_df = transactions_df\
    .groupBy("transaction_date").agg(_sum("total_price").alias("daily_revenue"))

revenue_by_customer_df = transactions_df.join(orders_df, "order_id") \
     .groupBy("customer_id").agg(_sum("total_price").alias("customer_revenue"))

revenue_per_order_df = transactions_df\
    .groupBy("order_id").agg(_sum("total_price").alias("order_revenue"))

aov_df = revenue_per_order_df.agg(avg("order_revenue").alias("average_order_value"))

# --- Customer KPIs ---
total_customers_df = customers_df.agg(countDistinct("customer_id").alias("total_customers"))

repeat_customers_df = orders_df.groupBy("customer_id").agg(count("order_id").alias("order_count")) \
    .filter(col("order_count") > 1).agg(count("customer_id").alias("repeat_customers"))

first_order_df = orders_df.groupBy("customer_id").agg(_min("order_date").alias("first_order_date"))

# --- Order KPIs ---
total_orders_df = orders_df.agg(countDistinct("order_id").alias("total_orders"))

orders_by_status_df = orders_df.groupBy("status").agg(count("order_id").alias("orders_count"))

orders_per_customer_df = orders_df.groupBy("customer_id").agg(count("order_id").alias("orders_per_customer"))

avg_items_per_order_df = transactions_df.groupBy("order_id").agg(_sum("quantity").alias("items_per_order")) \
    .agg(avg("items_per_order").alias("average_items_per_order"))

# --- Transaction KPIs ---
total_quantity_sold_df = transactions_df.agg(_sum("quantity").alias("total_quantity_sold"))

best_selling_products_df = transactions_df.groupBy("product_id").agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(col("total_quantity").desc())

total_transactions_df = transactions_df.agg(count("transaction_id").alias("total_transactions"))

avg_transaction_price_df = transactions_df.agg(avg("price").alias("avg_transaction_price"))

daily_transaction_volume_df = transactions_df.groupBy("transaction_date") \
    .agg(count("transaction_id").alias("transactions_per_day"))

# --- Product KPIs ---
product_count_df = products_df.agg(countDistinct("product_id").alias("total_products"))

avg_product_price_df = products_df.agg(avg("price").alias("avg_product_price"))

top_categories_by_sales_df = transactions_df.join(products_df, "product_id") \
    .withColumn("total_price", col("price") * col("quantity")) \
    .groupBy("category").agg(_sum("total_price").alias("category_sales")) \
    .orderBy(col("category_sales").desc())

product_price_distribution_df = products_df.select("price").summary("count", "min", "max", "mean", "stddev")

# --- Time-Based KPIs ---
monthly_revenue_df = transactions_df.withColumn("month", month("transaction_date")) \
    .withColumn("year", year("transaction_date")) \
    .withColumn("total_price", col("price") * col("quantity")) \
    .groupBy("year", "month").agg(_sum("total_price").alias("monthly_revenue"))

monthly_active_customers_df = orders_df.withColumn("month", month("order_date")) \
    .withColumn("year", year("order_date")) \
    .groupBy("year", "month").agg(countDistinct("customer_id").alias("active_customers"))

new_customers_per_month_df = customers_df.withColumn("month", month("join_date")) \
    .withColumn("year", year("join_date")) \
    .groupBy("year", "month").agg(count("customer_id").alias("new_customers"))

monthly_orders_df = orders_df.withColumn("month", month("order_date")) \
    .withColumn("year", year("order_date")) \
    .groupBy("year", "month").agg(count("order_id").alias("monthly_orders"))

# Revenue growth would be computed using window functions (optional)
