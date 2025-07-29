# Dictionary of save plans for each KPI
kpi_save_plan = {
    "total_revenue": {"mode": "overwrite", "partition": None},
    "revenue_by_product": {"mode": "overwrite", "partition": "product_id"},
    "revenue_by_category": {"mode": "overwrite", "partition": "category"},
    "revenue_by_day": {"mode": "append", "partition": "transaction_date"},
    "revenue_by_customer": {"mode": "overwrite", "partition": "customer_id"},
    "revenue_per_order": {"mode": "overwrite", "partition": "order_id"},
    "average_order_value": {"mode": "overwrite", "partition": None},
    "total_customers": {"mode": "overwrite", "partition": None},
    "repeat_customers": {"mode": "overwrite", "partition": None},
    "first_order_per_customer": {"mode": "overwrite", "partition": "customer_id"},
    "total_orders": {"mode": "overwrite", "partition": None},
    "orders_by_status": {"mode": "overwrite", "partition": "status"},
    "orders_per_customer": {"mode": "overwrite", "partition": "customer_id"},
    "average_items_per_order": {"mode": "overwrite", "partition": None},
    "total_quantity_sold": {"mode": "overwrite", "partition": None},
    "best_selling_products": {"mode": "overwrite", "partition": "product_id"},
    "total_transactions": {"mode": "overwrite", "partition": None},
    "avg_transaction_price": {"mode": "overwrite", "partition": None},
    "daily_transaction_volume": {"mode": "append", "partition": "transaction_date"},
    "total_products": {"mode": "overwrite", "partition": None},
    "avg_product_price": {"mode": "overwrite", "partition": None},
    "top_categories_by_sales": {"mode": "overwrite", "partition": "category"},
    "product_price_distribution": {"mode": "overwrite", "partition": None},
    "monthly_revenue_trend": {"mode": "append", "partition": "month"},
    "monthly_active_customers": {"mode": "append", "partition": "month"},
    "new_customers_per_month": {"mode": "append", "partition": "month"},
    "monthly_orders": {"mode": "append", "partition": "month"},
    "daily_revenue_growth": {"mode": "append", "partition": "transaction_date"},
}


for kpi_name, config in kpi_save_plan.items():
    mode = config["mode"]
    partition_col = config["partition"]
    table = f"retail_catalog.gold.{kpi_name}"  

    kpi_df = globals().get(f"{kpi_name}_df") 

    if kpi_df is not None:
        if partition_col:
            kpi_df.write.format("delta").mode(mode).partitionBy(partition_col).saveAsTable(table)
        else:
            kpi_df.write.format("delta").mode(mode).saveAsTable(table)
    else:
        print(f"{kpi_name}_df not found in environment.")
