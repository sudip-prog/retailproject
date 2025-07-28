# Retailproject
Ecommerce data pipeline project

**Overview<br>**
This repository contains code and utilities for building a retail ecommerce data pipeline on Azure Databricks. The pipeline transforms raw order data from the silver layer to the gold layer using PySpark, enabling advanced analytics and reporting.

**Features<br>**
Reads and processes order data from the silver layer (Delta Lake)
Aggregates daily order counts and distinct customer counts
Writes transformed data to the gold layer
Includes logging and audit logging utilities for monitoring and traceability

**Furture Enablements<br>**
Data marts are currently stored in Data Bricks in future can be migrated to Snowflake or other custom data warehouses. Also machine learning predictions will be added. 
