# Retailproject
Ecommerce data pipeline project

**Overview<br>**
This repository contains code and utilities for building a retail ecommerce data pipeline on Azure Databricks. The pipeline transforms raw order data from the silver layer to the gold layer using PySpark, enabling advanced analytics and reporting.

**Process<br>**
An external raw system is created which sends data for Customers, Products, Orders and Transactions. The Customer and Product data is ingested via batch process and incremental load is performed using Autoloader and the Orders and Transaction is processed via Kafka streams and processed in Silver layer. Once the data is arrive in Silver layer, data quality and schema validation is performed and all actions are included in a seperate audit table. Data from silver layer is further taken into gold layer which has the respective KPI stored into the Data Marts. 
Architecture used - STAR Schema
Reads and processes order data from the silver layer (Delta Lake)
Aggregates daily order counts and distinct customer counts
Writes transformed data to the gold layer
Includes logging and audit logging utilities for monitoring and traceability

**Furture Enablements<br>**
Data marts are currently stored in Data Bricks in future can be migrated to Snowflake or other custom data warehouses. Also machine learning predictions will be added. 
