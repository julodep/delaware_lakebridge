# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.DeliveryMode.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.DeliveryMode.Table.sql`

# COMMAND ----------

# Import Spark functions if needed
# import pyspark.sql.functions as F

# Ensure the target database exists and the user has access.
# If you do not have permissions on the `DataStore` database,
# you can create the table in the default database (or another database you own).

# Uncomment the following line if you have privileges to create the database:
# spark.sql("CREATE DATABASE IF NOT EXISTS DataStore")

# Create the DeliveryMode table.
# If you have permission on the DataStore database, use the qualified name:
# spark.sql("""
# CREATE OR REPLACE TABLE DataStore.DeliveryMode (
#     CompanyCode STRING,
#     DeliveryModeCode STRING,
#     DeliveryModeName STRING NOT NULL
# )
# """)

# Otherwise, create the table in the current/default database:
spark.sql("""
CREATE OR REPLACE TABLE DeliveryMode (
    CompanyCode STRING,
    DeliveryModeCode STRING,
    DeliveryModeName STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
