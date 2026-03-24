# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ExchangeRate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/DataStore.ExchangeRate.Table.sql`

# COMMAND ----------

# Setup imports (if needed for further processing)
from pyspark.sql import functions as F

# COMMAND ----------

# Databricks does not support the SQL Server session settings.
# The following table creation is adjusted to avoid permission issues
# by creating it in the default database (or a database you have access to).

# Optionally, create a database you have permissions for, e.g., "my_schema".
# Uncomment the next line if you need a dedicated schema.
# spark.sql("CREATE DATABASE IF NOT EXISTS my_schema")

# Use the appropriate database (default or the one you created above).
# spark.sql("USE my_schema")  # Uncomment if you created a custom schema.

# Create the ExchangeRate table without referencing the restricted DataStore schema.
spark.sql("""
CREATE TABLE IF NOT EXISTS ExchangeRate (
    ExchangeRateTypeCode STRING NOT NULL,
    ExchangeRateTypeName STRING NOT NULL,
    DataSource STRING NOT NULL,
    FromCurrencyCode STRING NOT NULL,
    ToCurrencyCode STRING NOT NULL,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ExchangeRate DECIMAL(38,17)
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
