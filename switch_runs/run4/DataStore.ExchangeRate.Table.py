# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ExchangeRate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/DataStore.ExchangeRate.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup imports (Databricks provides a SparkSession by default)
# ------------------------------------------------------------------
from pyspark.sql import functions as F

# COMMAND ----------

# ------------------------------------------------------------------
# Create the ExchangeRate table in a database that the current user has
# sufficient privileges for (e.g., the default database).
# ------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS `default`.`ExchangeRate` (
    ExchangeRateTypeCode STRING NOT NULL,
    ExchangeRateTypeName STRING NOT NULL,
    DataSource STRING NOT NULL,
    FromCurrencyCode STRING NOT NULL,
    ToCurrencyCode STRING NOT NULL,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ExchangeRate DECIMAL(38,17)
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Verify the table schema after creation.
# ------------------------------------------------------------------
exchange_rate_schema_df = spark.sql("DESCRIBE TABLE `default`.`ExchangeRate`")
display(exchange_rate_schema_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
