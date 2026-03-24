# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.GroupCurrency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/ETL.GroupCurrency.Table.sql`

# COMMAND ----------

# Set the current catalog to the default Spark catalog.
# This ensures that subsequent operations have the necessary metadata permissions
# on the catalog level.
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# Switch to the ETL database (schema) after it is created.
# Using fully‑qualified names and an explicit USE statement helps avoid
# permission issues related to implicit catalog/database resolution.
spark.sql("CREATE DATABASE IF NOT EXISTS ETL")
spark.sql("USE ETL")

# COMMAND ----------

# Create the GroupCurrency table in the ETL schema.
# NVARCHAR maps to STRING in Spark SQL; the NOT NULL constraint is preserved.
# The SQL Server specific clause \"ON [PRIMARY]\" has no equivalent in Databricks and is omitted.
spark.sql("""
CREATE TABLE IF NOT EXISTS GroupCurrency (
    GroupCurrencyCode STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
