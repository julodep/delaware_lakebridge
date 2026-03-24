# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Department.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Department.Table.sql`

# COMMAND ----------

# Databricks provides a Spark session by default.
# Ensure we are using a database that the current user has access to.
# Here we use the default database to avoid permission issues.

spark.sql("USE default")

# COMMAND ----------

# Create the Department table in the default schema.
spark.sql("""
CREATE TABLE IF NOT EXISTS Department (
    DepartmentId BIGINT NOT NULL,
    DepartmentCode STRING NOT NULL,
    DepartmentName STRING NOT NULL,
    DepartmentCodeName STRING NOT NULL,
    DimensionName STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
