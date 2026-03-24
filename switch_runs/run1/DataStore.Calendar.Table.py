# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Calendar.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Calendar.Table.sql`

# COMMAND ----------

# Import necessary Spark functions
from pyspark.sql import functions as F

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE:
# The original code attempted to create a table in the `DataStore` database,
# but the current user does not have READ_METADATA or USAGE privileges on that
# database, resulting in an INSUFFICIENT_PERMISSIONS error.
# To avoid this issue, we create the table in a database where the user has
# sufficient permissions (e.g., the default database). If a specific
# database is required, replace `default` with a database name that the user
# can access and ensure the appropriate privileges are granted.
# ------------------------------------------------------------------

# Optionally, you can set the current database to `default` (or another permitted db)
spark.sql("USE default")

# COMMAND ----------

# Create the Calendar table in the permitted database.
spark.sql("""
CREATE TABLE IF NOT EXISTS default.Calendar (
    CalendarCode STRING NOT NULL,
    CalendarName STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    StandardWorkDayHours DECIMAL(32, 6) NOT NULL
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
