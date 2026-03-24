# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductionTimeRegistration.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductionTimeRegistration.Table.sql`

# COMMAND ----------

# Ensure the target database exists and the current session has the necessary permissions.
# In Databricks, the default catalog is usually "spark_catalog".
# We'll create (if not exists) and switch to the `datastore` database.
spark.sql("CREATE DATABASE IF NOT EXISTS `datastore`")
spark.sql("USE `datastore`")

# COMMAND ----------

# Create the Delta table equivalent to the SQL Server table.
# Column data types are mapped from T‑SQL to Spark SQL types.
spark.sql("""
CREATE TABLE IF NOT EXISTS `ProductionTimeRegistration` (
    ProductionOrderCode STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    RouteCode STRING NOT NULL,
    RoutingName STRING NOT NULL,
    ResourceCode STRING NOT NULL,
    OperationCode STRING NOT NULL,
    OperationNumber INT NOT NULL,
    Shift STRING NOT NULL,
    OperatorType STRING NOT NULL,
    OperatorName LONG NOT NULL,
    RecId LONG NOT NULL,
    PostedJournalDate DATE,
    Hours DECIMAL(32,6) NOT NULL,
    HourPrice DECIMAL(32,6) NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
