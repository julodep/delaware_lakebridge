# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Employee.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Employee.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: import modules if needed (Databricks provides `spark` by default)
# ------------------------------------------------------------------
# No additional imports are required for executing SQL statements.

# ------------------------------------------------------------------
# NOTE: The following T‑SQL session settings are not applicable in Databricks.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON
# In Spark SQL these options are either always enabled or not relevant,
# so we simply omit them.
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# Create the `Employee` table in the current/default database.
# The original T‑SQL data types are mapped to Spark SQL equivalents:
#   BIGINT   -> BIGINT (Spark)
#   NVARCHAR -> STRING
# The `ON [PRIMARY]` clause is specific to Microsoft SQL Server storage
# and has no meaning in Delta Lake, so it is omitted.
# ------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS Employee (
    EmployeeId       BIGINT   NOT NULL,
    EmployeeCode     STRING   NOT NULL,
    EmployeeName     STRING   NOT NULL,
    EmployeeCodeName STRING   NOT NULL,
    DimensionName    STRING   NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
