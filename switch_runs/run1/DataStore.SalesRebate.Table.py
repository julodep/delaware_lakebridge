# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesRebate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SalesRebate.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Set session options (SQL Server specific; not applicable in Databricks)
# -------------------------------------------------------------------------
# In SQL Server the statements below control ANSI NULL handling and identifier quoting.
# Databricks does not use these settings, so they are commented out for reference.
# spark.sql("SET ANSI_NULLS ON")
# spark.sql("SET QUOTED_IDENTIFIER ON")

# -------------------------------------------------------------------------
# Create the SalesRebate table in the default database as a Delta table
# -------------------------------------------------------------------------
# The original T‑SQL uses NVARCHAR, BIGINT, and NUMERIC types.
# In Spark SQL we map them to STRING, BIGINT (LONG), and DECIMAL respectively.
# The table is created with OR REPLACE to avoid errors if it already exists.
spark.sql("""
CREATE OR REPLACE TABLE SalesRebate (
    CompanyCode               STRING,
    SalesRebateCode           STRING,
    SalesInvoiceCode          STRING NOT NULL,
    SalesInvoiceLineId        BIGINT NOT NULL,
    ProductCode               STRING NOT NULL,
    RebateCustomerCode        STRING NOT NULL,
    RebateCurrencyCode        STRING NOT NULL,
    RebateAmountOriginal      DECIMAL(32, 6) NOT NULL,
    RebateAmountCompleted     DECIMAL(38, 6),
    RebateAmountMarked        DECIMAL(38, 6),
    RebateAmountCancelled     DECIMAL(38, 6),
    RebateAmountVariance      DECIMAL(38, 6)
)
USING DELTA
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# Optional: Verify the table schema
# -------------------------------------------------------------------------
spark.sql("DESCRIBE TABLE SalesRebate").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
