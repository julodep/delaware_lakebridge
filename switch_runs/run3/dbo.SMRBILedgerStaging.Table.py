# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/dbo.SMRBILedgerStaging.Table.sql`

# COMMAND ----------

# Setup: import any required modules (Databricks provides Spark session by default)
# No additional imports are needed for this script.

# -------------------------------------------------------------------------
# Create the SMRBILedgerStaging table as a Delta table.
# The original T‑SQL included a reference to the `dbo` schema, but the current
# user does not have READ_METADATA/USAGE permissions on that schema.
# To avoid permission errors, the table is created in the default schema
# (or an accessible schema) without the `dbo.` qualifier.
# -------------------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE SMRBILedgerStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    BUDGETEXCHANGERATETYPE STRING NOT NULL,
    ACCOUNTINGCURRENCY STRING NOT NULL,
    CHARTOFACCOUNTSRECID LONG NOT NULL,
    EXCHANGERATETYPE STRING NOT NULL,
    NAME STRING NOT NULL,
    REPORTINGCURRENCY STRING NOT NULL,
    LEDGERRECID LONG NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# NOTE:
# - The original CREATE TABLE defined a CLUSTERED PRIMARY KEY on
#   (EXECUTIONID, LEDGERRECID, PARTITION). Delta Lake does not enforce primary
#   keys, so this constraint is not recreated. If uniqueness enforcement is
#   required, consider adding a deduplication step or using Delta constraints
#   (e.g., `ALTER TABLE ... ADD CONSTRAINT ...` when supported).
# - Table‑level options such as STATISTICS_NORECOMPUTE, IGNORE_DUP_KEY,
#   OPTIMIZE_FOR_SEQUENTIAL_KEY, and filegroup specifications are specific to
#   SQL Server and have no equivalent in Databricks; they are therefore omitted.
# -------------------------------------------------------------------------

# (Optional) Verify that the table was created successfully
created_df = spark.sql("DESCRIBE TABLE SMRBILedgerStaging")
display(created_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
