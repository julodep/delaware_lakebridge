# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324101927-oqdi/dbo.SMRBIVendSettlementStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required libraries (Databricks provides Spark session by default)
# ------------------------------------------------------------
# No additional imports are necessary for this script.

# ------------------------------------------------------------
# NOTE: The original T‑SQL script includes SQL Server‑specific settings
# (SET ANSI_NULLS, SET QUOTED_IDENTIFIER) and the "GO" batch separator.
# These are not supported in Databricks/Spark SQL and are therefore omitted.
# ------------------------------------------------------------

# ------------------------------------------------------------
# Create the staging table in the target catalog and schema.
# The original table name is dbo.SMRBIVendSettlementStaging;
# we map it to the Databricks catalog `dbe_dbx_internships` and schema `switchschema`.
# Data types are translated to Spark SQL equivalents:
#   - nvarchar → STRING
#   - int      → INT
#   - numeric(p,s) → DECIMAL(p,s)
#   - datetime → TIMESTAMP
#   - bigint   → LONG
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.SMRBIVendSettlementStaging (
    DEFINITIONGROUP   STRING,
    EXECUTIONID       STRING,
    ISSELECTED        INT,
    TRANSFERSTATUS    INT,
    ACCOUNTNUM        STRING,
    EXCHADJUSTMENT    DECIMAL(32,6),
    SETTLEAMOUNTCUR   DECIMAL(32,6),
    SETTLEAMOUNTMST   DECIMAL(32,6),
    TRANSCOMPANY      STRING,
    TRANSDATE         TIMESTAMP,
    TRANSRECID        LONG,
    VENDSETTLEMENTRECID LONG,
    PARTITION         STRING,
    DATAAREAID        STRING,
    SYNCSTARTDATETIME TIMESTAMP,
    RECID             LONG
)
""")

# COMMAND ----------

# ------------------------------------------------------------
# The original script referenced the PRIMARY filegroup (ON [PRIMARY]),
# which has no meaning in Delta Lake. The table created above is a Delta table
# stored in the default location for the catalog/schema.
# ------------------------------------------------------------

# ------------------------------------------------------------
# (Optional) Verify that the table was created successfully.
# ------------------------------------------------------------
created_df = spark.sql("SHOW TABLES IN dbe_dbx_internships.switchschema LIKE 'SMRBIVendSettlementStaging'")
display(created_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
