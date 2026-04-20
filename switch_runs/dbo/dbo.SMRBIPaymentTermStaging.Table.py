# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPaymentTermStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPaymentTermStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Drop the table if it already exists (avoids “already exists” errors)
# ------------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIPaymentTermStaging`")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Create the staging table in Delta Lake
# ------------------------------------------------------------------
# T‑SQL data types are mapped to Spark SQL types as follows:
#   NVARCHAR ➜ STRING
#   INT      ➜ INT
#   DATETIME ➜ TIMESTAMP
#
# Primary‑key and other SQL Server catalog features (e.g. PK_CLUSTERED)
# do not have a direct equivalent in Delta Lake, so they are omitted.
# --------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPaymentTermStaging` (
    DEFINITIONGROUP   STRING,
    EXECUTIONID       STRING,
    ISSELECTED        INT,
    TRANSFERSTATUS    INT,
    DESCRIPTION      STRING,
    NAME              STRING,
    COMPANY           STRING,
    PARTITION         STRING,
    DATAAREAID        STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
