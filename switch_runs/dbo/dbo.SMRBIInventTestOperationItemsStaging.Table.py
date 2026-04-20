# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTestOperationItemsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTestOperationItemsStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the SMRBIInventTestOperationItemsStaging table in Delta Lake
# --------------------------------------------------------------
# NOTE:  Because Delta Lake (Databricks) does not support the exact T‑SQL
#        PRIMARY KEY or INCLUDE clauses, the primary‑key definition from
#        the original script has been omitted.  You can enforce uniqueness
#        after the fact with a Delta Lake unique constraint if required.
# --------------------------------------------------------------

spark.sql(f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventTestOperationItemsStaging`
    (
        DEFINITIONGROUP STRING,
        EXECUTIONID    STRING,
        ISSELECTED     INT,
        TRANSFERSTATUS  INT,
        COSTAMOUNT     DECIMAL(32,6),
        INVENTNONCONFORMANCEID STRING,
        INVENTQTY      DECIMAL(32,6),
        ITEMID         STRING,
        LINENUM        DECIMAL(32,16),
        COMPANY        STRING,
        PARTITION      STRING,
        DATAAREAID     STRING,
        SYNCSTARTDATETIME TIMESTAMP,
        RECID          LONG
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
