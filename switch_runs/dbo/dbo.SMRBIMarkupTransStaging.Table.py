# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIMarkupTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIMarkupTransStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------
# 1. Table creation – Delta Lake (Data Lake) on Databricks
#    - Column types are mapped from T‑SQL to Spark SQL types
#    - Reserved word `PARTITION` is escaped with backticks
#    - Primary key and other SQL Server specific constraints are omitted
# ---------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIMarkupTransStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    CALCULATEDAMOUNT DECIMAL(32,6),
    CURRENCYCODE STRING,
    COMPANY STRING,
    MARKUPCATEGORY INT,
    MARKUPCODE STRING,
    TRANSRECID BIGINT,
    TRANSTABLEID INT,
    VALUE DECIMAL(32,6),
    MARKUPTRANSRECID BIGINT,
    `PARTITION` STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
