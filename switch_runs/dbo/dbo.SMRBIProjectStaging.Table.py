# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjectStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjectStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks / Python version of the T‑SQL CREATE TABLE statement
# ------------------------------------------------------------------
# The table SMRBIProjectStaging contains many standard columns.  
# Databricks uses Spark SQL where a simple CREATE TABLE (Delta Lake) is enough.
# Composite PRIMARY KEY constraints are not enforced in Delta Lake; we keep a
# comment to document the intended uniqueness.  Statistics, IGNORE_DUP_KEY, and
# OPTIMIZE_FOR_SEQUENTIAL_KEY options from SQL Server are omitted because
# Spark handles statistics automatically and does not support those flags.
# ------------------------------------------------------------------

# CREATE the table with the same column list and data‑type mapping
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIProjectStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT     NOT NULL,
    TRANSFERSTATUS INT     NOT NULL,
    PROJECTID      STRING NOT NULL,
    CUSTOMERACCOUNT STRING NOT NULL,
    ACTUALENDDATE  TIMESTAMP NOT NULL,
    PROJECTNAME    STRING NOT NULL,
    PROJECTGROUPID STRING NOT NULL,
    ACTUALSTARTDATE TIMESTAMP NOT NULL,
    COMPANY          STRING NOT NULL,
    PARTITION        STRING NOT NULL,
    DATAAREAID       STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
