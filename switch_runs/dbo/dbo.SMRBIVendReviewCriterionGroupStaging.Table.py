# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendReviewCriterionGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendReviewCriterionGroupStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------
# Databricks notebook – Create the staging table
# ------------------------------------------------
# This snippet creates the table that was defined in the T‑SQL
# CREATE TABLE statement.  All object references are fully‑qualified
# using the `dbe_dbx_internships` and `dbo` placeholders, which should be
# replaced by the actual catalog and schema names when the snippet is
# deployed.
#
#   • T‑SQL data types that do not exist in Spark are mapped to the
#     closest Spark SQL type:
#       NVARCHAR => STRING
#       INT      => INT
#       BIGINT   => BIGINT
#       DATETIME => TIMESTAMP
#   • PRIMARY KEY, ON PRIMARY, and other index/cluster options are
#     not supported in Delta Lake, so they are omitted.  The table
#     will be created with Delta format (the default for CREATE TABLE
#     in Databricks).
#
#   • The `dbe_dbx_internships` and `dbo` placeholders are left in the
#     string; when this snippet is used you should replace them
#     with the actual names (or use string formatting if you are
#     within a Python script).
#
#   • If the table already exists and you want to overwrite it,
#     use CREATE OR REPLACE TABLE.  If you want to keep the existing
#     data and only add the new table, use CREATE TABLE without
#     OR REPLACE (but this will error if the table already exists).

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIVendReviewCriterionGroupStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT   NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    NAME           STRING NOT NULL,
    VENDREVIEWRECID BIGINT NOT NULL,
    PARTITION      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
