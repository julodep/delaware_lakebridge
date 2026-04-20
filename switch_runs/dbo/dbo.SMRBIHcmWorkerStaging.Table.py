# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIHcmWorkerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIHcmWorkerStaging.Table.sql`

# COMMAND ----------

# ---- Create the staging table that was originally defined in T‑SQL ---- #
# The original DDL used square brackets and a primary‑key constraint.
# In Databricks (Delta Lake) we omit the brackets and the `PRIMARY KEY`
# clause – Delta tables do not enforce primary keys at the storage level.
# We keep the column names exactly as in the source table and map
# T‑SQL types to Spark SQL data types:
#
#   NVARCHAR → STRING
#   INT      → INT
#   BIGINT   → BIGINT
#   DATETIME → TIMESTAMP
#
# The `dbe_dbx_internships` and `dbo` placeholders should be replaced with
# your actual Unity Catalog catalog and schema names.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIHcmWorkerStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    PERSONNELNUMBER STRING NOT NULL,
    NAME           STRING NOT NULL,
    HCMWORKERRECID BIGINT NOT NULL,
    VALIDTO        TIMESTAMP NOT NULL,
    VALIDFROM      TIMESTAMP NOT NULL,
    PARTITION      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
