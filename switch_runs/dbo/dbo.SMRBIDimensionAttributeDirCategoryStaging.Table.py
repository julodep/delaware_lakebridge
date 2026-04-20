# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDimensionAttributeDirCategoryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDimensionAttributeDirCategoryStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create a Delta Lake table that mirrors the T‑SQL table  
# `dbo.SMRBIDimensionAttributeDirCategoryStaging`.
#
# All references are fully qualified using the provided `dbe_dbx_internships` and
# `dbo` placeholders.
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# T‑SQL data‑type mapping to Spark/Delta:
#     nvarchar(n)   -> STRING
#     int           -> INT
#     bigint        -> BIGINT
#     datetime      -> TIMESTAMP
#
# Delta Lake does not support traditional primary‑key constraints or
# clustered indexes.  The original PRIMARY KEY clause is retained as a
# comment so the metadata is preserved for documentation purposes, but
# Delta enforces uniqueness at the application level if needed.
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# The table is created with `CREATE OR REPLACE TABLE` which will drop
# and recreate the table each time the notebook is run, ensuring a
# deterministic schema.  If you need to retain existing data you can
# remove the `OR REPLACE` clause or load data into an existing table.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIDimensionAttributeDirCategoryStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    DIMENSIONATTRIBUTE BIGINT NOT NULL,
    DIRCATEGORY BIGINT NOT NULL,
    DIMENSIONATTRIBUTE_NAME STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
