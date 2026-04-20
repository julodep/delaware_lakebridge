# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDimensionAttributeLevelValueAllViewStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDimensionAttributeLevelValueAllViewStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  CREATE TABLE
#  ------------------------------------------------------------------
#  The original T‑SQL definition uses a clustered primary key and column
#  types that map directly to Delta Lake / Spark SQL data types:
#
#      NVARCHAR -> STRING
#      INT      -> INT
#      BIGINT   -> LONG
#      DATETIME -> TIMESTAMP
#
#  Delta Lake does not support PRIMARY KEY clauses or clustered indexes
#  in the DDL. Those constraints are omitted, but you can enforce
#  uniqueness in downstream logic if needed.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIDimensionAttributeLevelValueAllViewStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    VALUECOMBINATIONRECID LONG NOT NULL,
    DISPLAYVALUE STRING NOT NULL,
    ENTITYINSTANCE LONG NOT NULL,
    DIMENSIONATTRIBUTE LONG NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
