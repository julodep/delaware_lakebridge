# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingInputTargetLookup.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingInputTargetLookup.Table.sql`

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS ETL.`StagingInputTargetLookup` (
    `TargetName` STRING NOT NULL,
    `TargetSchema` STRING NOT NULL,
    `TargetTable` STRING NOT NULL,
    `LookupTableAlias` STRING NOT NULL,
    `LookupSchema` STRING NOT NULL,
    `LookupTable` STRING NOT NULL,
    `LookupColumn` STRING NOT NULL,
    `TargetColumn` STRING NOT NULL,
    `SourceJoinColumns` STRING NOT NULL,
    `LookupJoinColumns` STRING NOT NULL,
    `Description` STRING
)
USING delta
LOCATION '{catalog}/{schema}/StagingInputTargetLookup'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.dataSkippingNumIndexBuckets' = '128'
)
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS ETL.`StagingInputTargetLookup` (
    `TargetName` STRING NOT NULL,
    `TargetSchema` STRING NOT NULL,
    `TargetTable` STRING NOT NULL,
    `LookupTableAlias` STRING NOT NULL,
    `LookupSchema` STRING NOT NULL,
    `LookupTable` STRING NOT NULL,
    `LookupColumn` STRING NOT NULL,
    `TargetColumn` STRING NOT NULL,
    `SourceJoinColumns` STRING NOT NULL,
    `LookupJoinColumns` STRING NOT NULL,
    `Description` STRING
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
