# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingMissingColumn.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingMissingColumn.Table.sql`

# COMMAND ----------

# Create table in the target schema
spark.sql("""
CREATE TABLE IF NOT EXISTS ETL.`StagingMissingColumn` (
    `TableName` STRING,
    `ColumnName` STRING
)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/{catalog}/{schema}.db/StagingMissingColumn'
""")

# COMMAND ----------

# Alternatively, if you want to create the table in the default location
spark.sql("""
CREATE TABLE IF NOT EXISTS ETL.`StagingMissingColumn` (
    `TableName` STRING,
    `ColumnName` STRING
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
