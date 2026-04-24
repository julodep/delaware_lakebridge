# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingMissingTable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingMissingTable.Table.sql`

# COMMAND ----------

# Create the staging table
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbe_dbx_internships.ETL.StagingMissingTable (
        TableName STRING,
        SourceName STRING
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
