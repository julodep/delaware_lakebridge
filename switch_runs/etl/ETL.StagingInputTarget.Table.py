# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingInputTarget.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingInputTarget.Table.sql`

# COMMAND ----------

# Create the table
spark.sql("""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.ETL.StagingInputTarget (
    TargetName          STRING NOT NULL,
    TargetSchema        STRING NOT NULL,
    TargetTable         STRING NOT NULL,
    SourceSchema        STRING NOT NULL,
    SourceTable         STRING NOT NULL,
    PrimaryKey          STRING,
    ActionType          STRING NOT NULL DEFAULT 'Upsert',
    Category            STRING,
    Status              STRING NOT NULL DEFAULT 'ACTIVE',
    Description         STRING NOT NULL
)
USING delta
LOCATION '/mnt/{catalog}/{schema}/StagingInputTarget'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# Create primary key constraint
spark.sql("""
ALTER TABLE dbe_dbx_internships.ETL.StagingInputTarget
ADD CONSTRAINT PK_StagingInputTarget PRIMARY KEY (TargetName, TargetSchema, TargetTable)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
