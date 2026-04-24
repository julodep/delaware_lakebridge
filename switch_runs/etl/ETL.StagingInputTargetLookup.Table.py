# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingInputTargetLookup.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingInputTargetLookup.Table.sql`

# COMMAND ----------

# Fix the error by running the two SQL statements separately
spark.sql(f"""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.ETL.StagingInputTargetLookup (
    TargetName        STRING NOT NULL,
    TargetSchema      STRING NOT NULL,
    TargetTable       STRING NOT NULL,
    LookupTableAlias  STRING NOT NULL,
    LookupSchema      STRING NOT NULL,
    LookupTable       STRING NOT NULL,
    LookupColumn      STRING NOT NULL,
    TargetColumn      STRING NOT NULL,
    SourceJoinColumns STRING NOT NULL,
    LookupJoinColumns STRING NOT NULL,
    Description       STRING
)
USING delta
LOCATION 'dbe_dbx_internships.ETL.StagingInputTargetLookup'
""")

# COMMAND ----------

# Note that creating a unique index on a Delta table in Databricks is not directly supported.
# However, we can use the UNIQUE constraint in the CREATE TABLE statement instead.
# If you still want to create an index, you can use the following command to create a non-unique index.
spark.sql(f"""
CREATE INDEX IF NOT EXISTS PK_StagingInputTargetLookup
ON dbe_dbx_internships.ETL.StagingInputTargetLookup (TargetName, TargetSchema, TargetTable, LookupTableAlias)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
