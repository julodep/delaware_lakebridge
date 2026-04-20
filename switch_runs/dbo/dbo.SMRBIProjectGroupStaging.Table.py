# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjectGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjectGroupStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------
catalog = "default"          # replace with your catalog name
schema  = "my_schema"        # replace with your schema name
path    = "/mnt/delta/smrbi_project_group_staging"  # replace with your storage path

# COMMAND ----------

# -------------------------------------------------------------
# Create the staging table in Delta Lake
# -------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjectGroupStaging` (
    DEFINITIONGROUP      STRING,
    EXECUTIONID          STRING,
    ISSELECTED           INT,
    TRANSFERSTATUS       INT,
    NAME                 STRING,
    PROJECTGROUP         STRING,
    COMPANY              STRING,
    PARTITION            STRING,
    DATAAREAID           STRING,
    SYNCSTARTDATETIME    TIMESTAMP
)
USING DELTA
OPTIONS ('path' '{path}');
""")

# COMMAND ----------

# -------------------------------------------------------------
# Comment: Primary key definition
# -------------------------------------------------------------
# T‑SQL defined a clustered primary key on (EXECUTIONID, PROJECTGROUP,
# DATAAREAID, PARTITION). Delta Lake does not enforce primary keys by
# default.  If you need uniqueness you can:
# 1. Add a Delta Lake MERGE with an anti-duplicate condition before inserting.
# 2. Use an external database or catalog that supports constraints.
# -------------------------------------------------------------

# -------------------------------------------------------------
# Optional: Verify the table was created
# -------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjectGroupStaging`;") \
     .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near '`default`'. SQLSTATE: 42601 (line 1, pos 33)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN DESCRIBE TABLE IF EXISTS `default`.`my_schema`.`SMRBIProjectGroupStaging`;
# MAGIC ---------------------------------^^^
# MAGIC
# MAGIC ```
