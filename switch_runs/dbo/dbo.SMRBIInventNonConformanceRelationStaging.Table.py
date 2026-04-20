# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventNonConformanceRelationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventNonConformanceRelationStaging.Table.sql`

# COMMAND ----------

# --------------  CREATE TABLE  -----------------------
# The original T‑SQL statement creates a table in the dbo schema.  
# In Databricks we will create a Delta table that lives in the   |
# `dbe_dbx_internships`.`dbo` catalog‑schema context.  The columns
# are mapped to Spark SQL data types according to the guidelines:
#
#  NVARCHAR(n)      → STRING
#  INT              → INT
#  DATETIME         → TIMESTAMP
#
# Delta Lake (the default format in Databricks) does not enforce
# primary‑key constraints, so the PK definition is omitted.  If
# you need uniqueness checks you must enforce them via queries
# or Delta Lake schema evolution.
#
# The fully‑qualified table name is used everywhere.  Replace
# `dbe_dbx_internships` and `dbo` with your actual catalog and schema
# names before running the notebook.

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventNonConformanceRelationStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    INVENTNONCONFORMANCEID STRING,
    INVENTNONCONFORMANCEIDREF STRING,
    PARTITION STRING,
    SYNCSTARTDATETIME TIMESTAMP
);
""")

# COMMAND ----------

# --------------  (Optional) Verify the table  -----------------------
spark.sql("DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIInventNonConformanceRelationStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
