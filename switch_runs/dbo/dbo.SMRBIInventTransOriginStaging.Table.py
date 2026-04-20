# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTransOriginStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTransOriginStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table SMRBIInventTransOriginStaging in the target
# catalog and schema.  All column names are kept exactly as in the
# original T‑SQL, with data types translated to Spark SQL equivalents.
#
# Primary‑key and clustering options from the original T‑SQL are not
# supported in Delta Lake, so they are omitted here and a comment is
# added to explain the change.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventTransOriginStaging` (
    DEFINITIONGROUP          STRING,
    EXECUTIONID              STRING,
    ISSELECTED               INT,
    TRANSFERSTATUS           INT,
    INVENTTRANSID            STRING,
    ITEMID                   STRING,
    ITEMINVENTDIMID          STRING,
    REFERENCECATEGORY        INT,
    REFERENCEID              STRING,
    COMPANY                  STRING,
    INVENTTRANSORIGINRECID   LONG,
    PARTITION                STRING,
    DATAAREAID               STRING,
    SYNCSTARTDATETIME       TIMESTAMP,
    RECID                    LONG
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
