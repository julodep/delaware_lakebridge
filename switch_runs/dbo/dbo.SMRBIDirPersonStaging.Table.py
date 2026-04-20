# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDirPersonStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDirPersonStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Create the staging table in the Databricks catalog and schema.
# --------------------------------------------------------------------------
#    - datatypes: NVARCHAR -> STRING, INT -> INT, BIGINT -> LONG, DATETIME -> TIMESTAMP
#    - no SET statements are needed in Databricks – they are T‑SQL specific
#    - the table is created (or replaced if you wish) as a Delta table
# --------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIDirPersonStaging`
(
    DEFINITIONGROUP   STRING   NOT NULL,
    EXECUTIONID       STRING   NOT NULL,
    ISSELECTED        INT      NOT NULL,
    TRANSFERSTATUS    INT      NOT NULL,
    PARTYNUMBER       STRING   NOT NULL,
    USER_             STRING   NOT NULL,
    DIRPERSONRECID    LONG     NOT NULL,
    VALIDTO           TIMESTAMP NOT NULL,
    VALIDFROM         TIMESTAMP NOT NULL,
    PARTITION         STRING   NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID             LONG     NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
