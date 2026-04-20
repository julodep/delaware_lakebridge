# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchRFQTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchRFQTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table `SMRBIPurchRFQTableStaging` in the
#  target Unity Catalog catalog and schema.
#
#  NOTE:
#  * T‑SQL primary key constraints, statistics options and
#    ON PRIMARY clauses are not supported by Delta Lake / Spark SQL.
#    They are omitted because Spark does NOT enforce primary key
#    constraints and the corresponding DDL fragments would cause a
#    syntax error.
#  * All identifiers are fully‑qualified as
#    `dbe_dbx_internships`.`dbo`.`<object_name>` per the requirement.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPurchRFQTableStaging` (
    DEFINITIONGROUP STRING      NOT NULL,
    EXECUTIONID   STRING      NOT NULL,
    ISSELECTED    INT        NOT NULL,
    TRANSFERSTATUS INT      NOT NULL,
    COMPANY       STRING     NOT NULL,
    DLVMODE       STRING     NOT NULL,
    DLVTERM       STRING     NOT NULL,
    RFQCASEID     STRING     NOT NULL,
    RFQID         STRING     NOT NULL,
    RFQNAME       STRING     NOT NULL,
    VENDACCOUNT   STRING     NOT NULL,
    PURCHRFQTABLECREATEDDATETIME TIMESTAMP NOT NULL,
    PARTITION     STRING     NOT NULL,
    DATAAREAID    STRING     NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
