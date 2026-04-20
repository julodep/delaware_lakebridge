# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIUnitOfMeasureStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIUnitOfMeasureStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------------
# NOTE: This notebook creates the table `SMRBIUnitOfMeasureStaging` in the specified
#       Databricks catalog and schema.  All column data types have been mapped from
#       T‑SQL to Spark SQL types.  Primary‑key and other index/constraint clauses
#       are omitted because Delta Lake (and Spark SQL in general) does not support
#       those constructs.  If you need enforcement of uniqueness, consider adding a
#       unique index at the application level or using a Delta Lake transaction
#       to check duplicates before inserts.
# ------------------------------------------------------------------------------------

# 1.  Drop the table if it already exists to avoid creation errors
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIUnitOfMeasureStaging`")

# COMMAND ----------

# 2.  Create the table with the same column list as in the T‑SQL statement
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIUnitOfMeasureStaging` (
    DEFINITIONGROUP     STRING  NOT NULL,
    EXECUTIONID         STRING  NOT NULL,
    ISSELECTED          INT     NOT NULL,
    TRANSFERSTATUS      INT     NOT NULL,
    DECIMALPRECISION    INT     NOT NULL,
    UNITSYMBOL          STRING  NOT NULL,
    SYSTEMOFUNITS       INT     NOT NULL,
    UNITCLASS           INT     NOT NULL,
    UNITOFS_MEASURERECID BIGINT  NOT NULL,
    PARTITION           STRING  NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL,
    RECID               BIGINT  NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
