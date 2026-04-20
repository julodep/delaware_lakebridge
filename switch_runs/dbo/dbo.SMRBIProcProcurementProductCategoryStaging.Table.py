# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProcProcurementProductCategoryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProcProcurementProductCategoryStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------------------
#  Databricks Notebook – Translating a T‑SQL CREATE TABLE (STAGING TABLE)
# ---------------------------------------------------------------------------
# All references are fully‑qualified:  dbe_dbx_internships.dbo.{table}
# Primary‑key and clustering information are omitted because
# Delta Lake (the storage format used by Databricks) does not support
# those constraints natively.  If you need uniqueness guarantees,
# enforce them in application logic or by adding a unique index
# on a managed Delta table (using a transactional snapshot or a
# separate validation job).
# ---------------------------------------------------------------------------

# 1. Clean up any existing table so the notebook is idempotent
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProcProcurementProductCategoryStaging`")

# COMMAND ----------

# 2. Create the staging table with Spark SQL
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProcProcurementProductCategoryStaging` (
    `DEFINITIONGROUP`           STRING   NOT NULL,
    `EXECUTIONID`               STRING   NOT NULL,
    `ISSELECTED`                INT      NOT NULL,
    `TRANSFERSTATUS`            INT      NOT NULL,
    `CODE`                      STRING   NOT NULL,
    `PROCUREMENTCREATEDDATETIME` TIMESTAMP NOT NULL,
    `LEVEL_`                     BIGINT   NOT NULL,
    `PROCUREMENTMODIFIEDDATETIME` TIMESTAMP NOT NULL,
    `NAME`                      STRING   NOT NULL,
    `PARENTCATEGORY`            BIGINT   NOT NULL,
    `PROCUREMENTRECID`          BIGINT   NOT NULL,
    `PARTITION`                 STRING   NOT NULL,
    `SYNCSTARTDATETIME`         TIMESTAMP NOT NULL,
    `RECID`                     BIGINT   NOT NULL
    -- PRIMARY KEY (EXECUTIONID, PROCUREMENTRECID, PARTITION) is omitted.
    -- Delta does not enforce PKs; enforce uniqueness separately if required.
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 996)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProcProcurementProductCategoryStaging` (     `DEFINITIONGROUP`           STRING   NOT NULL,     `EXECUTIONID`               STRING   NOT NULL,     `ISSELECTED`                INT      NOT NULL,     `TRANSFERSTATUS`            INT      NOT NULL,     `CODE`                      STRING   NOT NULL,     `PROCUREMENTCREATEDDATETIME` TIMESTAMP NOT NULL,     `LEVEL_`                     BIGINT   NOT NULL,     `PROCUREMENTMODIFIEDDATETIME` TIMESTAMP NOT NULL,     `NAME`                      STRING   NOT NULL,     `PARENTCATEGORY`            BIGINT   NOT NULL,     `PROCUREMENTRECID`          BIGINT   NOT NULL,     `PARTITION`                 STRING   NOT NULL,     `SYNCSTARTDATETIME`         TIMESTAMP NOT NULL,     `RECID`                     BIGINT   NOT NULL     -- PRIMARY KEY (EXECUTIONID, PROCUREMENTRECID, PARTITION) is omitted.     -- Delta does not enforce PKs; enforce uniqueness separately if required. ) USING delta
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
