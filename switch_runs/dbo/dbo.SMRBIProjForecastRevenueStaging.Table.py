# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjForecastRevenueStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjForecastRevenueStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the staging table SMRBIProjForecastRevenueStaging
# on the target Unity Catalog dbe_dbx_internships.dbo.
# ------------------------------------------------------------
# NOTE: Delta Lake (the default format in Databricks) does not support
# classic PRIMARY KEY constraints or clustered indexes that exist in
# SQL Server.  The primary‑key definition is therefore omitted; the
# columns that were part of the key are documented in the table comment.
#
# Column type mapping (T‑SQL -> Spark SQL):
#   - NVARCHAR   -> STRING
#   - INT        -> INT
#   - DECIMAL(32,6) -> DECIMAL(32,6)
#   - DATETIME   -> TIMESTAMP
#   - BIGINT     -> BIGINT
# ------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastRevenueStaging` (
  DEFINITIONGROUP STRING NOT NULL,
  EXECUTIONID STRING NOT NULL,
  ISSELECTED INT NOT NULL,
  TRANSFERSTATUS INT NOT NULL,
  CATEGORY STRING NOT NULL,
  SALESCURRENCY STRING NOT NULL,
  LINEPROPERTY STRING NOT NULL,
  FORECASTMODEL STRING NOT NULL,
  PROJECTID STRING NOT NULL,
  QTY DECIMAL(32,6) NOT NULL,
  SALESPRICE DECIMAL(32,6) NOT NULL,
  PROJECTDATE TIMESTAMP NOT NULL,
  TRANSACTIONID STRING NOT NULL,
  DESCRIPTION STRING NOT NULL,
  COMPANY STRING NOT NULL,
  WORKER BIGINT NOT NULL,
  `PARTITION` STRING NOT NULL,
  DATAAREAID STRING NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Primary key (EXECUTIONID, TRANSACTIONID, DATAAREAID, PARTITION) is not enforced in Delta Lake. It is kept here for reference.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
