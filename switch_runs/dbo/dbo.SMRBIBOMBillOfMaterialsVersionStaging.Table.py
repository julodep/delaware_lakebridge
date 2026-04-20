# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBOMBillOfMaterialsVersionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBOMBillOfMaterialsVersionStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Databricks notebook – create staging table
# --------------------------------------------------------------
# Create an empty staging table with the desired schema.
# Spark SQL supports CREATE OR REPLACE TABLE without an AS clause.
# The table will be created with the specified columns and no data.
# --------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMBillOfMaterialsVersionStaging` (
  DEFINITIONGROUP          STRING   NOT NULL,
  EXECUTIONID             STRING   NOT NULL,
  ISSELECTED              INT      NOT NULL,
  TRANSFERSTATUS          INT      NOT NULL,
  BOMID                   STRING   NOT NULL,
  COMPANY                 STRING   NOT NULL,
  FROMQUANTITY            DECIMAL(32,6) NOT NULL,
  ISACTIVE                INT      NOT NULL,
  ISAPPROVED              INT      NOT NULL,
  MANUFACTUREDITEMNUMBER  STRING   NOT NULL,
  VALIDFROMDATE           TIMESTAMP NOT NULL,
  VALIDTODATE             TIMESTAMP NOT NULL,
  BOMVERSIONRECID         BIGINT   NOT NULL,
  CONFIGID                STRING   NOT NULL,
  PARTITION               STRING   NOT NULL,
  DATAAREAID              STRING   NOT NULL,
  SYNCSTARTDATETIME       TIMESTAMP NOT NULL,
  RECID                   BIGINT   NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
