# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdBOMStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdBOMStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBIProdBOMStaging` in the specified
# catalog and schema.
#
# - All column data types are mapped from T‑SQL to Spark/Delta
#   types (NVARCHAR → STRING, NUMERIC → DECIMAL, BIGINT → BIGINT,
#   INT → INT, DATETIME → TIMESTAMP).
# - Primary‑key constraints are not supported in Delta Lake.
#   We keep the definition commented out for reference.
# - The table will be created as a Delta table; Spark will
#   automatically register it as a managed table under the given
#   catalog/schema.
# ------------------------------------------------------------------

spark.sql(f"""
    CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdBOMStaging` (
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID STRING NOT NULL,
        ISSELECTED INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        BOMID STRING NOT NULL,
        BOMQTY DECIMAL(32,6) NOT NULL,
        BOMQTYSERIE DECIMAL(32,6) NOT NULL,
        BOMREFRECID BIGINT NOT NULL,
        CALCULATION INT NOT NULL,
        COMPANY STRING NOT NULL,
        INVENTDIMID STRING NOT NULL,
        INVENTREFID STRING NOT NULL,
        INVENTREFTRANSID STRING NOT NULL,
        ITEMID STRING NOT NULL,
        LINENUM DECIMAL(32,16) NOT NULL,
        OPRNUM INT NOT NULL,
        PRODID STRING NOT NULL,
        QTYINVENTCALC DECIMAL(32,6) NOT NULL,
        SCRAPCONST DECIMAL(32,6) NOT NULL,
        SCRAPVAR DECIMAL(32,6) NOT NULL,
        UNITID STRING NOT NULL,
        PRODBOMRECID BIGINT NOT NULL,
        PARTITION STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL,
        RECID BIGINT NOT NULL
    )
    USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
