# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdJournalProdStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdJournalProdStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the temporary staging table `SMRBIProdJournalProdStaging`
# ------------------------------------------------------------------
# The original T‑SQL defines a table with a clustered primary key.  
# Delta Lake (the default storage format in Databricks) does not support
# row‑level primary key constraints, so we omit the PK clause.  
#
# All column data types are mapped to their Spark SQL equivalents:
#   - NVARCHAR(x)  →  STRING
#   - INT          →  INT
#   - NUMERIC(p,s) →  DECIMAL(p,s)
#   - DATETIME     →  TIMESTAMP
#
# The table is created as a Delta table in the catalog and schema
# provided by the placeholders `dbe_dbx_internships` and `dbo`.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdJournalProdStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COMPANY STRING NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    JOURNALID STRING NOT NULL,
    LINENUM DECIMAL(32,16) NOT NULL,
    PRODFINISHED INT NOT NULL,
    PRODID STRING NOT NULL,
    QTYGOOD DECIMAL(32,6) NOT NULL,
    PRODJOURNALPRODRECID BIGINT NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
