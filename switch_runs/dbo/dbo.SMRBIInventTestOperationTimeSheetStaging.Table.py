# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTestOperationTimeSheetStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTestOperationTimeSheetStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the staging table in Unity Catalog
# ------------------------------------------------------------------
# NOTE:
# ├─ **Data types** – SQL Server types have been mapped to the
# │                      equivalent Spark SQL types (see mapping rules).
# ├─ **Primary‑key / clustering** – Delta Lake (and Unity Catalog)
# │                      does not support T‑SQL clustering or
# │                      FULL‑text indexes.  We keep a commented‑out
# │                      PRIMARY‑KEY clause that can be enabled if
# │                      your environment supports Delta Lake
# │                      constraints.
# └─ **Other options** – The `WITH ( … )` clause in T‑SQL is specific
#    to SQL Server and therefore omitted.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventTestOperationTimeSheetStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COSTAMOUNT DECIMAL(32,6) NOT NULL,
    INVENTNONCONFORMANCEID STRING NOT NULL,
    INVOICEDSALESID STRING NOT NULL,
    LINENUM DECIMAL(32,16) NOT NULL,
    OPERATIONBILLED INT NOT NULL,
    OPERATIONDATE TIMESTAMP NOT NULL,
    OPERATIONHOURS DECIMAL(32,6) NOT NULL,
    WORKER BIGINT NOT NULL,
    COMPANY STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL,
    -- ------------------------------------------------------------------
    -- PRIMARY KEY definition (Delta Lake supports the constraint but does
    -- not enforce uniqueness by default).  Uncomment the line below if
    -- you want the constraint to be enforced.
    -- ------------------------------------------------------------------
    -- PRIMARY KEY (EXECUTIONID, COSTAMOUNT, INVENTNONCONFORMANCEID,
    --              INVOICEDSALESID, LINENUM, OPERATIONBILLED,
    --              OPERATIONDATE, OPERATIONHOURS, WORKER,
    --              DATAAREAID, `PARTITION`)
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Verify that the table exists (optional)
# ------------------------------------------------------------------
spark.sql(
    f"SHOW TABLES IN `dbe_dbx_internships`.`dbo` LIKE 'SMRBIInventTestOperationTimeSheetStaging'"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1298)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.`SMRBIInventTestOperationTimeSheetStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     COSTAMOUNT DECIMAL(32,6) NOT NULL,     INVENTNONCONFORMANCEID STRING NOT NULL,     INVOICEDSALESID STRING NOT NULL,     LINENUM DECIMAL(32,16) NOT NULL,     OPERATIONBILLED INT NOT NULL,     OPERATIONDATE TIMESTAMP NOT NULL,     OPERATIONHOURS DECIMAL(32,6) NOT NULL,     WORKER BIGINT NOT NULL,     COMPANY STRING NOT NULL,     `PARTITION` STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL,     -- ------------------------------------------------------------------     -- PRIMARY KEY definition (Delta Lake supports the constraint but does     -- not enforce uniqueness by default).  Uncomment the line below if     -- you want the constraint to be enforced.     -- ------------------------------------------------------------------     -- PRIMARY KEY (EXECUTIONID, COSTAMOUNT, INVENTNONCONFORMANCEID,     --              INVOICEDSALESID, LINENUM, OPERATIONBILLED,     --              OPERATIONDATE, OPERATIONHOURS, WORKER,     --              DATAAREAID, `PARTITION`) ) USING DELTA
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
