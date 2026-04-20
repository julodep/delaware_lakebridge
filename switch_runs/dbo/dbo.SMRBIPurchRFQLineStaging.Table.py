# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchRFQLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchRFQLineStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------------------
# Create the staging table in Delta Lake
# ---------------------------------------------------------------------------

# Mapping summary
# ----------------------------------------------------------------------------
# T-SQL type → Spark type
# ---------------------------------------
# NVARCHAR / VARCHAR          → STRING
# INT                        → INT
# BIGINT                     → LONG
# DATETIME                   → TIMESTAMP
# NUMERIC(p, s) / DECIMAL(p,s) → DECIMAL(p,s)
# ---------------------------------------
# Primary key / index definitions are NOT supported in Delta Lake;
# they are kept as a commented reference.

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPurchRFQLineStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    COMPANY STRING NOT NULL,
    DELIVERYDATE TIMESTAMP NOT NULL,
    EXPIRYDATETIME TIMESTAMP NOT NULL,
    INVENTDIMID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    LINENUM DECIMAL(32,16) NOT NULL,
    RFQCASELINELINENUMBER LONG NOT NULL,
    RFQID STRING NOT NULL,
    STATUS INT NOT NULL,
    ITEMNAME STRING NOT NULL,
    PURCHUNIT STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- PRIMARY KEY constraint removed - Delta Lake does not enforce unique keys.
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 789)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `catalog`.`schema`.`SMRBIPurchRFQLineStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     CURRENCYCODE STRING NOT NULL,     COMPANY STRING NOT NULL,     DELIVERYDATE TIMESTAMP NOT NULL,     EXPIRYDATETIME TIMESTAMP NOT NULL,     INVENTDIMID STRING NOT NULL,     ITEMID STRING NOT NULL,     LINENUM DECIMAL(32,16) NOT NULL,     RFQCASELINELINENUMBER LONG NOT NULL,     RFQID STRING NOT NULL,     STATUS INT NOT NULL,     ITEMNAME STRING NOT NULL,     PURCHUNIT STRING NOT NULL,     PARTITION STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- PRIMARY KEY constraint removed - Delta Lake does not enforce unique keys. )
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
