# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.YSLEShipmentDetailsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.YSLEShipmentDetailsStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the YSLEShipmentDetailsStaging table in Databricks
# ------------------------------------------------------------------
# Fully‑qualified identifiers for the catalog and schema.
# The original T‑SQL `PRIMARY KEY` clause has been omitted because
# Delta Lake does not support it.  All column constraints are retained
# but the `NOT NULL` syntax is optional and supported in Spark SQL.
# ------------------------------------------------------------------

# Replace the placeholders with your actual catalog and schema names
catalog = "your_catalog"   # e.g., "my_catalog"
schema  = "your_schema"    # e.g., "my_schema"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`YSLEShipmentDetailsStaging`
USING DELTA
(
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    SHIPMENT      STRING NOT NULL,
    LINEOFBUSINESSID STRING NOT NULL,
    CUSTACCOUNT  STRING NOT NULL,
    CUSTNAME     STRING NOT NULL,
    JOBOWNER     STRING NOT NULL,
    BUSINESSOWNER STRING NOT NULL,
    DESTINATIONAGENT STRING NOT NULL,
    BRANCH       STRING NOT NULL,
    DEPARTMENT   STRING NOT NULL,
    MASTERBILLOFLADING STRING NOT NULL,
    HOUSEBILLOFLADING STRING NOT NULL,
    PORTOFORIGIN STRING NOT NULL,
    PORTOFDESTINATION STRING NOT NULL,
    ETD          TIMESTAMP NOT NULL,
    ETA          TIMESTAMP NOT NULL,
    MODEOFTRANSPORTID STRING NOT NULL,
    DESCRIPTION  STRING NOT NULL,
    REMARK       STRING NOT NULL,
    UPDATEDATE   TIMESTAMP NOT NULL,
    PARTITION    STRING NOT NULL,
    DATAAREAID   STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID        BIGINT NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'DEFINITIONGROUP'. SQLSTATE: 42601 (line 1, pos 97)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `your_catalog`.`your_schema`.`YSLEShipmentDetailsStaging` USING DELTA (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID   STRING NOT NULL,     ISSELECTED    INT    NOT NULL,     TRANSFERSTATUS INT    NOT NULL,     SHIPMENT      STRING NOT NULL,     LINEOFBUSINESSID STRING NOT NULL,     CUSTACCOUNT  STRING NOT NULL,     CUSTNAME     STRING NOT NULL,     JOBOWNER     STRING NOT NULL,     BUSINESSOWNER STRING NOT NULL,     DESTINATIONAGENT STRING NOT NULL,     BRANCH       STRING NOT NULL,     DEPARTMENT   STRING NOT NULL,     MASTERBILLOFLADING STRING NOT NULL,     HOUSEBILLOFLADING STRING NOT NULL,     PORTOFORIGIN STRING NOT NULL,     PORTOFDESTINATION STRING NOT NULL,     ETD          TIMESTAMP NOT NULL,     ETA          TIMESTAMP NOT NULL,     MODEOFTRANSPORTID STRING NOT NULL,     DESCRIPTION  STRING NOT NULL,     REMARK       STRING NOT NULL,     UPDATEDATE   TIMESTAMP NOT NULL,     PARTITION    STRING NOT NULL,     DATAAREAID   STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID        BIGINT NOT NULL )
# MAGIC -------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
