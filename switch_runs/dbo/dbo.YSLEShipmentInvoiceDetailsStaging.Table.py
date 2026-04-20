# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.YSLEShipmentInvoiceDetailsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.YSLEShipmentInvoiceDetailsStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------
# 1️⃣  Create the staging table in the target catalog/schema
# --------------------------------------------
# The T‑SQL script sets SET options that are irrelevant in Spark,
# and declares a clustered primary key together with a number of
# table options that Delta Lake does **not** enforce.  We therefore
# create a plain Delta table with the same column list and data
# types and add a comment explaining the omitted constraints.
# --------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`YSLEShipmentInvoiceDetailsStaging` (
    -- column definitions ---------------------------------------------------
    DEFINITIONGROUP STRING  NOT NULL,          -- nvarchar(60)
    EXECUTIONID   STRING  NOT NULL,          -- nvarchar(90)
    ISSELECTED    INT     NOT NULL,          -- int
    TRANSFERSTATUS INT    NOT NULL,          -- int
    SHIPMENTID    STRING  NOT NULL,          -- nvarchar(20)
    LINENUM       DECIMAL(32,16) NOT NULL,  -- numeric(32,16)
    INVOICEDATE   TIMESTAMP NOT NULL,       -- datetime
    VENDINVOICEID STRING  NOT NULL,          -- nvarchar(20)
    CUSTINVOICEID STRING  NOT NULL,          -- nvarchar(20)
    CURRENCYCODE STRING  NOT NULL,          -- nvarchar(3)
    AMOUNT        DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    VOUCHER       STRING  NOT NULL,          -- nvarchar(20)
    PARTITION     STRING  NOT NULL,          -- nvarchar(20)
    DATAAREAID    STRING  NOT NULL,          -- nvarchar(4)
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,  -- datetime
    RECID         BIGINT NOT NULL           -- bigint
)
USING DELTA
COMMENT 'Primary key constraints from the original T‑SQL are omitted because Delta Lake does not enforce them.'
""")

# COMMAND ----------

# --------------------------------------------
# 2️⃣  (Optional) Verify that the table was created
# --------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`YSLEShipmentInvoiceDetailsStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1234)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`YSLEShipmentInvoiceDetailsStaging` (     -- column definitions ---------------------------------------------------     DEFINITIONGROUP STRING  NOT NULL,          -- nvarchar(60)     EXECUTIONID   STRING  NOT NULL,          -- nvarchar(90)     ISSELECTED    INT     NOT NULL,          -- int     TRANSFERSTATUS INT    NOT NULL,          -- int     SHIPMENTID    STRING  NOT NULL,          -- nvarchar(20)     LINENUM       DECIMAL(32,16) NOT NULL,  -- numeric(32,16)     INVOICEDATE   TIMESTAMP NOT NULL,       -- datetime     VENDINVOICEID STRING  NOT NULL,          -- nvarchar(20)     CUSTINVOICEID STRING  NOT NULL,          -- nvarchar(20)     CURRENCYCODE STRING  NOT NULL,          -- nvarchar(3)     AMOUNT        DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     VOUCHER       STRING  NOT NULL,          -- nvarchar(20)     PARTITION     STRING  NOT NULL,          -- nvarchar(20)     DATAAREAID    STRING  NOT NULL,          -- nvarchar(4)     SYNCSTARTDATETIME TIMESTAMP NOT NULL,  -- datetime     RECID         BIGINT NOT NULL           -- bigint ) USING DELTA COMMENT 'Primary key constraints from the original T‑SQL are omitted because Delta Lake does not enforce them.'
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
