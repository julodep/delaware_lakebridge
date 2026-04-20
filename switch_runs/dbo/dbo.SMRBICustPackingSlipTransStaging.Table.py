# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustPackingSlipTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustPackingSlipTransStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------
# 1️⃣  Create the staging table `SMRBICustPackingSlipTransStaging`
# ---------------------------------------------------------------
# Databricks (Delta Lake) does not enforce PRIMARY KEY constraints,
# so we keep the constraint declaration in a comment for reference
# only.  The table’s column types are mapped from T‑SQL to Spark SQL
# data types as described in the guidelines.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustPackingSlipTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT     NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    CURRENCYCODE  STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    DELIVERYDATE  TIMESTAMP NOT NULL,
    INVENTDIMID   STRING NOT NULL,
    INVENTREFTRANSID STRING NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    INVOICETRANSREFRECID BIGINT NOT NULL,
    ITEMID        STRING NOT NULL,
    LINENUM       DECIMAL(32,16) NOT NULL,
    ORDERED       DECIMAL(32,6)  NOT NULL,
    ORIGSALESID   STRING NOT NULL,
    PACKINGSLIPID STRING NOT NULL,
    PRICEUNIT     DECIMAL(32,12) NOT NULL,
    QTY           DECIMAL(32,6)  NOT NULL,
    REMAIN        DECIMAL(32,6)  NOT NULL,
    SALESID       STRING NOT NULL,
    SALESLINESHIPPINGDATECONFIRMED TIMESTAMP NOT NULL,
    SALESLINESHIPPINGDATEREQUESTED TIMESTAMP NOT NULL,
    SALESUNIT     STRING NOT NULL,
    CUSTPACKINGSLIPTRANSRECID BIGINT NOT NULL,
    `PARTITION`   STRING NOT NULL,   -- escaped reserved keyword
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID         BIGINT NOT NULL,
    -- PRIMARY KEY (EXECUTIONID, CUSTPACKINGSLIPTRANSRECID, DATAAREAID, PARTITION)
    -- Delta Lake does not enforce this constraint, it is kept only for
    -- reference and for compatibility with downstream tools that
    -- might read this metadata.
)
""")

# COMMAND ----------

# --------------------------------------
# 2️⃣  Verify that the table was created
# --------------------------------------
print("Table created successfully.")
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBICustPackingSlipTransStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1482)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBICustPackingSlipTransStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID   STRING NOT NULL,     ISSELECTED    INT     NOT NULL,     TRANSFERSTATUS INT    NOT NULL,     CURRENCYCODE  STRING NOT NULL,     COMPANY       STRING NOT NULL,     DELIVERYDATE  TIMESTAMP NOT NULL,     INVENTDIMID   STRING NOT NULL,     INVENTREFTRANSID STRING NOT NULL,     INVENTTRANSID STRING NOT NULL,     INVOICETRANSREFRECID BIGINT NOT NULL,     ITEMID        STRING NOT NULL,     LINENUM       DECIMAL(32,16) NOT NULL,     ORDERED       DECIMAL(32,6)  NOT NULL,     ORIGSALESID   STRING NOT NULL,     PACKINGSLIPID STRING NOT NULL,     PRICEUNIT     DECIMAL(32,12) NOT NULL,     QTY           DECIMAL(32,6)  NOT NULL,     REMAIN        DECIMAL(32,6)  NOT NULL,     SALESID       STRING NOT NULL,     SALESLINESHIPPINGDATECONFIRMED TIMESTAMP NOT NULL,     SALESLINESHIPPINGDATEREQUESTED TIMESTAMP NOT NULL,     SALESUNIT     STRING NOT NULL,     CUSTPACKINGSLIPTRANSRECID BIGINT NOT NULL,     `PARTITION`   STRING NOT NULL,   -- escaped reserved keyword     DATAAREAID    STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID         BIGINT NOT NULL,     -- PRIMARY KEY (EXECUTIONID, CUSTPACKINGSLIPTRANSRECID, DATAAREAID, PARTITION)     -- Delta Lake does not enforce this constraint, it is kept only for     -- reference and for compatibility with downstream tools that     -- might read this metadata. )
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
