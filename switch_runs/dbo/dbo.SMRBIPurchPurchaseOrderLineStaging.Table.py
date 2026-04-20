# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchPurchaseOrderLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchPurchaseOrderLineStaging.Table.sql`

# COMMAND ----------

# Create the delta table `SMRBIPurchPurchaseOrderLineStaging`
spark.sql("""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIPurchPurchaseOrderLineStaging` (
    -- Column definitions ----------------------------------------------------
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    PURCHASEORDERLINESTATUS INT NOT NULL,
    CONFIRMEDDELIVERYDATE TIMESTAMP NOT NULL,
    REQUESTEDDELIVERYDATE TIMESTAMP NOT NULL,
    LINEAMOUNT DECIMAL(32,6) NOT NULL,
    LINENUMBER BIGINT NOT NULL,
    PURCHASEPRICEQUANTITY DECIMAL(32,12) NOT NULL,
    PURCHASEORDERNUMBER STRING NOT NULL,
    PURCHASEPRICE DECIMAL(32,6) NOT NULL,
    ORDEREDPURCHASEQUANTITY DECIMAL(32,6) NOT NULL,
    PURCHASEUNITSYMBOL STRING NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTREFTRANSID STRING NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    REMAINPURCHPHYSICAL DECIMAL(32,6) NOT NULL,
    COMPANY STRING NOT NULL,
    PURCHLINECREATEDDATETIME TIMESTAMP NOT NULL,
    PURCHLINERECID BIGINT NOT NULL,
    PRICEUNIT DECIMAL(32,12) NOT NULL,
    LINEDISCOUNTPERCENTAGE DECIMAL(32,6) NOT NULL,
    LINEDISCOUNTAMOUNT DECIMAL(32,6) NOT NULL,
    ITEMNUMBER STRING NOT NULL,
    TAXITEMGROUP STRING NOT NULL,
    TAXGROUP STRING NOT NULL,
    NAME STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL

    -- -----------------------------------------------
    -- NOTE: PRIMARY KEY constraint is omitted because
    -- Delta Lake does not enforce uniqueness.
    -- The original T‑SQL PK clause has been commented
    -- out for reference only:
    --
    -- CONSTRAINT PK_SMRBIPurchPurchaseOrderLineStaging
    --     PRIMARY KEY (EXECUTIONID, LINENUMBER, PURCHASEORDERNUMBER,
    --                  DATAAREAID, PARTITION)
    -- -----------------------------------------------
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1894)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`SMRBIPurchPurchaseOrderLineStaging` (     -- Column definitions ----------------------------------------------------     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     PURCHASEORDERLINESTATUS INT NOT NULL,     CONFIRMEDDELIVERYDATE TIMESTAMP NOT NULL,     REQUESTEDDELIVERYDATE TIMESTAMP NOT NULL,     LINEAMOUNT DECIMAL(32,6) NOT NULL,     LINENUMBER BIGINT NOT NULL,     PURCHASEPRICEQUANTITY DECIMAL(32,12) NOT NULL,     PURCHASEORDERNUMBER STRING NOT NULL,     PURCHASEPRICE DECIMAL(32,6) NOT NULL,     ORDEREDPURCHASEQUANTITY DECIMAL(32,6) NOT NULL,     PURCHASEUNITSYMBOL STRING NOT NULL,     CURRENCYCODE STRING NOT NULL,     INVENTDIMID STRING NOT NULL,     INVENTREFTRANSID STRING NOT NULL,     INVENTTRANSID STRING NOT NULL,     REMAINPURCHPHYSICAL DECIMAL(32,6) NOT NULL,     COMPANY STRING NOT NULL,     PURCHLINECREATEDDATETIME TIMESTAMP NOT NULL,     PURCHLINERECID BIGINT NOT NULL,     PRICEUNIT DECIMAL(32,12) NOT NULL,     LINEDISCOUNTPERCENTAGE DECIMAL(32,6) NOT NULL,     LINEDISCOUNTAMOUNT DECIMAL(32,6) NOT NULL,     ITEMNUMBER STRING NOT NULL,     TAXITEMGROUP STRING NOT NULL,     TAXGROUP STRING NOT NULL,     NAME STRING NOT NULL,     `PARTITION` STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL      -- -----------------------------------------------     -- NOTE: PRIMARY KEY constraint is omitted because     -- Delta Lake does not enforce uniqueness.     -- The original T‑SQL PK clause has been commented     -- out for reference only:     --     -- CONSTRAINT PK_SMRBIPurchPurchaseOrderLineStaging     --     PRIMARY KEY (EXECUTIONID, LINENUMBER, PURCHASEORDERNUMBER,     --                  DATAAREAID, PARTITION)     -- ----------------------------------------------- ) USING delta
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
