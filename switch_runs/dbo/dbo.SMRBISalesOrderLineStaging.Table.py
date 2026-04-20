# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBISalesOrderLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBISalesOrderLineStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the staging table 'SMRBISalesOrderLineStaging' in the
# specified catalog and schema.  All column names and types are
# mapped to Delta‑Lake compatible equivalents.
# --------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBISalesOrderLineStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    SALESORDERNUMBER STRING NOT NULL,
    INVENTORYLOTID STRING NOT NULL,
    SALESORDERLINESTATUS INT NOT NULL,
    ITEMNUMBER STRING NOT NULL,
    CONFIRMEDRECEIPTDATE TIMESTAMP NOT NULL,
    REQUESTEDRECEIPTDATE TIMESTAMP NOT NULL,
    SALESPRICE DECIMAL(32,6) NOT NULL,
    ORDEREDSALESQUANTITY DECIMAL(32,6) NOT NULL,
    SALESUNITSYMBOL STRING NOT NULL,
    CONFIRMEDSHIPPINGDATE TIMESTAMP NOT NULL,
    REQUESTEDSHIPPINGDATE TIMESTAMP NOT NULL,
    INVENTDIMID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    SALESLINERECID BIGINT NOT NULL,
    INVENTREFTRANSID STRING NOT NULL,
    LINEAMOUNT DECIMAL(32,6) NOT NULL,
    LINEDISCOUNTAMOUNT DECIMAL(32,6) NOT NULL,
    LINEDISCOUNTPERCENTAGE DECIMAL(32,6) NOT NULL,
    LINENUM DECIMAL(32,16) NOT NULL,
    REMAINSALESPHYSICAL DECIMAL(32,6) NOT NULL,
    SALESPRICEQUANTITY DECIMAL(32,12) NOT NULL,
    DEFAULTDIMENSION BIGINT NOT NULL,
    DEFAULTDIMENSIONDISPLAYVALUE STRING NOT NULL,
    COSTPRICE DECIMAL(32,6) NOT NULL,
    EXTERNALITEMID STRING NOT NULL,
    ITEMBOMID STRING NOT NULL,
    NAME STRING NOT NULL,
    TAXGROUP STRING NOT NULL,
    TAXITEMGROUP STRING NOT NULL,
    RETAILVARIANTID STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
);
""")

# COMMAND ----------

# Verify that the table was created
spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`dbo` LIKE 'SMRBISalesOrderLineStaging'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
