# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBISalesOrderHeaderStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBISalesOrderHeaderStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------
# Create a table in Databricks that mirrors the T‑SQL definition of
# dbo.SMRBISalesOrderHeaderStaging.
#
# All column types have been mapped to the closest Spark‑SQL data types.
# Length specifications for VARCHAR/NVARCHAR are omitted because Spark
# stores strings without an explicit length limit.
#
# Constraints that are not supported in Delta Lake (e.g. PRIMARY KEY,
# statistics settings, IGNORE_DUP_KEY) are commented out with an
# explanatory note.  If strict uniqueness is required, it should be
# enforced at the application layer or via a batch ETL process.
# ------------------------------------------------------------------------------

# Use a fully‑qualified name for the target table.
catalog = 'dbe_dbx_internships'
schema  = 'dbo'
table   = 'SMRBISalesOrderHeaderStaging'

# COMMAND ----------

# Build the CREATE TABLE statement.
create_table_sql = f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`{table}` (
    DEFINITIONGROUP            STRING NOT NULL,
    EXECUTIONID                STRING NOT NULL,
    ISSELECTED                 INT    NOT NULL,
    TRANSFERSTATUS             INT    NOT NULL,
    SALESORDERNUMBER           STRING NOT NULL,
    ORDERINGCUSTOMERACCOUNTNUMBER STRING NOT NULL,
    INVOICECUSTOMERACCOUNTNUMBER  STRING NOT NULL,
    CURRENCYCODE               STRING NOT NULL,
    TOTALDISCOUNTPERCENTAGE    DECIMAL(32, 6) NOT NULL,
    DELIVERYMODECODE           STRING NOT NULL,
    DELIVERYTERMSCODE          STRING NOT NULL,
    PAYMENTTERMSNAME           STRING NOT NULL,
    CONFIRMEDRECEIPTDATE       TIMESTAMP NOT NULL,
    SALESORDERNAME             STRING NOT NULL,
    CONFIRMEDSHIPPINGDATE      TIMESTAMP NOT NULL,
    DELIVERYADDRESSCITY       STRING NOT NULL,
    DELIVERYADDRESSCOUNTRYREGIONID STRING NOT NULL,
    DELIVERYADDRESSSTREET      STRING NOT NULL,
    DELIVERYADDRESSSTREETNUMBER STRING NOT NULL,
    SALESTABLECREATEDDATETIME  TIMESTAMP NOT NULL,
    COMPANY                    STRING NOT NULL,
    DELIVERYADDRESSZIPCODE     STRING NOT NULL,
    DELIVERYDATE               TIMESTAMP NOT NULL,
    INVENTLOCATIONID           STRING NOT NULL,
    SALESTABLERECID            BIGINT NOT NULL,
    REQUESTEDRECEIPTDATE       TIMESTAMP NOT NULL,
    REQUESTEDSHIPPINGDATE      TIMESTAMP NOT NULL,
    VALIDFROM                  TIMESTAMP NOT NULL,
    VALIDTO                    TIMESTAMP NOT NULL,
    CUSTOMERREF                STRING NOT NULL,
    PURCHORDERFORMNUM          STRING NOT NULL,
    DLVREASON                  STRING NOT NULL,
    DOCUMENTSTATUS             INT    NOT NULL,
    RETURNITEMNUM              STRING NOT NULL,
    SALESORIGINID              STRING NOT NULL,
    RETURNREASONCODEID         STRING NOT NULL,
    RETAILCHANNELTABLE         BIGINT NOT NULL,
    SALESGROUP                 STRING NOT NULL,
    SALESSTATUS                INT    NOT NULL,
    SALESTYPE                  INT    NOT NULL,
    WORKERSALESTAKER           BIGINT NOT NULL,
    PAYMMODE                   STRING NOT NULL,
    DEFAULTDIMENSION           BIGINT NOT NULL,
    PARTITION                  STRING NOT NULL,
    DATAAREAID                 STRING NOT NULL,
    SYNCSTARTDATETIME          TIMESTAMP NOT NULL
    /* The following PRIMARY KEY constraint is not supported by Delta Lake
       and is therefore commented out.  If you need to enforce uniqueness,
       consider adding a unique index in ETL or using a CHANGE DATA
       capture solution. */
    /* PRIMARY KEY (EXECUTIONID, SALESORDERNUMBER, DATAAREAID, PARTITION) */
)
USING DELTA
"""

# COMMAND ----------

# Execute the creation.  If the table already exists we simply overwrite.
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`{table}`")
spark.sql(create_table_sql)

# COMMAND ----------

# Optional: verify the new schema
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`{table}`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
