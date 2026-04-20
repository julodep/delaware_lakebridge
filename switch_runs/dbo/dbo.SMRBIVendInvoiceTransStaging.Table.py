# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendInvoiceTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendInvoiceTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------
# NOTE: Translated from the T‑SQL statement
#
# * ANSI_NULLS and QUOTED_IDENTIFIER are Microsoft SQL Server options that have no
#   equivalent in Spark.  They are therefore omitted.
#
# * The original table definition includes a composite PRIMARY KEY spanning
#   (EXECUTIONID, VENDINVOICETRANSRECID, DATAAREAID, PARTITION).  Delta Lake
#   (the underlying storage used by Databricks) does not support primary keys
#   or other indexes.  The constraint is omitted in the CREATE TABLE statement
#   and the column list is otherwise unchanged.
#
# * All T‑SQL data types are mapped to their closest Spark SQL/Spark‑Delta
#   equivalents:
#     nvarchar(N)  -> STRING
#     datetime     -> TIMESTAMP
#     numeric(p,s) -> DECIMAL(p,s)
#     int          -> INT
#     bigint       -> BIGINT
# ------------------------------------------------------------------------------

# Create the Delta table in the desired catalog and schema
create_table_sql = f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIVendInvoiceTransStaging (
  DEFINITIONGROUP STRING,
  EXECUTIONID STRING,
  ISSELECTED INT,
  TRANSFERSTATUS INT,
  CURRENCYCODE STRING,
  COMPANY STRING,
  DESCRIPTION STRING,
  INTERNALINVOICEID STRING,
  INVENTDIMID STRING,
  INVENTTRANSID STRING,
  INVOICEDATE TIMESTAMP,
  INVOICEID STRING,
  ITEMID STRING,
  LINEAMOUNT DECIMAL(32,6),
  LINEAMOUNTMST DECIMAL(32,6),
  LINEDISC DECIMAL(32,6),
  LINENUM DECIMAL(32,16),
  LINEPERCENT DECIMAL(32,6),
  ORIGPURCHID STRING,
  PRICEUNIT DECIMAL(32,12),
  PURCHPRICE DECIMAL(32,6),
  PURCHUNIT STRING,
  QTY DECIMAL(32,6),
  VENDINVOICETRANSRECID BIGINT,
  TAXWRITECODE STRING,
  DEFAULTDIMENSIONDISPLAYVALUE STRING,
  LEDGERDIMENSIONDISPLAYVALUE STRING,
  VENDINVOICETRANSDIMENSION BIGINT,
  DEFAULTDIMENSION BIGINT,
  PARTITION STRING,
  DATAAREAID STRING,
  SYNCSTARTDATETIME TIMESTAMP,
  RECID BIGINT
)
USING DELTA
"""

# COMMAND ----------

spark.sql(create_table_sql)

# COMMAND ----------

# ------------------------------------------------------------------
# OPTIONAL: Log creation for debugging purposes
print("Delta table SMRBIVendInvoiceTransStaging created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
