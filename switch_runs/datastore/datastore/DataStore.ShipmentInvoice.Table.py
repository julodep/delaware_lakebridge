# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ShipmentInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ShipmentInvoice.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
#  Create the ShipmentInvoice table in Databricks (Unity Catalog)
#  -----------------------------------------------------------
#  Every object reference is fully‑qualified:
#  `dbe_dbx_internships`.`datastore`.`ShipmentInvoice`
#  All T‑SQL data types are mapped to Spark SQL types:
#    - NVARCHAR(_)         → STRING
#    - NUMERIC(p,s)        → DECIMAL(p,s)
#    - DATE                → DATE
#    - NOT NULL constraints are retained
#  The table is materialised as a Delta Lake table so that it can be
#  queried, modified, and snapshot‑traced natively in Databricks.
# --------------------------------------------------------------------

# Replace `dbe_dbx_internships` and `datastore` with your actual catalog and schema names
spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`ShipmentInvoice` (
  BusinessOwnerCode          STRING NOT NULL,
  CompanyCode                STRING NOT NULL,
  CustomerCode               STRING NOT NULL,
  DepartmentCode             STRING NOT NULL,
  DestinationAgentCode       STRING NOT NULL,
  JobOwnerCode               STRING NOT NULL,
  LineOfBusinessCode         STRING NOT NULL,
  ModeOfTransportCode        STRING NOT NULL,
  PurchaseInvoiceCode        STRING NOT NULL,
  SalesInvoiceCode           STRING NOT NULL,
  ShipmentContractCode       STRING NOT NULL,
  TransactionCurrencyCode    STRING NOT NULL,
  MasterBillOfLading         STRING NOT NULL,
  HouseBillOfLading          STRING NOT NULL,
  PortOfDestination          STRING NOT NULL,
  PortOfOrigin               STRING NOT NULL,
  Etd                        DATE,
  Eta                        DATE,
  Description                STRING NOT NULL,
  Branch                     STRING NOT NULL,
  Remark                     STRING NOT NULL,
  ShipmentInvoiceLineNumber  DECIMAL(32,16) NOT NULL,
  ShipmentInvoiceDate        DATE,
  Amount                     DECIMAL(32,6) NOT NULL,
  Voucher                    STRING NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
