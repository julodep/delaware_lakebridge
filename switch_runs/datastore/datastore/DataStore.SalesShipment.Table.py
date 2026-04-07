# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesShipment.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.SalesShipment.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the SalesShipment table in Unity Catalog
# --------------------------------------------------------------
#
# The source T‑SQL script defines a table `[DataStore].[SalesShipment]`
# with a mix of NVARCHAR and NUMERIC columns.  For Delta Lake we
# translate the data types as follows:
#
#   • NVARCHAR → STRING
#   • NUMERIC(p, s) → DECIMAL(p, s)
#
# Unity Catalog tables are fully‑qualified, so the table will be
# created as dbe_dbx_internships.datastore.SalesShipment.  The ON [PRIMARY]
# clause in T‑SQL is an engine‑specific hint that is irrelevant for
# Delta Lake, so it is omitted.
#
# After the create statement you can query the table normally:
#   spark.sql("SELECT * FROM `dbe_dbx_internships`.`datastore`.SalesShipment").show()
#
# -------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`SalesShipment` (
    CustPackingSlipCode               STRING   NOT NULL,
    CustPackingSlipLineNumber        DECIMAL(32,16) NOT NULL,
    CustPackingSlipLineNumberCombination STRING NOT NULL,
    SalesOrderCode                   STRING   NOT NULL,
    SalesInvoiceCode                 STRING   NOT NULL,
    CompanyCode                      STRING   NOT NULL,
    ProductCode                      STRING   NOT NULL,
    OrderCustomerCode                STRING   NOT NULL,
    CustomerCode                     STRING   NOT NULL,
    InventTransCode                  STRING   NOT NULL,
    InventDimCode                    STRING   NOT NULL,
    RequestedShippingDate            DATE,
    ConfirmedShippingDate            DATE,
    ActualDeliveryDate               DATE,
    SalesUnit                        STRING   NOT NULL,
    OrderedQuantity                  DECIMAL(32,6)  NOT NULL,
    DeliveredQuantity                DECIMAL(32,6)  NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
