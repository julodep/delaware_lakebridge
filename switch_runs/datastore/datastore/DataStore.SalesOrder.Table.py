# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.SalesOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the SalesOrder table in Delta/Unity Catalog
# ------------------------------------------------------------------
#
# • All references are fully‑qualified – `dbe_dbx_internships`.`datastore`.  
# • T‑SQL column types are mapped to Spark SQL types per the guidance:
#     • NVARCHAR           → STRING
#     • NUMERIC(p,s)       → DECIMAL(p,s)
#     • BIGINT             → LONG
#     • DATE               → DATE
# • `NOT NULL` constraints are preserved where they existed in the T‑SQL
#    definition.  Columns that can be NULL are omitted from the NOT NULL
#    clause; in Delta these columns will be nullable by default.
# • `CREATE OR REPLACE TABLE` is used so the statement can be re‑run
#    (e.g. in a notebook re‑execution) without worrying about the
#    table already existing.
#
# --------------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`SalesOrder` (
    SalesOrderCode STRING NOT NULL,
    SalesOrderLineNumber DECIMAL(32,16) NOT NULL,
    SalesOrderLineNumberCombination STRING,
    OrderTransaction STRING,
    DeliveryAddress STRING NOT NULL,
    DocumentStatus STRING NOT NULL,
    HeaderRecId LONG NOT NULL,
    LineRecId LONG NOT NULL,
    DefaultDimension LONG NOT NULL,
    CompanyCode STRING,
    InventTransCode STRING NOT NULL,
    InventDimCode STRING NOT NULL,
    OrderCustomerCode STRING NOT NULL,
    CustomerCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    DeliveryModeCode STRING NOT NULL,
    PaymentTermsCode STRING NOT NULL,
    DeliveryTermsCode STRING NOT NULL,
    SalesOrderStatus STRING NOT NULL,
    TransactionCurrencyCode STRING,
    CreationDate DATE,
    RequestedShippingDate DATE,
    ConfirmedShippingDate DATE,
    RequestedDeliveryDate DATE,
    ConfirmedDeliveryDate DATE,
    FirstShipmentDate DATE,
    LastShipmentDate DATE,
    SalesUnit STRING NOT NULL,
    OrderedQuantity DECIMAL(32,6) NOT NULL,
    OrderedQuantityRemaining DECIMAL(32,6) NOT NULL,
    DeliveredQuantity DECIMAL(33,6),
    SalesPricePerUnitTC DECIMAL(38,6),
    GrossSalesTC DECIMAL(38,6),
    DiscountAmountTC DECIMAL(38,6),
    InvoicedSalesAmountTC DECIMAL(38,6),
    MarkupAmountTC DECIMAL(38,6),
    NetSalesTC DECIMAL(38,6)
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Verify creation (optional – for debugging or confirmation)
# ------------------------------------------------------------------
# If you run the following you should see a table description with
# the columns and their data types as defined above.
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`datastore`.`SalesOrder`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
