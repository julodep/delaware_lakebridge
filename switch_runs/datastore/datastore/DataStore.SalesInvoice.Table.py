# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.SalesInvoice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣ Create the SalesInvoice table in the target catalog / schema
# ------------------------------------------------------------------
#    • All table and column names are quoted using back‑ticks (`) because
#      SQL Server allows square brackets ([]) while Spark SQL uses back‑ticks.
#    • Data‑type mapping:
#        • NVARCHAR → STRING
#        • NUMERIC(p,s) → DECIMAL(p,s)
#        • BIGINT → BIGINT
#        • INT → INT
#        • DATE → DATE
#    • Primary key or other constraints are omitted – Delta Lake manages
#      integrity through the Spark catalog.  If you need a primary key you
#      should add a unique constraint in a subsequent ALTER TABLE statement.
#
# 2️⃣ The statement is executed via spark.sql – this is the Databricks
#    preferred way to run DDL.  The ``CREATE OR REPLACE TABLE`` syntax
#    ensures that if the table already exists it will be dropped and
#    recreated with the desired schema.
#
# 3️⃣ Replace the placeholders `dbe_dbx_internships` and `datastore` with your
#    actual Unity Catalog catalog and schema names before running.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`SalesInvoice` (
    `SalesInvoiceCode`                     STRING      NOT NULL,
    `TransactionType`                     STRING,
    `SalesInvoiceLineNumber`              DECIMAL(32,16)  NOT NULL,
    `SalesInvoiceLineNumberCombination`  STRING,
    `HeaderRecId`                        BIGINT      NOT NULL,
    `LineRecId`                          BIGINT      NOT NULL,
    `SalesOrderCode`                    STRING      NOT NULL,
    `InventTransCode`                   STRING      NOT NULL,
    `InventDimCode`                    STRING      NOT NULL,
    `TaxWriteCode`                      INT,
    `SalesOrderStatus`                  STRING,
    `CompanyCode`                       STRING,
    `ProductCode`                      STRING      NOT NULL,
    `OrderCustomerCode`                STRING      NOT NULL,
    `CustomerCode`                     STRING      NOT NULL,
    `DeliveryModeCode`                 STRING      NOT NULL,
    `PaymentTermsCode`                 STRING      NOT NULL,
    `DeliveryTermsCode`                STRING      NOT NULL,
    `TransactionCurrencyCode`          STRING,
    `LedgerCode`                       STRING      NOT NULL,
    `OrigSalesOrderId`                 STRING      NOT NULL,
    `InvoiceDate`                       DATE,
    `RequestedDeliveryDate`             DATE,
    `ConfirmedDeliveryDate`             DATE,
    `SalesUnit`                        STRING      NOT NULL,
    `InvoicedQuantity`                 DECIMAL(32,6)   NOT NULL,
    `SalesPricePerUnitTC`              DECIMAL(38,6),
    `GrossSalesTC`                    DECIMAL(38,6),
    `DiscountAmountTC`                DECIMAL(38,6),
    `InvoicedSalesAmountTC`           DECIMAL(38,6),
    `MarkupAmountTC`                  DECIMAL(38,6),
    `NetSalesTC`                      DECIMAL(38,6)
);
""")

# COMMAND ----------

# ---------------------------------------------
# 4️⃣ Optional: Verify creation by inspecting schema
# ---------------------------------------------
df_schema = spark.table(f"`dbe_dbx_internships`.`datastore`.`SalesInvoice`").dtypes
print("SalesInvoice table schema:")
for col_name, col_type in df_schema:
    print(f"  - {col_name}: {col_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
