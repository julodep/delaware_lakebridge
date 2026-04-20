# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustInvoiceTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustInvoiceTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the staging table in Databricks Delta Lake
#     All object references are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBICustInvoiceTransStaging`
#     Databricks does not support PRIMARY KEY constraints, so the PK clause is omitted.
#     The schema is translated as follows:
#         NVARCHAR       → STRING
#         INT            → INT
#         BIGINT         → BIGINT
#         NUMERIC(p,s)   → DECIMAL(p,s)
#         DATETIME       → TIMESTAMP
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustInvoiceTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    COMPANY STRING NOT NULL,
    DLVDATE TIMESTAMP NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTQTY DECIMAL(32,6) NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    INVOICEDATE TIMESTAMP NOT NULL,
    INVOICEID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    LINEAMOUNT DECIMAL(32,6) NOT NULL,
    LINEAMOUNTMST DECIMAL(32,6) NOT NULL,
    LINEDISC DECIMAL(32,6) NOT NULL,
    LINEHEADER STRING NOT NULL,
    LINENUM DECIMAL(32,16) NOT NULL,
    LINEPERCENT DECIMAL(32,6) NOT NULL,
    ORIGSALESID STRING NOT NULL,
    PRICEUNIT DECIMAL(32,12) NOT NULL,
    QTY DECIMAL(32,6) NOT NULL,
    QTYPHYSICAL DECIMAL(32,6) NOT NULL,
    CUSTINVOICETRANSRECID BIGINT NOT NULL,
    SALESID STRING NOT NULL,
    SALESPRICE DECIMAL(32,6) NOT NULL,
    SALESUNIT STRING NOT NULL,
    TAXWRITECODE STRING NOT NULL,
    DEFAULTDIMENSIONDISPLAYVALUE STRING NOT NULL,
    LEDGERDIMENSIONDISPLAYVALUE STRING NOT NULL,
    CUSTINVOICETRANSDIMENSION BIGINT NOT NULL,
    NAME STRING NOT NULL,
    DISCPERCENT DECIMAL(32,6) NOT NULL,
    DISCAMOUNT DECIMAL(32,6) NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
USING delta
-- Primary key constraint has been omitted because Delta Lake does not enforce PKs.
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Optional: Persist the table for future reuse
# ------------------------------------------------------------------
# The `CREATE OR REPLACE TABLE` command above already creates a persistent Delta table.
# No `spark.udf.register()` call is necessary.

# ------------------------------------------------------------------
# 3️⃣  Sample usage: show the first 5 rows to verify creation
# ------------------------------------------------------------------
df_preview = spark.table(f"dbe_dbx_internships.dbo.SMRBICustInvoiceTransStaging").limit(5)
display(df_preview)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
