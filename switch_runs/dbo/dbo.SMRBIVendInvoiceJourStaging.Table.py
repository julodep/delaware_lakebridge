# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendInvoiceJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendInvoiceJourStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks-compatible notebook: Create SMRBIVendInvoiceJourStaging table
# ------------------------------------------------------------------
# 1. Define the fully-qualified table name
tbl_name = f"`dbe_dbx_internships`.`dbo`.`SMRBIVendInvoiceJourStaging`"

# COMMAND ----------

# 2. Create (or replace) the table with the same column layout as the T-SQL CREATE TABLE statement.
#    - `NVARCHAR(...)` is mapped to `STRING`
#    - `NUMERIC(p,s)` is mapped to `DECIMAL(p,s)`
#    - `DATETIME` is mapped to `TIMESTAMP` (Spark represents both date and time)
#    - `BIGINT` remains `BIGINT`
#    - `INT`    -> `INT`
spark.sql(f"""
CREATE OR REPLACE TABLE {tbl_name} (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    CURRENCYCODE  STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    DLVMODE       STRING NOT NULL,
    DLVTERM       STRING NOT NULL,
    INTERNALINVOICEID STRING NOT NULL,
    INVOICEACCOUNT   STRING NOT NULL,
    INVOICEAMOUNT    DECIMAL(32,6) NOT NULL,
    INVOICEAMOUNTMST DECIMAL(32,6) NOT NULL,
    INVOICEDATE      TIMESTAMP NOT NULL,
    INVOICEID        STRING NOT NULL,
    PAYMENT          STRING NOT NULL,
    PURCHID          STRING NOT NULL,
    VENDINVOICEJOURRECID BIGINT NOT NULL,
    QTY               DECIMAL(32,6) NOT NULL,
    SUMTAX            DECIMAL(32,6) NOT NULL,
    VENDINVOICEJOURCREATEDBY STRING NOT NULL,
    VENDINVOICEJOURCREATEDDATETIME TIMESTAMP NOT NULL,
    PARTITION        STRING NOT NULL,
    DATAAREAID       STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID            BIGINT NOT NULL
)
USING delta
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Note on the PRIMARY KEY
# ------------------------------------------------------------------
# T-SQL uses:
#   PRIMARY KEY CLUSTERED (EXECUTIONID, VENDINVOICEJOURRECID, DATAAREAID, PARTITION)
#
# Spark/Databricks Delta Lake does not support explicit clustered primary keys.
# For data integrity you can:
#   1. Add a unique constraint via a custom validation script, or
#   2. Rely on application logic / distributed transactions.
#
# If you really need a unique index you can create a cluster key in Delta
# but this has performance implications and will not enforce uniqueness.
# For now we simply create the table without the key constraint.

# ------------------------------------------------------------------
# Verify the schema (optional)
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE EXTENDED {tbl_name}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
