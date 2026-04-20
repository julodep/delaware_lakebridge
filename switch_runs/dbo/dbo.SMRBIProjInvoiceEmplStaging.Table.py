# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceEmplStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceEmplStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in the target Unity Catalog database.
# ------------------------------------------------------------------
# These variables should be set to the catalog and schema that contain
# the table in Databricks (replace with your actual names).
catalog = "<your_catalog>"
schema  = "<your_schema>"

# COMMAND ----------

# Delta Lake automatically manages the storage format, partitions,
# and ACID semantics for us.  The T‑SQL table is recreated in
# Spark SQL syntax with datatype mappings:
#   NVARCHAR   -> STRING
#   DATETIME   -> TIMESTAMP
#   NUMERIC(p,s) -> DECIMAL(p,s)
#   BIGINT     -> BIGINT
#
# Primary‑key enforcement is not available in Delta Lake, so the
# PK definition is omitted.  If you need uniqueness guarantees,
# enforce them in the application layer or by using a unique
# constraint through a business‑logic layer.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjInvoiceEmplStaging` (
  DEFINITIONGROUP STRING NOT NULL,
  EXECUTIONID   STRING NOT NULL,
  ISSELECTED    INT    NOT NULL,
  TRANSFERSTATUS INT   NOT NULL,
  ACTIVITYNUMBER STRING NOT NULL,
  CATEGORYID   STRING NOT NULL,
  CURRENCYID   STRING NOT NULL,
  COMPANY      STRING NOT NULL,
  INVOICEDATE  TIMESTAMP NOT NULL,
  LINEAMOUNT   DECIMAL(32,6) NOT NULL,
  PROJID       STRING NOT NULL,
  PROJINVOICEID STRING NOT NULL,
  QTY          DECIMAL(32,6) NOT NULL,
  TAXAMOUNT   DECIMAL(32,6) NOT NULL,
  TRANSDATE    TIMESTAMP NOT NULL,
  TRANSID      STRING NOT NULL,
  TXT          STRING NOT NULL,
  WORKER       BIGINT NOT NULL,
  PARTITION    STRING NOT NULL,
  DATAAREAID   STRING NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL,
  RECID        BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Verify the schema if desired
# ------------------------------------------------------------------
schema_df = spark.table(f"dbe_dbx_internships.dbo.SMRBIProjInvoiceEmplStaging")
schema_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
