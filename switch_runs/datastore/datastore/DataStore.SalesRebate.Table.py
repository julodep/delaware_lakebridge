# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesRebate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.SalesRebate.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------
# Create the persistent SalesRebate table on Unity Catalog
# ------------------------------------------------------------------------------

# Define catalog and schema. Use configured values or defaults.
catalog = spark.conf.get("spark.databricks.catalog", "default_catalog")
schema = spark.conf.get("spark.databricks.schema", "default_schema")

# COMMAND ----------

# Create or replace the table with the desired schema
spark.sql(f"""
CREATE OR REPLACE TABLE dbe_dbx_internships.datastore.SalesRebate (
    CompanyCode            STRING,          -- NVARCHAR(4)
    SalesRebateCode        STRING,          -- NVARCHAR(12)
    SalesInvoiceCode       STRING NOT NULL,  -- NVARCHAR(20)
    SalesInvoiceLineId     BIGINT NOT NULL,  -- BIGINT
    ProductCode            STRING NOT NULL,  -- NVARCHAR(20)
    RebateCustomerCode     STRING NOT NULL,  -- NVARCHAR(20)
    RebateCurrencyCode     STRING NOT NULL,  -- NVARCHAR(3)
    RebateAmountOriginal   DECIMAL(32,6) NOT NULL,  -- NUMERIC(32,6)
    RebateAmountCompleted  DECIMAL(38,6),          -- NUMERIC(38,6)
    RebateAmountMarked     DECIMAL(38,6),          -- NUMERIC(38,6)
    RebateAmountCancelled  DECIMAL(38,6),          -- NUMERIC(38,6)
    RebateAmountVariance   DECIMAL(38,6)           -- NUMERIC(38,6)
)
USING delta
""")

# COMMAND ----------

# Optional: Show the first few rows
df = spark.table(f"dbe_dbx_internships.datastore.SalesRebate")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 845)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE _placeholder_._placeholder_.SalesRebate (     CompanyCode            STRING,          -- NVARCHAR(4)     SalesRebateCode        STRING,          -- NVARCHAR(12)     SalesInvoiceCode       STRING NOT NULL,  -- NVARCHAR(20)     SalesInvoiceLineId     BIGINT NOT NULL,  -- BIGINT     ProductCode            STRING NOT NULL,  -- NVARCHAR(20)     RebateCustomerCode     STRING NOT NULL,  -- NVARCHAR(20)     RebateCurrencyCode     STRING NOT NULL,  -- NVARCHAR(3)     RebateAmountOriginal   DECIMAL(32,6) NOT NULL,  -- NUMERIC(32,6)     RebateAmountCompleted  DECIMAL(38,6),          -- NUMERIC(38,6)     RebateAmountMarked     DECIMAL(38,6),          -- NUMERIC(38,6)     RebateAmountCancelled  DECIMAL(38,6),          -- NUMERIC(38,6)     RebateAmountVariance   DECIMAL(38,6)           -- NUMERIC(38,6) ) USING delta
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
