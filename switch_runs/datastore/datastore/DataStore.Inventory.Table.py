# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Inventory.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Inventory.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  Create the Inventory table in Unity Catalog
# -------------------------------------------------------------
# All objects are fully‑qualified using the dbe_dbx_internships and datastore
# placeholders.  Spark SQL on Databricks maps the T‑SQL types
# to their Spark equivalents:
#   - NVARCHAR -> STRING
#   - NUMERIC(p,s) -> DECIMAL(p,s)
#   - DATETIME -> TIMESTAMP
#
# The syntax `CREATE OR REPLACE TABLE` ensures the table is
# created if it does not exist and replaced if it already exists.
# -------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Inventory` (
    ProductCode                   STRING NOT NULL,
    CompanyCode                   STRING NOT NULL,
    ProductConfigurationCode      STRING NOT NULL,
    BatchCode                     STRING NOT NULL,
    ReportDate                    TIMESTAMP,
    DefaultExchangeRateTypeCode   STRING NOT NULL,
    BudgetExchangeRateTypeCode    STRING NOT NULL,
    AccountingCurrencyCode       STRING NOT NULL,
    ReportingCurrencyCode         STRING NOT NULL,
    GroupCurrencyCode             STRING NOT NULL,
    InventoryUnit                 STRING NOT NULL,
    StockQuantity                 DECIMAL(32, 6) NOT NULL,
    StockValueAC                  DECIMAL(38, 6) NOT NULL,
    StockValueRC                  DECIMAL(38, 6) NOT NULL,
    StockValueGC                  DECIMAL(38, 6) NOT NULL,
    StockValueAC_Budget           DECIMAL(38, 6) NOT NULL,
    StockValueRC_Budget           DECIMAL(38, 6) NOT NULL,
    StockValueGC_Budget           DECIMAL(38, 6) NOT NULL,
    AppliedExchangeRateRC         DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateAC         DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateGC         DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateRC_Budget  DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateAC_Budget  DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateGC_Budget  DECIMAL(38, 21) NOT NULL
)
""")

# COMMAND ----------

# Optional: view the schema to confirm creation
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`Inventory`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
