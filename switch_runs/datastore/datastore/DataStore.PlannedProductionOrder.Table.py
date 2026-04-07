# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PlannedProductionOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PlannedProductionOrder.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the PlannedProductionOrder table in Unity Catalog.
# All references use the fully‑qualified format dbe_dbx_internships.datastore.PlannedProductionOrder
# --------------------------------------------------------------
# Make sure that the variables `catalog` and `schema` are defined in the notebook
# before executing this statement.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.PlannedProductionOrder (
    PlannedProductionOrderCode      STRING NOT NULL,
    ProductCode                     STRING NOT NULL,
    CompanyCode                     STRING NOT NULL,
    ProductConfigurationCode        STRING NOT NULL,
    ProductionOrderCode             STRING NOT NULL,
    RequirementDate                 TIMESTAMP NOT NULL,
    RequestedDate                   TIMESTAMP NOT NULL,
    OrderDate                        TIMESTAMP NOT NULL,
    DeliveryDate                     TIMESTAMP,          -- nullable
    Status                           STRING,             -- nullable
    LeadTime                         INT NOT NULL,
    InventoryUnit                    STRING NOT NULL,
    RequirementQuantity              DECIMAL(32, 6) NOT NULL,
    PurchaseUnit                     STRING NOT NULL,
    PurchaseQuantity                 DECIMAL(32, 6) NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 944)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.PlannedProductionOrder (     PlannedProductionOrderCode      STRING NOT NULL,     ProductCode                     STRING NOT NULL,     CompanyCode                     STRING NOT NULL,     ProductConfigurationCode        STRING NOT NULL,     ProductionOrderCode             STRING NOT NULL,     RequirementDate                 TIMESTAMP NOT NULL,     RequestedDate                   TIMESTAMP NOT NULL,     OrderDate                        TIMESTAMP NOT NULL,     DeliveryDate                     TIMESTAMP,          -- nullable     Status                           STRING,             -- nullable     LeadTime                         INT NOT NULL,     InventoryUnit                    STRING NOT NULL,     RequirementQuantity              DECIMAL(32, 6) NOT NULL,     PurchaseUnit                     STRING NOT NULL,     PurchaseQuantity                 DECIMAL(32, 6) NOT NULL )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
