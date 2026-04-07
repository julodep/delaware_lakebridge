# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Product.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Product.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the `Product` table in the target catalog and schema.
# All object references are fully‑qualified using the placeholders
# `dbe_dbx_internships` and `datastore` which you can replace at runtime.
# -------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Product` (
    -- Primary key / identifier for the product
    ProductId BIGINT NOT NULL,

    -- Optional company code
    CompanyCode STRING,

    -- Product codes and descriptive names
    ProductCode STRING NOT NULL,
    ProductName STRING NOT NULL,
    ProductGroupCode STRING NOT NULL,
    ProductGroupName STRING NOT NULL,
    ProductGroupCodeName STRING NOT NULL,

    -- Units for inventory, purchase and sales
    ProductInventoryUnit STRING NOT NULL,
    ProductPurchaseUnit STRING NOT NULL,
    ProductSalesUnit STRING NOT NULL,

    -- Physical attributes
    PhysicalUnitSymbol STRING NOT NULL,
    PhysicalVolume DECIMAL(38,6),
    PhysicalWeight DECIMAL(32,12) NOT NULL,

    -- Vendor and origin information
    PrimaryVendorCode STRING,
    CountryOfOrigin STRING NOT NULL,
    IntrastatCommodityCode STRING NOT NULL,

    -- Classification and business context
    ABCClassification STRING,
    Brand STRING NOT NULL,
    Material STRING NOT NULL,
    BusinessType STRING NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1078)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`Product` (     -- Primary key / identifier for the product     ProductId BIGINT NOT NULL,      -- Optional company code     CompanyCode STRING,      -- Product codes and descriptive names     ProductCode STRING NOT NULL,     ProductName STRING NOT NULL,     ProductGroupCode STRING NOT NULL,     ProductGroupName STRING NOT NULL,     ProductGroupCodeName STRING NOT NULL,      -- Units for inventory, purchase and sales     ProductInventoryUnit STRING NOT NULL,     ProductPurchaseUnit STRING NOT NULL,     ProductSalesUnit STRING NOT NULL,      -- Physical attributes     PhysicalUnitSymbol STRING NOT NULL,     PhysicalVolume DECIMAL(38,6),     PhysicalWeight DECIMAL(32,12) NOT NULL,      -- Vendor and origin information     PrimaryVendorCode STRING,     CountryOfOrigin STRING NOT NULL,     IntrastatCommodityCode STRING NOT NULL,      -- Classification and business context     ABCClassification STRING,     Brand STRING NOT NULL,     Material STRING NOT NULL,     BusinessType STRING NOT NULL ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
