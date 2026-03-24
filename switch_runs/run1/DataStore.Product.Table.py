# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Product.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Product.Table.sql`

# COMMAND ----------

# Set up the target database in a catalog the user has access to.
# Fully qualify the database with the catalog name (e.g., spark_catalog) to avoid
# Spark attempting to list all catalogs, which can trigger READ_METADATA permission errors.
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# Create the Product table as a Delta table, also fully qualified with the catalog.
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.DataStore.Product (
    ProductId BIGINT NOT NULL,
    CompanyCode STRING,
    ProductCode STRING NOT NULL,
    ProductName STRING NOT NULL,
    ProductGroupCode STRING NOT NULL,
    ProductGroupName STRING NOT NULL,
    ProductGroupCodeName STRING NOT NULL,
    ProductInventoryUnit STRING NOT NULL,
    ProductPurchaseUnit STRING NOT NULL,
    ProductSalesUnit STRING NOT NULL,
    PhysicalUnitSymbol STRING NOT NULL,
    PhysicalVolume DECIMAL(38,6),
    PhysicalWeight DECIMAL(32,12) NOT NULL,
    PrimaryVendorCode STRING,
    CountryOfOrigin STRING NOT NULL,
    IntrastatCommodityCode STRING NOT NULL,
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
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
