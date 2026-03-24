# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Supplier.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Supplier.Table.sql`

# COMMAND ----------

# Create the Supplier table in the default database (or a database you have access to).
# If you need a specific database, ensure it exists and you have READ_METADATA and USAGE permissions.
spark.sql("""
CREATE TABLE IF NOT EXISTS Supplier (
    SupplierId BIGINT NOT NULL,
    CompanyCode STRING,
    SupplierCode STRING,
    SupplierName STRING NOT NULL,
    SupplierCodeName STRING,
    SupplierGroupCode STRING NOT NULL,
    SupplierGroupName STRING NOT NULL,
    SupplierGroupCodeName STRING NOT NULL,
    Address STRING,
    PostalCode STRING NOT NULL,
    City STRING NOT NULL,
    CountryRegionCode STRING NOT NULL,
    CompanyChainName STRING NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
