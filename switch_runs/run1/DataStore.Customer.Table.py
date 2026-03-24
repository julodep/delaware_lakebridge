# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Customer.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Customer.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: create the target database if it does not already exist.
# Explicitly specify the catalog (e.g., hive_metastore) to avoid
# permission issues related to the default catalog.
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.datastore")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Customer table in the datastore database.
# Square‑bracketed identifiers are replaced with plain identifiers,
# and T‑SQL data types are mapped to Spark SQL types.
# The `ON [PRIMARY]` clause is specific to SQL Server storage
# and is not applicable in Delta Lake; it is omitted.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS hive_metastore.datastore.customer (
    CustomerId BIGINT NOT NULL,
    CompanyCode STRING,
    CustomerCode STRING NOT NULL,
    CustomerName STRING NOT NULL,
    CustomerCodeName STRING,
    CustomerGroup STRING NOT NULL,
    CustomerGroupName STRING NOT NULL,
    CustomerGroupCodeName STRING NOT NULL,
    CustomerClass STRING NOT NULL,
    CustomerClassName STRING NOT NULL,
    CustomerClassCodeName STRING NOT NULL,
    Address STRING,
    PostalCode STRING NOT NULL,
    City STRING NOT NULL,
    Country STRING NOT NULL,
    SalesGroup STRING NOT NULL,
    Agent STRING NOT NULL,
    SalesResponsibleCode STRING NOT NULL,
    SalesResponsibleName STRING NOT NULL,
    SalesSegmentCode STRING NOT NULL,
    SalesSubSegmentCode STRING NOT NULL,
    DeliveryTerms STRING NOT NULL,
    OnholdStatus STRING,
    CreditLimitIsMandatory STRING,
    CreditLimit DECIMAL(32,6) NOT NULL,
    CompanyChain STRING NOT NULL,
    TaxGroup STRING NOT NULL
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
