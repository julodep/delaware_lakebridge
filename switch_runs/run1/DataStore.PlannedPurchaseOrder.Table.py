# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PlannedPurchaseOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PlannedPurchaseOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Set the catalog to the default Spark catalog.
# This ensures the user has the appropriate metadata permissions.
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create the target schema if it does not already exist.
# Using plain identifiers (no backticks) avoids case‑sensitivity issues.
# ------------------------------------------------------------
spark.sql("""
CREATE SCHEMA IF NOT EXISTS DataStore
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the PlannedPurchaseOrder table in the DataStore schema.
# Data types are mapped to Spark SQL equivalents.
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE DataStore.PlannedPurchaseOrder (
    PlannedPurchaseOrderCode STRING NOT NULL,
    ProductCode              STRING NOT NULL,
    SupplierCode             STRING NOT NULL,
    CompanyCode              STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    PurchaseOrderCode        STRING NOT NULL,
    RequirementDate          TIMESTAMP NOT NULL,
    RequestedDate            TIMESTAMP NOT NULL,
    OrderDate                TIMESTAMP NOT NULL,
    DeliveryDate             TIMESTAMP,
    Status                   STRING,
    LeadTime                 INT NOT NULL,
    InventoryUnit            STRING NOT NULL,
    RequirementQuantity      DECIMAL(32,6) NOT NULL,
    PurchaseUnit             STRING NOT NULL,
    PurchaseQuantity         DECIMAL(32,6) NOT NULL
)
""")

# COMMAND ----------

# ------------------------------------------------------------
# Verify that the table was created successfully.
# ------------------------------------------------------------
display(spark.sql("DESCRIBE FORMATTED DataStore.PlannedPurchaseOrder"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
