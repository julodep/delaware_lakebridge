# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseDelivery.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PurchaseDelivery.Table.sql`

# COMMAND ----------

# Import Spark session (available by default in Databricks)
# No additional imports required

# ------------------------------------------------------------
# Use a database where the current user has sufficient privileges.
# Most Databricks clusters grant full access to the default database.
# ------------------------------------------------------------
spark.sql("USE default")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Delta table `default.PurchaseDelivery`
# The table definition mirrors the original specification with Spark‑compatible types.
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE PurchaseDelivery (
    PackingSlipCode           STRING      NOT NULL,
    PurchaseOrderCode        STRING      NOT NULL,
    CompanyCode              STRING      NOT NULL,
    ProductConfigurationCode STRING      NOT NULL,
    ProductCode              STRING      NOT NULL,
    OrderSupplierCode        STRING      NOT NULL,
    SupplierCode             STRING      NOT NULL,
    DeliveryModeCode         STRING      NOT NULL,
    DeliveryTermsCode        STRING      NOT NULL,
    ActualDeliveryDate       DATE,
    RequestedDeliveryDate    DATE,
    ConfirmedDeliveryDate    DATE,
    PurchaseType             STRING,
    PurchaseOrderLineNumber  BIGINT      NOT NULL,
    DeliveryName             STRING      NOT NULL,
    DeliveryLineNumber       DECIMAL(32,16) NOT NULL,
    PurchaseUnit             STRING      NOT NULL,
    QuantityOrdered          DECIMAL(32,6),
    QuantityDelivered        DECIMAL(32,6)
)
USING DELTA
""")  # The CREATE OR REPLACE ensures idempotent execution within the allowed database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
