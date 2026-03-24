# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ShipmentInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ShipmentInvoice.Table.sql`

# COMMAND ----------

# -------------------------------------------------
# Ensure the default Spark catalog is selected
# -------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# -------------------------------------------------
# Get the current user for permission grants
# -------------------------------------------------
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# COMMAND ----------

# -------------------------------------------------
# Grant the necessary permissions to the current user
# -------------------------------------------------
# Grant usage on the catalog (required to read metadata)
spark.sql(f"GRANT USAGE ON CATALOG spark_catalog TO `{current_user}`")

# COMMAND ----------

# -------------------------------------------------
# Setup: ensure the target database exists (lower‑case name to match later references)
# -------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# -------------------------------------------------
# Grant permissions on the newly created database
# -------------------------------------------------
spark.sql(f"GRANT USAGE, READ_METADATA ON DATABASE datastore TO `{current_user}`")

# COMMAND ----------

# -------------------------------------------------
# Create the ShipmentInvoice table in the datastore schema
# -------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE datastore.ShipmentInvoice (
    BusinessOwnerCode          STRING      NOT NULL,
    CompanyCode                STRING      NOT NULL,
    CustomerCode               STRING      NOT NULL,
    DepartmentCode             STRING      NOT NULL,
    DestinationAgentCode       STRING      NOT NULL,
    JobOwnerCode               STRING      NOT NULL,
    LineOfBusinessCode         STRING      NOT NULL,
    ModeOfTransportCode        STRING      NOT NULL,
    PurchaseInvoiceCode        STRING      NOT NULL,
    SalesInvoiceCode           STRING      NOT NULL,
    ShipmentContractCode       STRING      NOT NULL,
    TransactionCurrencyCode    STRING      NOT NULL,
    MasterBillOfLading         STRING      NOT NULL,
    HouseBillOfLading          STRING      NOT NULL,
    PortOfDestination          STRING      NOT NULL,
    PortOfOrigin               STRING      NOT NULL,
    Etd                        DATE,
    Eta                        DATE,
    Description                STRING      NOT NULL,
    Branch                     STRING      NOT NULL,
    Remark                     STRING      NOT NULL,
    ShipmentInvoiceLineNumber  DECIMAL(32,16) NOT NULL,
    ShipmentInvoiceDate        DATE,
    Amount                     DECIMAL(32,6)  NOT NULL,
    Voucher                    STRING      NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC Error in query 4: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
