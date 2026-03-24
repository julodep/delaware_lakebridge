# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.IntercompanyARAP.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.IntercompanyARAP.Table.sql`

# COMMAND ----------

# Setup: ensure required imports are available (Databricks provides Spark session by default)
# No additional imports are needed for executing SQL statements via spark.sql()

# ------------------------------------------------------------
# Use the default catalog (required for metadata access)
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create the target database if it does not already exist
# ------------------------------------------------------------
# Use the lowercase database name that matches the permissions granted to the user.
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the IntercompanyARAP table in the datastore database
# ------------------------------------------------------------
# Column data types are mapped from T‑SQL to Spark SQL equivalents:
#   datetime, datetime2 -> TIMESTAMP
#   int                -> INT
#   nvarchar(max)      -> STRING
#   varchar(...)       -> STRING
#   numeric(p,s)       -> DECIMAL(p,s)
spark.sql("""
CREATE TABLE IF NOT EXISTS datastore.IntercompanyARAP (
    InvoiceDate          TIMESTAMP,
    YearInvoice          INT,
    PostedDate           TIMESTAMP,
    DueDate              TIMESTAMP,
    Brn                  STRING NOT NULL,
    Departement          STRING NOT NULL,
    AR_AP_Type           STRING NOT NULL,
    Type                 STRING NOT NULL,
    SalesInvoiceCode    STRING NOT NULL,
    PurchaseInvoiceCode STRING NOT NULL,
    JobInvoice           STRING NOT NULL,
    Currency             STRING NOT NULL,
    InvoiceTotal         DECIMAL(38, 8) NOT NULL,
    CustomerCode         STRING NOT NULL,
    SupplierCode         STRING NOT NULL,
    AccountName          STRING NOT NULL,
    AP_Settlement        STRING NOT NULL,
    AR_Settlement        STRING NOT NULL,
    CrGRP                STRING NOT NULL,
    DrGRP                STRING NOT NULL,
    DestDisch            STRING NOT NULL,
    ETA                  TIMESTAMP,
    ETD                  TIMESTAMP,
    House                STRING NOT NULL,
    JobNumber            STRING NOT NULL,
    Master               STRING NOT NULL,
    OrigCountry          STRING NOT NULL,
    OrigCountryName      STRING NOT NULL,
    OriginLoad           STRING NOT NULL,
    CompanyCode          STRING NOT NULL,
    ExchangeRate         DECIMAL(32, 16) NOT NULL
)
USING DELTA
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
