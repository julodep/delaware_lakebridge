# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PriceAgreement.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PriceAgreement.Table.sql`

# COMMAND ----------

# Setup: import any required modules (none needed for pure SQL execution)
import sys

# COMMAND ----------

# -------------------------------------------------------------------------
# Switch to the default catalog to ensure we have metadata access.
# In Databricks the default catalog is typically `spark_catalog`.
# -------------------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# -------------------------------------------------------------------------
# Ensure the target database/schema exists.
# Databricks uses databases (schemas) within a catalog; create it if it does not exist.
# -------------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS DataStore")

# COMMAND ----------

# -------------------------------------------------------------------------
# Switch to the newly created (or existing) database for subsequent table creation.
# -------------------------------------------------------------------------
spark.sql("USE DataStore")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the PriceAgreement table.
# T‑SQL options such as SET ANSI_NULLS, SET QUOTED_IDENTIFIER, GO, and
# ON [PRIMARY] have no direct equivalent in Databricks and are therefore
# omitted (commented for reference).
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE PriceAgreement (
    VendorCode   STRING         NOT NULL,
    ProductCode  STRING         NOT NULL,
    CompanyCode  STRING         NOT NULL,
    Amount       DECIMAL(32,6)  NOT NULL,
    Currency     STRING         NOT NULL,
    FromDate     TIMESTAMP      NOT NULL,
    ToDate       TIMESTAMP      NOT NULL,
    QtyFrom      DECIMAL(32,6)  NOT NULL,
    QtyTo        DECIMAL(32,6)  NOT NULL,
    UnitId       STRING         NOT NULL,
    PriceUnit    DECIMAL(32,12) NOT NULL
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
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
