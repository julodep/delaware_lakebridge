# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Schema
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Schema.sql`

# COMMAND ----------

# Attempt to create the DataStore schema (namespace) in the default catalog.
# Note: Creating a schema may require READ_METADATA privileges on the catalog.
# If the current user lacks these permissions, the operation will fail.
# We catch the exception to handle insufficient permission scenarios gracefully.

try:
    # Specify the catalog explicitly to avoid ambiguity.
    # Adjust "spark_catalog" to the appropriate catalog if needed.
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS spark_catalog.DataStore
    """)
except Exception as e:
    # Check for Spark security exceptions related to insufficient privileges.
    if "INSUFFICIENT_PERMISSIONS" in str(e):
        # Log a warning; in a real notebook you might use `print` or a logging framework.
        print("WARNING: Unable to create schema 'DataStore' due to insufficient permissions. "
              "Please contact your Databricks administrator to grant READ_METADATA on the catalog.")
    else:
        # Re‑raise unexpected exceptions.
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC ```
