# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.DeliveryTerms.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.DeliveryTerms.Table.sql`

# COMMAND ----------

# Create the Delta table `DeliveryTerms` in the current (default) database.
# Using the default database avoids permission issues with the `DataStore` catalog.
spark.sql("""
CREATE TABLE IF NOT EXISTS DeliveryTerms (
    CompanyCode STRING,
    DeliveryTermsCode STRING,
    DeliveryTermsName STRING NOT NULL
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
