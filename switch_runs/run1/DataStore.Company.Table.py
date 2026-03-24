# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Company.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Company.Table.sql`

# COMMAND ----------

# Ensure the target database exists and the user has access.
# If you do not have permission to create or use the `DataStore` database,
# you can omit the database qualifier and use the default database instead.
# Uncomment the line below if you have the required privileges.
# spark.sql("CREATE DATABASE IF NOT EXISTS DataStore")

# Option 1: Use the `DataStore` database (requires READ_METADATA and USAGE privileges)
# spark.sql("USE DataStore")
# spark.sql("""
# CREATE OR REPLACE TABLE Company (
#     CompanyCode STRING,
#     CompanyName STRING NOT NULL,
#     CompanyCodeName STRING,
#     CompanyType STRING NOT NULL
# )
# """)

# Option 2: Use the default database (no additional privileges required)
spark.sql("""
CREATE OR REPLACE TABLE Company (
    CompanyCode STRING,
    CompanyName STRING NOT NULL,
    CompanyCodeName STRING,
    CompanyType STRING NOT NULL
)
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
