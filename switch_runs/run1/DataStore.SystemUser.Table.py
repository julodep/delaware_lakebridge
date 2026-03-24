# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SystemUser.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SystemUser.Table.sql`

# COMMAND ----------

# -----------------------------------------------
# Ensure we are using a catalog where the user has metadata read permissions.
# The default Unity Catalog is typically `spark_catalog`.
# -----------------------------------------------
try:
    spark.sql("USE CATALOG spark_catalog")
except Exception as e:
    # If the user lacks permission to change the catalog, proceed with the current context.
    print(f"Warning: unable to set catalog due to: {e}")

# COMMAND ----------

# -----------------------------------------------
# Create the DataStore schema (database) if it does not already exist.
# Using `CREATE DATABASE` which is synonymous with `CREATE SCHEMA` in Spark SQL.
# Backticks protect the identifier.
# -----------------------------------------------
try:
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS `DataStore`
    """)
except Exception as e:
    # Handle insufficient privileges gracefully.
    print(f"Warning: could not create schema `DataStore`: {e}")

# COMMAND ----------

# -----------------------------------------------
# Create the SystemUser table as a Delta table.
# NVARCHAR is mapped to STRING; primary key constraints are omitted.
# -----------------------------------------------
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS `DataStore`.`SystemUser` (
            SystemUserCode STRING NOT NULL,
            UserName STRING NOT NULL,
            DomainUserName STRING
        )
        USING DELTA
    """)
except Exception as e:
    # Handle insufficient privileges or other errors gracefully.
    print(f"Warning: could not create table `DataStore`.`SystemUser`: {e}")

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
