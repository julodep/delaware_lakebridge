# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CostPrice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.CostPrice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: No additional imports required; Databricks provides `spark`
# ------------------------------------------------------------

# ----------------------------------------------------------------
# Helper: Ensure the target database exists (fallback to default schema)
# ----------------------------------------------------------------
_target_db = "DataStore"

# COMMAND ----------

# Check if the database already exists
existing_dbs = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]

# COMMAND ----------

if _target_db not in existing_dbs:
    # Create the database if it does not exist.
    # Note: Using the default location; adjust `LOCATION` if a specific path is required.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_target_db}")

# COMMAND ----------

# ----------------------------------------------------------------
# Create the Delta table `CostPrice` in the target database.
# If the user lacks permission on the target DB, fall back to the
# default database (no schema qualifier).
# ----------------------------------------------------------------
_create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {_target_db}.CostPrice (
    ItemNumber        STRING      NOT NULL,
    InventDimCode     STRING      NOT NULL,
    UnitCode          STRING      NOT NULL,
    CompanyCode       STRING      NOT NULL,
    Price             DECIMAL(38,9),
    StartValidityDate TIMESTAMP   NOT NULL,
    EndValidityDate   TIMESTAMP
)
USING DELTA
"""

# COMMAND ----------

try:
    spark.sql(_create_table_sql)
except Exception as e:
    # Detect insufficient‑privilege errors and fallback gracefully
    if "INSUFFICIENT_PERMISSIONS" in str(e):
        # Create the table in the default (current) schema instead
        fallback_sql = """
        CREATE TABLE IF NOT EXISTS CostPrice (
            ItemNumber        STRING      NOT NULL,
            InventDimCode     STRING      NOT NULL,
            UnitCode          STRING      NOT NULL,
            CompanyCode       STRING      NOT NULL,
            Price             DECIMAL(38,9),
            StartValidityDate TIMESTAMP   NOT NULL,
            EndValidityDate   TIMESTAMP
        )
        USING DELTA
        """
        spark.sql(fallback_sql)
    else:
        # Re‑raise unexpected errors
        raise

# COMMAND ----------

# ------------------------------------------------------------
# Optional: Verify the table schema (shows the table in the
# database where it was actually created)
# ------------------------------------------------------------
display(spark.sql("DESCRIBE FORMATTED CostPrice"))

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
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
