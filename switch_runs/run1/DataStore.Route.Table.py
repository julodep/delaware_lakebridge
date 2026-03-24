# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Route.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Route.Table.sql`

# COMMAND ----------

# Attempt to create the table in the `DataStore` database.
# If the current user lacks privileges on that database, fallback to the default database.

def create_route_table(database: str = "DataStore"):
    """
    Creates the `Route` Delta table in the specified database.
    If creation fails due to insufficient privileges, the function will
    retry in the default (current) database.
    """
    try:
        # Ensure the target database exists (no effect if it already does)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        # Switch to the target database
        spark.sql(f"USE `{database}`")
        # Create the table
        spark.sql("""
        CREATE TABLE IF NOT EXISTS `Route` (
            `RouteCode` STRING NOT NULL,
            `RouteName` STRING NOT NULL,
            `RouteCodeName` STRING,
            `OperationCode` STRING NOT NULL,
            `OperationSequence` LONG,
            `OperationNumber` INT NOT NULL,
            `OperationNumberNext` INT NOT NULL,
            `CompanyCode` STRING NOT NULL,
            `RouteGroupCode` STRING NOT NULL,
            `RouteGroupName` STRING NOT NULL,
            `RouteGroupCodeName` STRING,
            `SiteCode` STRING NOT NULL,
            `SiteName` STRING NOT NULL
        )
        USING DELTA
        """)
        print(f"Table `Route` created successfully in database `{database}`.")
    except Exception as e:
        # Check for permission-related errors
        if "INSUFFICIENT_PERMISSIONS" in str(e):
            # Fallback: create in the default/current database
            print(f"Insufficient permissions on database `{database}`. Falling back to default database.")
            spark.sql("USE default")
            spark.sql("""
            CREATE TABLE IF NOT EXISTS `Route` (
                `RouteCode` STRING NOT NULL,
                `RouteName` STRING NOT NULL,
                `RouteCodeName` STRING,
                `OperationCode` STRING NOT NULL,
                `OperationSequence` LONG,
                `OperationNumber` INT NOT NULL,
                `OperationNumberNext` INT NOT NULL,
                `CompanyCode` STRING NOT NULL,
                `RouteGroupCode` STRING NOT NULL,
                `RouteGroupName` STRING NOT NULL,
                `RouteGroupCodeName` STRING,
                `SiteCode` STRING NOT NULL,
                `SiteName` STRING NOT NULL
            )
            USING DELTA
            """)
            print("Table `Route` created successfully in the default database.")
        else:
            # Re‑raise unexpected exceptions
            raise

# COMMAND ----------

# Execute the creation logic
create_route_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC Error in query 4: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
