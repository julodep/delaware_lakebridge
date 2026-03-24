# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Inventory.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Inventory.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------------
# Ensure the current session has the necessary privileges.
# Grant READ_METADATA on the catalog and USAGE/READ_METADATA on the target database.
# Replace `your_user` with the appropriate principal (e.g., the current user or a group).
# --------------------------------------------------------------------------------
spark.sql("""
    GRANT READ_METADATA ON CATALOG spark_catalog TO USER `your_user`
""")
spark.sql("""
    GRANT USAGE, READ_METADATA ON DATABASE `DataStore` TO USER `your_user`
""")

# COMMAND ----------

# --------------------------------------------------------------------------------
# Setup: ensure the target database exists (Databricks provides a default Spark session)
# --------------------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS `DataStore`")

# COMMAND ----------

# --------------------------------------------------------------------------------
# Create the Inventory table in the DataStore schema.
# NOTE:
# • T‑SQL SET options (ANSI_NULLS, QUOTED_IDENTIFIER) and the GO batch separator have no effect in Databricks and are therefore omitted.
# • The ON [PRIMARY] clause is a SQL Server storage hint; it is not applicable to Delta tables and is commented out.
# • NVARCHAR → STRING, DATETIME → TIMESTAMP, NUMERIC(p,s) → DECIMAL(p,s) in Spark SQL.
# --------------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS `DataStore`.`Inventory` (
    ProductCode STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    BatchCode STRING NOT NULL,
    ReportDate TIMESTAMP,
    DefaultExchangeRateTypeCode STRING NOT NULL,
    BudgetExchangeRateTypeCode STRING NOT NULL,
    AccountingCurrencyCode STRING NOT NULL,
    ReportingCurrencyCode STRING NOT NULL,
    GroupCurrencyCode STRING NOT NULL,
    InventoryUnit STRING NOT NULL,
    StockQuantity DECIMAL(32,6) NOT NULL,
    StockValueAC DECIMAL(38,6) NOT NULL,
    StockValueRC DECIMAL(38,6) NOT NULL,
    StockValueGC DECIMAL(38,6) NOT NULL,
    StockValueAC_Budget DECIMAL(38,6) NOT NULL,
    StockValueRC_Budget DECIMAL(38,6) NOT NULL,
    StockValueGC_Budget DECIMAL(38,6) NOT NULL,
    AppliedExchangeRateRC DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateAC DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateGC DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateRC_Budget DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateAC_Budget DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateGC_Budget DECIMAL(38,21) NOT NULL
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: [PARSE_SYNTAX_ERROR] Syntax error at or near '`your_user`': extra input '`your_user`'. SQLSTATE: 42601 (line 1, pos 61)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN GRANT READ_METADATA ON CATALOG spark_catalog TO USER `your_user`
# MAGIC -------------------------------------------------------------^^^
# MAGIC Error in query 1: [PARSE_SYNTAX_ERROR] Syntax error at or near '`your_user`': extra input '`your_user`'. SQLSTATE: 42601 (line 1, pos 67)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN GRANT USAGE, READ_METADATA ON DATABASE `DataStore` TO USER `your_user`
# MAGIC -------------------------------------------------------------------^^^
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
