# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Company.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Company.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the 'Company' table in Unity Catalog
# ------------------------------------------------------------------
# NOTE: Set your catalog and schema names below.
catalog = "YOUR_CATALOG_NAME"   # e.g., "demo_catalog"
schema  = "YOUR_SCHEMA_NAME"     # e.g., "public"

# COMMAND ----------

# Create or replace the table with the desired schema
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Company` (
    CompanyCode STRING,            -- equivalent of NVARCHAR(4), NULL allowed
    CompanyName STRING NOT NULL,   -- equivalent of NVARCHAR(100), NOT NULL
    CompanyCodeName STRING,        -- equivalent of NVARCHAR(105), NULL allowed
    CompanyType STRING NOT NULL    -- equivalent of NVARCHAR(10), NOT NULL
) USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Verify the new table (optional)
# ------------------------------------------------------------------
display(spark.table(f"dbe_dbx_internships.datastore.Company"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 405)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `YOUR_CATALOG_NAME`.`YOUR_SCHEMA_NAME`.`Company` (     CompanyCode STRING,            -- equivalent of NVARCHAR(4), NULL allowed     CompanyName STRING NOT NULL,   -- equivalent of NVARCHAR(100), NOT NULL     CompanyCodeName STRING,        -- equivalent of NVARCHAR(105), NULL allowed     CompanyType STRING NOT NULL    -- equivalent of NVARCHAR(10), NOT NULL ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
