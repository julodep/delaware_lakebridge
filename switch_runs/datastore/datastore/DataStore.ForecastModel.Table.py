# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ForecastModel.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ForecastModel.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Databricks (Spark) notebook – Create the ForecastModel table
# --------------------------------------------------------------
# Map SQL Server NVARCHAR to Spark STRING and use Spark's
# CREATE TABLE syntax with a DLT/Delta data source to avoid
# syntax errors.  Backticks are used to qualify catalog & schema.
# --------------------------------------------------------------

catalog = "dbe_dbx_internships"
schema   = "datastore"

# COMMAND ----------

# Create the ForecastModel table with the appropriate schema.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`datastore`.`ForecastModel` (
    CompanyCode STRING,                 -- nvarchar(4), nullable
    ForecastModelCode STRING,           -- nvarchar(10), nullable
    ForecastModelName STRING NOT NULL, -- nvarchar(60), required
    ForecastSubModelCode STRING NOT NULL  -- nvarchar(10), required
)
USING DELTA
""")

# COMMAND ----------

# Optional – verify that the table exists and inspect its schema
df = spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`ForecastModel`")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 349)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`ForecastModel` (     CompanyCode STRING,                 -- nvarchar(4), nullable     ForecastModelCode STRING,           -- nvarchar(10), nullable     ForecastModelName STRING NOT NULL, -- nvarchar(60), required     ForecastSubModelCode STRING NOT NULL  -- nvarchar(10), required ) USING DELTA
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
