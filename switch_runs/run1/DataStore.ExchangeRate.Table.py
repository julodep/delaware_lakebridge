# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ExchangeRate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ExchangeRate.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required libraries (Databricks provides `spark` by default)
# ------------------------------------------------------------
# No additional imports required for Spark SQL operations

# ------------------------------------------------------------
# Helper function to safely execute Spark SQL statements
# ------------------------------------------------------------
def execute_sql(statement: str):
    """
    Executes a Spark SQL statement.
    Catches and logs permission‑related errors so that the notebook can continue
    even when the user lacks READ_METADATA privileges on the catalog or database.
    """
    try:
        spark.sql(statement)
    except Exception as e:
        # Log the error (in Databricks notebooks, `print` writes to the cell output)
        print(f"SQL execution failed: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Create the target schema (database) if it does not already exist
# ------------------------------------------------------------
execute_sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the ExchangeRate table as a Delta table.
# The original T‑SQL data types are mapped to Spark SQL equivalents:
#   nvarchar / varchar -> STRING
#   datetime          -> TIMESTAMP
#   numeric(p,s)      -> DECIMAL(p,s)
# The `ON [PRIMARY]` clause and SET options have no meaning in Databricks
# and are therefore omitted (commented for reference).
# ------------------------------------------------------------
execute_sql("""
CREATE TABLE IF NOT EXISTS `DataStore`.`ExchangeRate` (
    ExchangeRateTypeCode STRING NOT NULL,
    ExchangeRateTypeName STRING NOT NULL,
    DataSource STRING NOT NULL,
    FromCurrencyCode STRING NOT NULL,
    ToCurrencyCode STRING NOT NULL,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ExchangeRate DECIMAL(38,17)
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: [PARSE_SYNTAX_ERROR] Syntax error at or near '_placeholder_'. SQLSTATE: 42601 (line 1, pos 8)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN _placeholder_
# MAGIC --------^^^
# MAGIC ```
