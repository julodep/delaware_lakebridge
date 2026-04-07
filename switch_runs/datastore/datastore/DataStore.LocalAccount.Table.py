# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.LocalAccount.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.LocalAccount.Table.sql`

# COMMAND ----------

# -----------------------------------------------------------
# Databricks notebook (Python) – Create the *LocalAccount* table
# in the target catalog & schema.
# -----------------------------------------------------------

# ------------------------------------------------------------------
# 1.  Define the catalog and schema names (replace with actual names)
# ------------------------------------------------------------------
catalog = "<catalog_name>"      # e.g., 'shared' or your catalog ID
schema  = "<schema_name>'"     # e.g., 'finance' or your schema ID

# COMMAND ----------

# ------------------------------------------------------------------
# 2.  Build a fully‑qualified, back‑ticked table name
# ------------------------------------------------------------------
full_name = f"`dbe_dbx_internships`.`datastore`.`LocalAccount`"

# COMMAND ----------

# ------------------------------------------------------------------
# 3.  Drop the table if it already exists – safe for re‑runs
# ------------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS {full_name}")

# COMMAND ----------

# ------------------------------------------------------------------
# 4.  Create the table with the appropriate Spark datatypes
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE {full_name} (
    LocalAccountId     BIGINT,       -- PK in T‑SQL, not enforced here
    LocalAccountCode   STRING,
    LocalAccountName   STRING,
    LocalAccountCodeName STRING,
    DimensionName      STRING
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 5.  (Optional) Insert sample rows for verification
# ------------------------------------------------------------------
# spark.sql(f"""
# INSERT INTO {full_name} (LocalAccountId, LocalAccountCode, LocalAccountName,
#                         LocalAccountCodeName, DimensionName)
# VALUES
#   (1, 'CODE1', 'Account One', 'Code One', 'Dimension A'),
#   (2, 'CODE2', 'Account Two', 'Code Two', 'Dimension B')
# """)

# ------------------------------------------------------------------
# 6.  Verify creation by selecting a few rows
# ------------------------------------------------------------------
result = spark.sql(f"SELECT * FROM {full_name} LIMIT 5")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 280)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `<catalog_name>`.`<schema_name>'`.`LocalAccount` (     LocalAccountId     BIGINT,       -- PK in T‑SQL, not enforced here     LocalAccountCode   STRING,     LocalAccountName   STRING,     LocalAccountCodeName STRING,     DimensionName      STRING )
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
