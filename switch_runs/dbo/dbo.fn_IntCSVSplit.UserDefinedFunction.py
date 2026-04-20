# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.fn_IntCSVSplit.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.fn_IntCSVSplit.UserDefinedFunction.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create a persistent “table‑valued” function in Unity Catalog
# ------------------------------------------------------------------
# This function parses a comma‑delimited string and returns a table of integers,
# inserting NULL for non‑numeric items.
# The function is defined with a fully‑qualified name: <catalog>.<schema>.fn_IntCSVSplit

spark.sql(f"""
CREATE OR REPLACE FUNCTION `dbe_dbx_internships`.`dbo`.fn_IntCSVSplit(
    RowData STRING
)
RETURNS TABLE (
    Data INT
)
AS
RETURN
SELECT
    TRY_CAST(element AS INT) AS Data
FROM
    EXPLODE(SPLIT(RowData, ',')) AS element;
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Usage example (optional)
# ------------------------------------------------------------------
# The function can now be used in any Spark SQL query.
example_df = spark.sql(f"""
SELECT *
FROM `dbe_dbx_internships`.`dbo`.fn_IntCSVSplit('10,20,abc,30')
""")
display(example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'RETURN'. SQLSTATE: 42601 (line 1, pos 138)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE FUNCTION `_placeholder_`.`_placeholder_`.fn_IntCSVSplit(     RowData STRING ) RETURNS TABLE (     Data INT ) AS RETURN SELECT     TRY_CAST(element AS INT) AS Data FROM     EXPLODE(SPLIT(RowData, ',')) AS element;
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
