# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.fn_IntCSVSplit.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/fn/dbo.fn_IntCSVSplit.UserDefinedFunction.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# 1️⃣  Create a persistent Unity Catalog table‑valued function
#     -> splits a comma‑separated string into INT values.
# --------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE FUNCTION dbe_dbx_internships.switchschema.fn_IntCSVSplit(V_RowData STRING)
RETURNS TABLE (
  Data INT
)
RETURN
SELECT
  CASE
    WHEN try_cast(trim(value) AS INT) IS NOT NULL THEN try_cast(trim(value) AS INT)
    ELSE NULL
  END AS Data
FROM
  (SELECT explode(split(V_RowData, ',')) AS value) v;
""")

# COMMAND ----------

# --------------------------------------------------------------------
# 2️⃣  Example usage of the function
# --------------------------------------------------------------------
sample_row = " 1, 23, 045, abc, 99 "
results_df = spark.sql(f"""
SELECT * FROM dbe_dbx_internships.switchschema.fn_IntCSVSplit('{sample_row}')
""")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELECT'. SQLSTATE: 42601 (line 1, pos 133)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE FUNCTION dbe_dbx_internships.switchschema.fn_IntCSVSplit(V_RowData STRING) RETURNS TABLE (   Data INT ) AS SELECT   CASE     WHEN try_cast(trim(value) AS INT) IS NOT NULL THEN try_cast(trim(value) AS INT)     ELSE NULL   END AS Data FROM   (SELECT explode(split(V_RowData, ',')) AS value) v;
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
