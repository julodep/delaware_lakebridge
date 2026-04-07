# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.fn_IntCSVSplit.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/fn/dbo.fn_IntCSVSplit.UserDefinedFunction.sql`

# COMMAND ----------

# ------------------------------------------------------------
#  Persistent Unity Catalog function: fn_IntCSVSplit
#  ------------------------------------------------------------
#  Purpose:
#  ───────
#  • Accepts a string containing comma‑separated values.
#  • Splits the string into individual tokens.
#  • Trims surrounding spaces from each token.
#  • Converts the token to INT; if the token is not numeric the cast
#    yields NULL (Spark’s cast semantics).
#  • Returns a one‑column “virtual table” named Data (INT).
#
#  Translation Notes:
#  ─────────────────────
#  - T‑SQL’s table‑valued function is expressed in Databricks as
#    a persistent function that “RETURNS TABLE”.
#  - The `@RowData NVARCHAR(MAX)` parameter becomes `V_RowData STRING`
#    (Spark SQL string = T‑SQL NVARCHAR).
#  - The SQL body uses `SPLIT` + `explode` to walk over each comma‑separated
#    fragment.  `trim()` mirrors the original `LTRIM`/`RTRIM`.
#  - Spark’s `CAST(... AS INT)` automatically returns NULL for non‑numeric
#    fragments, which matches the behaviour of the original T‑SQL
#    `ISNUMERIC` guard.
#  - Fully‑qualified references use the target catalog
#    `dbe_dbx_internships` and the target schema `switchschema`;
#    no reference to the source MS‑SQL catalog (ETL/dbo) is present.
#
#  Usage (example):
#  ----------------
#  SELECT * FROM dbe_dbx_internships.switchschema.fn_IntCSVSplit('1, 2, abc, 4')
#  -- Returns a table with rows [1, 2, NULL, 4]
#

spark.sql("""
CREATE OR REPLACE FUNCTION dbe_dbx_internships.switchschema.fn_IntCSVSplit (
    V_RowData STRING
)
RETURNS TABLE (
    Data INT
)
RETURN (
    SELECT CAST(TRIM(v) AS INT) AS Data
    FROM explode(split(V_RowData, ',')) AS t(v)
);
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function with a sample CSV string
# MAGIC SELECT *
# MAGIC FROM dbe_dbx_internships.switchschema.fn_IntCSVSplit('1,2,3,4,5');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
