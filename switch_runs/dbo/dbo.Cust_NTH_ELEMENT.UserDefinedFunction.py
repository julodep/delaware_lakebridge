# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.Cust_NTH_ELEMENT.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.Cust_NTH_ELEMENT.UserDefinedFunction.sql`

# COMMAND ----------

# ---------------------------------------------
# NOTE: This notebook registers a persistent function
# named `Cust_NTH_ELEMENT` in Unity Catalog.
#
# T‑SQL original:
#   CREATE FUNCTION dbo.Cust_NTH_ELEMENT (@Input NVARCHAR(MAX),
#                                        @Delim CHAR = '-',
#                                        @N INT = 0)
#   RETURNS NVARCHAR(MAX)
#   AS BEGIN
#       RETURN (SELECT value 
#               FROM OPENJSON('["' + REPLACE(@Input,@Delim,'","') + '"]')
#               WHERE [key] = @N)
#   END
#
# Key translations:
#   • NVARCHAR(MAX)   → STRING
#   • CHAR default '-' is expressed with a default value
#   • JSON split logic is replaced by Spark's `split()` and
#     `element_at()` functions.  Spark arrays are 1‑based, so we
#     add 1 to the index passed by the caller to mimic the
#     0‑based behaviour of T‑SQL's OPENJSON.
#   • The function is declared as a persistent catalog object
#     using CREATE OR REPLACE FUNCTION; no spark.udf.register()
#     is invoked.
# ---------------------------------------------

# Register the function in the specified catalog and schema.
# Replace `dbe_dbx_internships` and `dbo` with your actual values.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `dbe_dbx_internships`.`dbo`.Cust_NTH_ELEMENT (
    input STRING,
    delim STRING DEFAULT '-',
    N INT DEFAULT 0
)
RETURNS STRING
RETURN element_at(split(input, delim), N + 1)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
