# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_MonthKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324112609-alwp/ETL.fn_MonthKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# ----------------------------------------------------
# Setup cell – import required classes for UDF handling
# ----------------------------------------------------
from pyspark.sql import types as T
from pyspark.sql.functions import udf

# COMMAND ----------

# ----------------------------------------------------
# Function equivalent to the T‑SQL scalar function
# [ETL].[fn_MonthKeyInt] – generates an int in the form YYYYMM
# ----------------------------------------------------
def fn_MonthKeyInt_py(date):
    """
    Convert a Python datetime (or pandas Timestamp) to an integer
    representing the year and month in the same way as the original
    T‑SQL function:

        ISNULL(CONVERT(Integer,
                       RIGHT(CONVERT(varchar, DatePart(Year,@Date)),4) +
                       RIGHT('0'+CONVERT(varchar, DatePart(Month,@Date)),2)),
               190001)

    If the input `date` is None (SQL NULL), the function returns
    190001, matching the default in the T‑SQL function.

    Args:
        date (datetime or None): Input date value.

    Returns:
        int: YYYYMM integer or 190001 for NULL input.
    """
    # Handle NULL input analogous to ISNULL in T‑SQL
    if date is None:
        return 190001

    # Extract year and month components
    year = date.year
    month = date.month

    # Build the integer representation
    return year * 100 + month

# COMMAND ----------

# ----------------------------------------------------
# Register the Python function as a Spark UDF
# ----------------------------------------------------
# Spark UDFs are referenced by name in SQL queries.  We
# register it under the same simple name that the T‑SQL
# function used, so that existing code can call
# `SELECT fn_MonthKeyInt(OrderDate) ...` in SQL.
spark.udf.register("fn_MonthKeyInt", fn_MonthKeyInt_py, T.IntegerType())

# COMMAND ----------

# ----------------------------------------------------
# Example usage – applying the UDF to a Delta table
# ----------------------------------------------------
# NOTE: All database object references must use the fully‑qualified
# form `dbe_dbx_internships.switchschema.{object_name}`.
df_with_month = spark.sql("""
    SELECT
        *,
        fn_MonthKeyInt(OrderDate) AS MonthKey
    FROM dbe_dbx_internships.switchschema.Orders
""")

# COMMAND ----------

# Show the result of the sample query
df_with_month.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
