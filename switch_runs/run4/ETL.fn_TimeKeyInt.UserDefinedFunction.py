# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Register a Python UDF that mimics the T‑SQL/SQL‑style function 
# `ETL.fn_TimeKeyInt`. The function converts a TIMESTAMP to an 
# integer in the form HHMM (hour concatenated with a zero‑padded 
# minute).
# ------------------------------------------------------------

# Import necessary Spark types and functions
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# COMMAND ----------

# ----------------------------------------------------------------
# Define the Python implementation.
# It receives a Python `datetime` (or Spark `TimestampType`) value
# and returns an integer like 905 for 09:05, 1415 for 14:15, etc.
# Returns None when the input is null.
# ----------------------------------------------------------------
def _time_key_int(v_time):
    if v_time is None:
        return None
    # `v_time` is a `datetime.datetime` object when called from PySpark
    hour = v_time.hour               # 0‑23
    minute = v_time.minute           # 0‑59
    # Concatenate hour and minute (minute zero‑padded to two digits)
    return int(f"{hour}{minute:02d}")

# COMMAND ----------

# Create a Spark UDF from the Python function
fn_time_key_int_udf = udf(_time_key_int, IntegerType())

# COMMAND ----------

# Register the UDF in the Spark catalog (makes it available in SQL)
# The original function lived in the `ETL` schema, so we keep that 
# naming convention for compatibility.
spark.udf.register("ETL.fn_TimeKeyInt", fn_time_key_int_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
