# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# Import required PySpark functions and types
from pyspark.sql.functions import udf, hour, minute, lpad, concat, col
from pyspark.sql.types import IntegerType

# COMMAND ----------

# -------------------------------------------------------------------------
# Original T‑SQL / Spark‑SQL function definition:
# CREATE FUNCTION `ETL`.`fn_TimeKeyInt` ( V_Time TIMESTAMP ) RETURNS int ...
# This syntax is not directly supported in Databricks notebooks.
# Instead, we define an equivalent Python UDF and register it for use in SQL.
# -------------------------------------------------------------------------

def _time_key_int(v_time):
    """
    Convert a timestamp to an integer in HHMM format.
    Example: 2023-07-15 09:05:00 -> 905
    """
    if v_time is None:
        return None
    # Extract hour and minute components
    h = v_time.hour
    m = v_time.minute
    # Build HHMM string (hour may be 0‑23, minute 0‑59) and convert to int
    return int(f"{h}{m:02d}")

# COMMAND ----------

# Register the Python function as a Spark SQL UDF named fn_TimeKeyInt
spark.udf.register("fn_TimeKeyInt", _time_key_int, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
