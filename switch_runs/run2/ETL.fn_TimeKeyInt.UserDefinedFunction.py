# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323121756-cmb3/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import required modules for defining and registering UDFs
# ------------------------------------------------------------
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# COMMAND ----------

# ------------------------------------------------------------
# Drop existing function (if any) to allow redefinition
# ------------------------------------------------------------
spark.sql("DROP FUNCTION IF EXISTS ETL.fn_TimeKeyInt")

# COMMAND ----------

# ------------------------------------------------------------
# Define the Python function that mimics the T‑SQL logic
# ------------------------------------------------------------
def fn_time_key_int(v_time):
    """
    Convert a timestamp to an integer time key in HHMM format.
    Example: 2023-07-15 09:05:00 -> 905
    """
    if v_time is None:
        return None
    # v_time is a Python datetime object when called from Spark
    hour = v_time.hour       # 0‑23
    minute = v_time.minute   # 0‑59
    return hour * 100 + minute

# COMMAND ----------

# ------------------------------------------------------------
# Register the function as a Spark SQL UDF in the ETL database/schema
# ------------------------------------------------------------
spark.udf.register("ETL.fn_TimeKeyInt", fn_time_key_int, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
