# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# ----------------------------------------------
# Setup: import required modules and define a Python UDF 
# that replicates the T‑SQL function `fn_TimeKeyInt`.
# The function converts a TIMESTAMP to an integer 
# in the format HHMM (e.g., 13:05 → 1305).
# ----------------------------------------------

from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import udf

# COMMAND ----------

# Python implementation of the time‑key logic
def _time_key_int(v_time):
    """
    Convert a timestamp to an integer key of the form HHMM.
    Returns None if the input is null.
    """
    if v_time is None:
        return None
    # `v_time` is a Python datetime (or pandas Timestamp) object
    hour = v_time.hour
    minute = v_time.minute
    # Build a zero‑padded string HHMM and cast to int
    return int(f"{hour}{minute:02d}")

# COMMAND ----------

# Register the function as a Spark SQL UDF.
# The full name follows the catalog/schema convention:
# `dbe_dbx_internships.ETL.fn_TimeKeyInt`
spark.udf.register(
    "dbe_dbx_internships.ETL.fn_TimeKeyInt",  # qualified function name
    _time_key_int,
    IntegerType()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
