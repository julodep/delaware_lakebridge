# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324101927-oqdi/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Register a Python UDF that replicates the T‑SQL function
# `fn_TimeKeyInt` (returns an integer HHMM from a TIMESTAMP).
# The UDF is made available in the Spark session and can be
# used in SQL queries like any built‑in function.
# ------------------------------------------------------------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# COMMAND ----------

def _fn_time_key_int(v_time):
    """
    Convert a timestamp to an integer representing HHMM.
    Example: 13:05 --> 1305
    Returns None if the input is null.
    """
    if v_time is None:
        return None
    # v_time is a python datetime.datetime (or pandas Timestamp)
    hour = v_time.hour          # 0‑23
    minute = v_time.minute      # 0‑59
    # Concatenate hour and zero‑padded minute, then cast to int
    return int(f"{hour}{minute:02d}")

# COMMAND ----------

# Register the function so it can be called from Spark SQL.
# The name chosen matches the original T‑SQL name.
spark.udf.register("fn_TimeKeyInt", _fn_time_key_int, IntegerType())

# COMMAND ----------

# ------------------------------------------------------------
# Create a permanent view to expose the UDF under the
# fully‑qualified name `<catalog>.<schema>.fn_TimeKeyInt`.
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE VIEW dbe_dbx_internships.switchschema.fn_TimeKeyInt AS
SELECT fn_TimeKeyInt AS fn_TimeKeyInt
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
