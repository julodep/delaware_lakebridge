# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_MonthKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324112609-alwp/ETL.fn_MonthKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# Register a Python UDF to replace the T‑SQL scalar function fn_MonthKeyInt
# The original function returns an integer in the form YYYYMM for a given datetime,
# defaulting to 190001 when the input is NULL.

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import datetime

# COMMAND ----------

def _month_key_int(dt: datetime.datetime) -> int:
    """
    Convert a datetime value to an integer month key (YYYYMM).
    Returns 190001 if the input is None.
    """
    if dt is None:
        return 190001
    # Ensure we have a Python datetime; Spark may pass a java.sql.Timestamp which
    # behaves like a Python datetime via the `strftime` method.
    return int(dt.strftime("%Y%m"))

# COMMAND ----------

# Create the UDF and register it with the fully‑qualified name expected in the catalog.
month_key_int_udf = udf(_month_key_int, IntegerType())
spark.udf.register("dbe_dbx_internships.switchschema.fn_MonthKeyInt", month_key_int_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
