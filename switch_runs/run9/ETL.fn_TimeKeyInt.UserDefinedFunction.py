# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/switch_scripts_functions/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# --------------------------------------------------------------------------------
# 1️⃣  Setup: import necessary Spark SQL types & functions
# --------------------------------------------------------------------------------
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, expr, col

# COMMAND ----------

# --------------------------------------------------------------------------------
# 2️⃣  Define the Python implementation of the original T‑SQL function
#     fn_TimeKeyInt(V_Time TIMESTAMP) → INT
#     The function returns an integer of the form HHMM (e.g., 13:05 → 1305)
# --------------------------------------------------------------------------------
def fn_TimeKeyInt_py(v_time):
    """
    Compute integer representation of a timestamp in HHMM format.
    
    Parameters
    ----------
    v_time : datetime.datetime or None
        Timestamp value from which to extract hour and minute.

    Returns
    -------
    int or None
        Integer value HHMM or None if the input is null.
    """
    if v_time is None:
        return None
    return int(f"{v_time.hour}{v_time.minute:02d}")

# COMMAND ----------

# --------------------------------------------------------------------------------
# 3️⃣  Register the UDF in Spark with a fully‑qualified name
#     (catalog.schema.function_name).  Databricks/UDFs are still identified by name
#     in SQL, so we use the fully‑qualified namespace for consistency with the
#     required output format.
# --------------------------------------------------------------------------------
spark.udf.register(
    name="dbe_dbx_internships.switchschema.fn_TimeKeyInt",
    f=fn_TimeKeyInt_py,
    returnType=IntegerType()
)

# COMMAND ----------

# --------------------------------------------------------------------------------
# 4️⃣  Optional test: Use the UDF in a SQL expression
# --------------------------------------------------------------------------------
# Create a sample DataFrame with a timestamp column
sample_df = (
    spark
    .createDataFrame(
        [
            ("2024-01-01 13:05:00",),
            ("2024-01-02 00:00:00",),
            ("2024-01-03 09:07:00",)
        ],
        ["V_Time"]
    )
    .withColumn("V_Time", col("V_Time").cast("timestamp"))
)

# COMMAND ----------

# Apply the UDF via Spark SQL expression
tested_df = sample_df.withColumn(
    "TimeKey",
    expr("dbe_dbx_internships.switchschema.fn_TimeKeyInt(V_Time)")
)

# COMMAND ----------

# Display the result to verify correctness
display(tested_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
