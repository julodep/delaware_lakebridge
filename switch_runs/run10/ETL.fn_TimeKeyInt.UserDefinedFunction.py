# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.fn_TimeKeyInt.UserDefinedFunction
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/switch_scripts_functions/ETL.fn_TimeKeyInt.UserDefinedFunction.sql`

# COMMAND ----------

# Define a Spark SQL scalar function that converts a TIMESTAMP to an integer
# by concatenating the hour and minute components (e.g., 14:07 -> 1407).
# Equivalent logic to the original T‑SQL function:
#   CAST(EXTRACT(HOUR FROM V_Time) AS STRING) + LPAD(EXTRACT(MINUTE FROM V_Time), 2, '0')
# The result is then cast to INT.
spark.sql("""
CREATE OR REPLACE FUNCTION dbe_dbx_internships.switchschema.fn_TimeKeyInt(
    V_Time TIMESTAMP
)
RETURNS INT
RETURN CAST(
    CONCAT(
        CAST(EXTRACT(HOUR FROM V_Time) AS STRING),
        LPAD(CAST(EXTRACT(MINUTE FROM V_Time) AS STRING), 2, '0')
    ) AS INT
);
""")

# COMMAND ----------

# Test fn_TimeKeyInt with various cases
test_cases = [
    ("14:07", "2023-01-01 14:07:00", 1407),
    ("09:00", "2023-01-01 09:00:00", 900),
    ("00:00", "2023-01-01 00:00:00", 0),
    ("23:59", "2023-01-01 23:59:00", 2359),
    ("08:05", "2023-01-01 08:05:00", 805),
    ("12:30", "2023-01-01 12:30:00", 1230),
]

print(f"{'Input Time':<12} {'Expected':>10} {'Got':>10} {'Pass?':>8}")
print("-" * 44)

all_passed = True
for label, ts, expected in test_cases:
    result = spark.sql(f"""
        SELECT dbe_dbx_internships.switchschema.fn_TimeKeyInt(
            CAST('{ts}' AS TIMESTAMP)
        ) AS result
    """).collect()[0]["result"]

    passed = result == expected
    all_passed = all_passed and passed
    status = "✅ PASS" if passed else f"❌ FAIL"
    print(f"{label:<12} {expected:>10} {result:>10} {status:>8}")

print("-" * 44)
print(f"\n{'All tests passed! ✅' if all_passed else 'Some tests failed. ❌'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
