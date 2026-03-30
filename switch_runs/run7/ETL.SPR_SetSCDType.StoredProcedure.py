# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.SPR_SetSCDType.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324112609-alwp/ETL.SPR_SetSCDType.StoredProcedure.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import required PySpark functions (Databricks provides SparkSession)
# ------------------------------------------------------------
from pyspark.sql.functions import when, col, lit, concat

# COMMAND ----------

# ------------------------------------------------------------
# Define widgets for input parameters (schema and table names)
# ------------------------------------------------------------
dbutils.widgets.text("SchemaName", "switchschema", "Target schema")
dbutils.widgets.text("TableName", "", "Target table name")

# COMMAND ----------

# Retrieve widget values
schema_name = dbutils.widgets.get("SchemaName").strip()
table_name = dbutils.widgets.get("TableName").strip()

# COMMAND ----------

if not table_name:
    raise ValueError("TableName widget must be provided.")

# COMMAND ----------

# Fully‑qualified table identifier for later ALTER statements
catalog_name = "dbe_dbx_internships"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# COMMAND ----------

# ------------------------------------------------------------
# NOTE:
# The original T‑SQL procedure adds an extended property named 'SCDType'
# to each column via sp_addextendedproperty. Databricks Delta Lake does not
# support SQL Server extended properties. As an alternative, we annotate
# each column with a comment that includes the SCD type.
# ------------------------------------------------------------

# ------------------------------------------------------------
# Step 1: Retrieve column metadata from the metastore
# ------------------------------------------------------------
# The INFORMATION_SCHEMA view is available in Databricks. It returns
# columns for the specified catalog, schema, and table.
columns_df = spark.sql(f"""
SELECT 
    column_name,
    table_name,
    table_schema
FROM information_schema.columns
WHERE table_name = '{table_name}'
  AND table_schema = '{schema_name}'
""")

# COMMAND ----------

# ------------------------------------------------------------
# Step 2: Determine the SCD type for each column
# ------------------------------------------------------------
# Mapping logic mirrors the original CASE expression:
#   - 'CreatedETLRunId' or 'ModifiedETLRunId'   → SCD0
#   - 'SCDStartDate', 'SCDEndDate', 'SCDIsCurrent' → Historical
#   - Column named <TableName>Id               → PK
#   - All others                               → SCD1
# ------------------------------------------------------------
columns_with_scd = columns_df.withColumn(
    "scd_type",
    when(col("column_name").isin("CreatedETLRunId", "ModifiedETLRunId"), lit("SCD0"))
    .when(col("column_name").isin("SCDStartDate", "SCDEndDate", "SCDIsCurrent"), lit("Historical"))
    .when(col("column_name") == concat(lit(table_name), lit("Id")), lit("PK"))
    .otherwise(lit("SCD1"))
)

# COMMAND ----------

# ------------------------------------------------------------
# Step 3: Apply the SCD type as a column comment
# ------------------------------------------------------------
# For each column we issue an ALTER TABLE statement that adds a comment.
# This is safe for a modest number of columns (metadata size is tiny).
# ------------------------------------------------------------
try:
    for row in columns_with_scd.collect():
        col_name = row["column_name"]
        scd_type = row["scd_type"]
        # Build the COMMENT string. Adjust as needed for your documentation standards.
        comment_text = f"SCDType: {scd_type}"
        # Execute the ALTER statement. Backticks protect identifiers with special characters.
        spark.sql(f"""
        ALTER TABLE {full_table_name}
        ALTER COLUMN `{col_name}` COMMENT '{comment_text}'
        """)
except Exception as e:
    # If any ALTER fails, log the error and re‑raise.
    print(f"Error applying SCD comments: {e}")
    raise

# COMMAND ----------

# ------------------------------------------------------------
# Optional: Verify that comments were applied (display result)
# ------------------------------------------------------------
# Spark's DESCRIBE EXTENDED shows column comments.
verification_df = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
display(verification_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
