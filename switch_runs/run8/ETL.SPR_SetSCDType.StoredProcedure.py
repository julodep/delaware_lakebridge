# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.SPR_SetSCDType.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324112609-alwp/ETL.SPR_SetSCDType.StoredProcedure.sql`

# COMMAND ----------

# --------------------------------------------------
# Setup: import any helper libraries (Databricks provides SparkSession and dbutils by default)
# --------------------------------------------------
import sys  # used for graceful exit if needed
from pyspark.sql.types import StructType, ArrayType, MapType

# COMMAND ----------

# --------------------------------------------------
# Helper function to set SCDType metadata (as column comments) for columns of a Delta table
# --------------------------------------------------
def SPR_SetSCDType(schema_name: str, table_name: str):
    """
    Mimics the behaviour of the T‑SQL procedure SPR_SetSCDType.
    For each column in the specified table, calculates its SCDType
    ('SCD0', 'Historical', 'PK', or 'SCD1') and stores that value
    in the column COMMENT field of the Delta table.
    """
    # Build the fully‑qualified table name: dbe_dbx_internships.switchschema.{schema_name}.{table_name}
    full_table = f"dbe_dbx_internships.switchschema.{schema_name}.{table_name}"

    # --------------------------------------------------
    # Retrieve column metadata using the catalog API
    # --------------------------------------------------
    try:
        cols_info = spark.catalog.listColumns(full_table)
    except Exception as e:
        print(f"Failed to list columns for table '{full_table}': {e}")
        return

    # --------------------------------------------------
    # Map column names to SCDType values according to the legacy rules
    # --------------------------------------------------
    scd_mapping = {}
    for col in cols_info:
        col_name = col.name
        col_type = col.dataType  # this is a DataType object; use its simpleString for DDL
        # Determine SCDType
        if col_name in ["CreatedETLRunId", "ModifiedETLRunId"]:
            scd_type = "SCD0"
        elif col_name in ["SCDStartDate", "SCDEndDate", "SCDIsCurrent"]:
            scd_type = "Historical"
        elif col_name == f"{table_name}Id":
            scd_type = "PK"
        else:
            scd_type = "SCD1"
        # Avoid placeholder types for complex columns
        if isinstance(col_type, (StructType, ArrayType, MapType)):
            scd_mapping[col_name] = (scd_type, None, True)  # None indicates skip
        else:
            scd_mapping[col_name] = (scd_type, col_type.simpleString(), False)

    # --------------------------------------------------
    # Apply the comments to each column in the Delta table
    # --------------------------------------------------
    for col_name, (scd_type, col_type_str, skip) in scd_mapping.items():
        if skip:
            print(f"Skipping complex column '{col_name}' (struct/array/map) due to placeholder type.")
            continue
        try:
            # Use backticks to quote identifiers safely
            alter_sql = (
                f"ALTER TABLE `{full_table}` "
                f"CHANGE COLUMN `{col_name}` `{col_name}` {col_type_str} "
                f"COMMENT '{scd_type}'"
            )
            spark.sql(alter_sql)
            print(f"Set SCDType comment for column '{col_name}' to '{scd_type}'.")
        except Exception as e:
            print(f"Failed to alter column '{col_name}' in table '{full_table}': {e}")

# COMMAND ----------

# --------------------------------------------------
# Parameter handling via widgets (allow the user to specify schema and table names)
# --------------------------------------------------
dbutils.widgets.text("SchemaName", "switchschema", "Target schema name")
dbutils.widgets.text("TableName", "Products", "Target table name")

# COMMAND ----------

# Retrieve parameters
schema_name_input = dbutils.widgets.get("SchemaName")
table_name_input = dbutils.widgets.get("TableName")

# COMMAND ----------

# --------------------------------------------------
# Execute the procedure with the provided parameters
# --------------------------------------------------
try:
    SPR_SetSCDType(schema_name_input, table_name_input)
except Exception as err:
    print(f"An error occurred while setting SCDType properties: {err}")
    dbutils.notebook.exit(f"Failure: {err}")

# COMMAND ----------

# --------------------------------------------------
# Optionally, display the updated table schema to verify the comments
# --------------------------------------------------
try:
    updated_schema_df = spark.sql(
        f"DESCRIBE `{f'dbe_dbx_internships.switchschema.{schema_name_input}.{table_name_input}'}`"
    )
    display(updated_schema_df)
except Exception as e:
    print(f"Could not describe updated table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [UNSUPPORTED_DATATYPE] Unsupported data type "_PLACEHOLDER_". SQLSTATE: 0A000
# MAGIC == SQL (line 1, position 130) ==
# MAGIC ...`_placeholder_` `_placeholder_` _placeholder_ COMMENT 'SCD0'
# MAGIC                                    ^^^^^^^^^^^^^
# MAGIC
# MAGIC ```
