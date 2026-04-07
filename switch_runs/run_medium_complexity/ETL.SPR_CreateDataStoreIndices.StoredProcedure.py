# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.SPR_CreateDataStoreIndices.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_medium/ETL.SPR_CreateDataStoreIndices.StoredProcedure.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Setup imports and environment (Databricks provides spark by default)
# ------------------------------------------------------------------
import sys

# COMMAND ----------

# ------------------------------------------------------------------
# 2. Define parameters via widgets
# ------------------------------------------------------------------
# These widgets allow the caller to input the target schema and table for which indices should be created
dbutils.widgets.text("DataStoreSchema", "")
dbutils.widgets.text("DataStoreTable", "")

# COMMAND ----------

# ------------------------------------------------------------------
# 3. Retrieve and validate widget values
# ------------------------------------------------------------------
try:
    data_store_schema = dbutils.widgets.get("DataStoreSchema").strip()
    data_store_table   = dbutils.widgets.get("DataStoreTable").strip()
    if not data_store_schema or not data_store_table:
        raise ValueError("Both DataStoreSchema and DataStoreTable must be provided.")
except Exception as e:
    dbutils.notebook.exit(f"Parameter validation failed: {e}")

# COMMAND ----------

# ------------------------------------------------------------------
# 4. Identify the list of indices that need to be created
# ------------------------------------------------------------------
try:
    indices_df = spark.sql(f"""
        SELECT
            I.IndexName,
            ROW_NUMBER() OVER (ORDER BY I.IndexName) AS idx_row
        FROM dbe_dbx_internships.switchschema.StagingInputIndex I
        WHERE I.SourceName = 'Staging'
          AND I.TargetSchema = '{data_store_schema}'
          AND I.TargetTable = '{data_store_table}'
    """)
    rows_to_process = indices_df.count()
    if rows_to_process == 0:
        dbutils.notebook.exit("No indices found for the specified DataStoreSchema/Table.")
except Exception as e:
    dbutils.notebook.exit(f"Error retrieving indices list: {e}")

# COMMAND ----------

# ------------------------------------------------------------------
# 5. Loop over each index and process
# ------------------------------------------------------------------
try:
    # Collect the index names into a driver‑side list for iteration
    index_names = [row.IndexName for row in indices_df.select("IndexName").collect()]
    
    for idx_name in index_names:
        # Databricks currently does not support stored procedures via CALL.
        # As a workaround, we log the intended action here.
        print(f"Index '{idx_name}' would be created here (stored procedure not supported).")
except Exception as e:
    dbutils.notebook.exit(f"Error during index list processing: {e}")

# COMMAND ----------

# ------------------------------------------------------------------
# 6. Finished – report success
# ------------------------------------------------------------------
print("Index processing completed (no actual index creation due to Spark limitation).")
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
