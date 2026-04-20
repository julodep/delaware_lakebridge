# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.uspRebuildTablesD365.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.uspRebuildTablesD365.StoredProcedure.sql`

# COMMAND ----------

# ==========================================================
# Databricks notebook – Re‑build all base tables in a schema
# ----------------------------------------------------------
#  Purpose
#  -------
#  The original T‑SQL stored procedure dynamically builds and executes
#  a list of ALTER TABLE … REBUILD statements for every base table
#  that belongs to the *dbo* schema.  In Databricks / Delta Lake, the
#  ALTER TABLE … REBUILD command is not supported – tables do not
#  have indexes in the same sense as SQL Server.  A close‑equivalent
#  operation would be to trigger a stats recalculation or a vacuum,
#  but the procedure’s intent is simply to “re‑build” the table
#  metadata.  We therefore emulate the loop and provide a comment
#  explaining that the rebuild step is a no‑op in Delta Lake.
#
#  Fully‑qualified names are used throughout:
#      `dbe_dbx_internships`.`dbo`.`{object_name}`
#  Replace the placeholders with the actual catalog and schema names
#  when you run the notebook (e.g. set `catalog = 'my_catalog'`,
#  `schema  = 'my_schema'`).
# ==========================================================

# ------------------------------------------------------------------
# 1. Variables – replace these with the actual catalog and schema
# ------------------------------------------------------------------
catalog = 'dbe_dbx_internships'
schema  = 'dbo'

# COMMAND ----------

# ------------------------------------------------------------------

# ------------------------------------------------------------------
# 2. Identify all base tables in the target schema
# ------------------------------------------------------------------
#  We query the standard INFORMATION_SCHEMA view which Databricks
#  exposes by default when the metastore is populated.
tables_df = spark.sql(f"""
    SELECT
        table_schema,
        table_name
    FROM `dbe_dbx_internships`.`dbo`.information_schema.tables
    WHERE table_schema = 'dbo'
      AND table_type   = 'BASE TABLE'
""")

# COMMAND ----------

# Convert to a Python list for iterative processing
base_tables = [row.table_name for row in tables_df.collect()]

# COMMAND ----------

# ------------------------------------------------------------------
# 3. Loop over each table and "re‑build" it
# ------------------------------------------------------------------
for tbl in base_tables:
    full_name = f"`dbe_dbx_internships`.`dbo`.`{tbl}`"

    # --------------------------------------------------------------
    #  In SQL Server this would be:
    #     ALTER TABLE dbo.<TableName> REBUILD
    # --------------------------------------------------------------
    # delta tables do not support REBUILD.  If you need to refresh
    # metadata you could run ANALYZE TABLE, or VACUUM to reclaim space.
    # For the purpose of this conversion we simply emit a comment.
    #
    # Example of a possible Delta equivalent (optional, uncomment if needed):
    # spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS")
    #
    # For now we log the intended operation.
    print(f"-- Intended to rebuild table {full_name}")

# COMMAND ----------

    # If you truly want to trigger a stats recomputation, uncomment:
    # spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS")
# ------------------------------------------------------------------
# 4. Return a placeholder value to mirror the original SELECT –1 AS OUTPUT
# ------------------------------------------------------------------
output_val = -1
print(f"Procedure completed. OUTPUT = {output_val}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
