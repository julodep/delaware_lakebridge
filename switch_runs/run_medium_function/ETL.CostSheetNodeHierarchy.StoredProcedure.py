# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.CostSheetNodeHierarchy.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_high/ETL.CostSheetNodeHierarchy.StoredProcedure.sql`

# COMMAND ----------

# ------------------------------------------------------------
#  Databricks notebook:  ETL.CostSheetNodeHierarchy
#  Purpose:
#      Replicate the logic of the T‑SQL `ETL.CostSheetNodeHierarchy` procedure
#      (dynamic, hierarchical SQL builder) using Spark SQL / DataFrames.
#
#  Notes on translation:
#      • All references are fully qualified, pointing to the Unity
#        Catalog database `dbe_dbx_internships` and schema `switchschema`.
#      • The original script builds a dynamic SELECT that iterates over
#        `maxLevels` (the maximum depth of the hierarchy) and
#        `LEFT JOIN`s the staging table to itself.
#      • In Spark we can accomplish the same with a simple iterative
#        join loop – the result is a single DataFrame that contains
#        one row per hierarchy root (`L1`) together with the nested
#        attributes from each level.  The SELECT logic (COALESCE, aliases)
#        is expressed with DataFrame column operations.
#      • The final result is written as a persistent Delta table
#        `CostSheetNodeHierarchyTable` in the same catalog/schema.
#      • There is no stored‑procedure construct in Databricks; the
#        notebook itself (or a Python function) acts as the "procedure".
# ------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Load staging table
# ------------------------------------------------------------------
staging_tbl = "dbe_dbx_internships.switchschema.SMRBICostSheetNodeTableStaging"
stg = spark.table(staging_tbl)

# COMMAND ----------

# ------------------------------------------------------------------
# 2. Determine maximum hierarchy depth (= @maxLevels in T‑SQL)
# ------------------------------------------------------------------
# The T‑SQL defines "depthCheck" by recursively joining the table
# starting at nodes where ParentNodeId=0 and Level_ = 0.
# We mimic this by iteratively joining until no new rows appear,
# then counting the maximum level found.

# Base set (root nodes)
root_nodes = stg.filter((F.col("ParentNodeId") == 0) & (F.col("Level_") == 0))

# COMMAND ----------

# Build the recursive set
current = root_nodes.select("NodeId").alias("Curr")
visited = root_nodes.select("NodeId").distinct()
max_level = 0

# COMMAND ----------

while True:
    # Find children of the current set
    children = (
        stg
        .join(current, stg.ParentNodeId == current.Curr, how="inner")
        .select("NodeId", "Level_")
    )
    # Remove nodes already seen
    new_nodes = children.subtract(visited)
    count_new = new_nodes.count()
    if count_new == 0:
        # No more levels – stop
        break
    max_level += 1
    visited = visited.union(new_nodes).distinct()
    # Prepare for next iteration
    current = (
        children
        .join(stg, stg.NodeId == F.col("NodeId"), how="inner")
        .select("NodeId")
        .distinct()
    )

# COMMAND ----------

# If the table is empty, default to 3 levels as the original script does
final_max_levels = max_level + 1 if max_level > 0 else 3

# COMMAND ----------

# ------------------------------------------------------------------
# 3. Build a DataFrame that contains one row per root node
#    together with all nested levels up to @maxLevels
# ------------------------------------------------------------------
# Start from the root nodes (`L1`)
df = root_nodes.alias("L1")

# COMMAND ----------

# List of alias names so we can reference them later
alias_names = ["L1"]

# COMMAND ----------

# Iteratively join the next level (L2, L3, …)
for lvl in range(2, final_max_levels + 1):
    alias = f"L{lvl}"
    df = (
        df.join(
            stg.alias(alias),
            on=[
                F.col(f"L{lvl-1}.NodeId") == F.col(f"{alias}.ParentNodeId"),
                F.col(f"L{lvl-1}.DataAreaId") == F.col(f"{alias}.DataAreaId")
            ],
            how="left"
        )
    )
    alias_names.append(alias)

# COMMAND ----------

# ------------------------------------------------------------------
# 4. Select the columns with the same column names that the T‑SQL
#    produced (COALESCE is represented by F.coalesce – if a column is
#    NULL it will simply return NULL; the query logic is preserved).
# ------------------------------------------------------------------
select_expr = []

# COMMAND ----------

for lvl in range(1, final_max_levels + 1):
    alias = f"L{lvl}"
    select_expr.extend(
        [
            F.coalesce(F.col(f"{alias}.CostGroupId")).alias(f"Level_{lvl}_CostGroupId"),
            F.coalesce(F.col(f"{alias}.Description")).alias(f"Level_{lvl}_Description"),
            F.coalesce(F.col(f"{alias}.Code")).alias(f"Level_{lvl}_Code")
        ]
    )

# COMMAND ----------

# AccId and DataAreaId columns come from the root node (L1)
select_expr.append(F.col("L1.NodeId").alias("AccId"))
select_expr.append(
    F.upper(F.coalesce(F.col("L1.DataAreaId"))).alias("DataAreaId")
)

# COMMAND ----------

# Build the final DataFrame
hierarchy_df = df.select(*select_expr)

# COMMAND ----------

# ------------------------------------------------------------------
# 5. Persist the result as a Delta table
#    This is equivalent to the T‑SQL "SELECT … INTO dbo.CostSheetNodeHierarchyTable"
# ------------------------------------------------------------------
output_table = "dbe_dbx_internships.switchschema.CostSheetNodeHierarchyTable"

# COMMAND ----------

# Drop the table if it already exists – the original script always writes
# with "DROP TABLE" before the INSERT.
spark.sql(f"DROP TABLE IF EXISTS {output_table}")

# COMMAND ----------

# Write as a Delta table (persistent in UC catalog)
hierarchy_df.write.mode("overwrite").saveAsTable(output_table)

# COMMAND ----------

print(f"Hierarchy table written to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
