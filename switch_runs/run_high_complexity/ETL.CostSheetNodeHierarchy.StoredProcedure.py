# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.CostSheetNodeHierarchy.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_high/ETL.CostSheetNodeHierarchy.StoredProcedure.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1️⃣ Imports & helpers
# ------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    upper,
    lit
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create test staging table with hierarchy data
# ------------------------------------------------------------
# 🧪 Create a test staging table for the stored procedure
# ------------------------------------------------------------
staging_tbl = "dbe_dbx_internships.switchschema.SMRBICostSheetNodeTableStaging"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {staging_tbl} (
    NodeId        INT,
    ParentNodeId  INT,
    Code          STRING,
    CostGroupId   STRING,
    Description   STRING,
    DataAreaId    STRING,
    Level_        INT
)
""")

# Insert test hierarchy data (only if table is empty)
if spark.table(staging_tbl).count() == 0:
    spark.sql(f"""
    INSERT INTO {staging_tbl} VALUES
        -- Level 0: roots (ParentNodeId = 0)
        (1,  0, 'ROOT-A', 'CG-ROOT-A', 'Root Node A',       'DA01', 0),
        (2,  0, 'ROOT-B', 'CG-ROOT-B', 'Root Node B',       'DA01', 0),
        -- Level 1: children of root A
        (10, 1, 'MAT',    'CG-MAT',    'Materials',          'DA01', 1),
        (11, 1, 'LAB',    'CG-LAB',    'Labor',              'DA01', 1),
        -- Level 1: child of root B
        (20, 2, 'OVH',    'CG-OVH',    'Overhead',           'DA01', 1),
        -- Level 2: children of Materials
        (100, 10, 'RAW',  'CG-RAW',    'Raw Materials',      'DA01', 2),
        (101, 10, 'PKG',  'CG-PKG',    'Packaging',          'DA01', 2),
        -- Level 2: child of Labor
        (110, 11, 'DIR',  'CG-DIR',    'Direct Labor',       'DA01', 2),
        -- Level 2: child of Overhead
        (200, 20, 'UTL',  'CG-UTL',    'Utilities',          'DA01', 2)
    """)
    print(f"Inserted test data into {staging_tbl}")
else:
    print(f"{staging_tbl} already has data, skipping insert")

# COMMAND ----------

# ------------------------------------------------------------
# 2️⃣ Source table
# ------------------------------------------------------------
# The staging table that contains the node hierarchy
src_tbl = "dbe_dbx_internships.switchschema.SMRBICostSheetNodeTableStaging"

# COMMAND ----------

# Verify that the table exists – if not, nothing to do
if not spark._jsparkSession.catalog().tableExists(src_tbl):
    raise RuntimeError(f"Source table {src_tbl} does not exist")

# COMMAND ----------

src_df = spark.table(src_tbl)

# COMMAND ----------

# ------------------------------------------------------------
# 3️⃣ Compute the maximum depth of the hierarchy
# ------------------------------------------------------------
# The original T‑SQL code calculates
#   maxLevels = CASE WHEN EXISTS (SELECT 1 FROM depthCheck) THEN MAX(Level) + 1 ELSE 3 END
max_level_val = src_df.agg({"Level_": "max"}).first()[0]

# COMMAND ----------

if max_level_val is None:          # empty source table
    max_levels = 3
else:
    max_levels = int(max_level_val) + 1

# COMMAND ----------

print(f"Determined maximum hierarchy depth: {max_levels}")

# COMMAND ----------

# ------------------------------------------------------------
# 4️⃣ Build a multi‑level left‑join chain
# ------------------------------------------------------------
# We build a chain of joins that walks the hierarchy from the root
# (Level_ = 0) up to the maximum depth.  Each iteration adds a new
# "level" alias L{n} that contains the parent of the previous level.
# -------------------------------------------------------------------------

# **Step 4.1 – Root level (level 1)**
level_df = src_df.select(
    col("NodeId").alias("NodeId_1"),
    col("ParentNodeId").alias("ParentNodeId_1"),
    col("Code").alias("Code_1"),
    col("CostGroupId").alias("CostGroupId_1"),
    col("Description").alias("Description_1"),
    col("DataAreaId").alias("DataAreaId_1"),
    col("Level_").alias("Level_1")
)

# COMMAND ----------

# **Step 4.2 – Iteratively join each deeper level**
for lvl in range(2, max_levels + 1):
    # Alias names for the parent table at this depth
    parent_df = src_df.select(
        col("NodeId").alias(f"NodeId_{lvl}"),
        col("ParentNodeId").alias(f"ParentNodeId_{lvl}"),
        col("Code").alias(f"Code_{lvl}"),
        col("CostGroupId").alias(f"CostGroupId_{lvl}"),
        col("Description").alias(f"Description_{lvl}"),
        col("DataAreaId").alias(f"DataAreaId_{lvl}"),
        col("Level_").alias(f"Level_{lvl}")
    )
    # Each new level joins on the ParentNodeId of the previous level
    join_cond = level_df[f"ParentNodeId_{lvl-1}"] == parent_df[f"NodeId_{lvl}"]
    level_df = level_df.join(parent_df, on=join_cond, how="left")

# COMMAND ----------

# At this point `level_df` contains columns for every level (1…max_levels)

# ------------------------------------------------------------
# 5️⃣ Build the final result set
# ------------------------------------------------------------
# We mirror the original SELECT:
#     SELECT DISTINCT
#         L1.NodeId                                   AS AccId,
#         UPPER(COALESCE(L1.DataAreaId))              AS DataAreaId,
#         COALESCE(L1.CostGroupId)                   AS Level_1_CostGroupId,
#          … (repeating for each level) …
# where L1.Level_ = 0 AND L1.ParentNodeId = 0
# ---------------------------------------------------------------------------

# Columns for the SELECT
select_cols = [
    col("NodeId_1").alias("AccId"),
    upper(coalesce(col("DataAreaId_1"), lit(""))).alias("DataAreaId")
]

# COMMAND ----------

# Add Level_n_* columns
for lvl in range(1, max_levels + 1):
    select_cols.extend([
        col(f"CostGroupId_{lvl}").alias(f"Level_{lvl}_CostGroupId"),
        col(f"Description_{lvl}").alias(f"Level_{lvl}_Description"),
        col(f"Code_{lvl}").alias(f"Level_{lvl}_Code")
    ])

# COMMAND ----------

# Filter to keep only root rows as in the original procedure
final_df = (
    level_df
    .filter((col("Level_1") == 0) & (col("ParentNodeId_1") == 0))
    .select(*select_cols)
    .distinct()            # SELECT DISTINCT in the original query
)

# COMMAND ----------

# ------------------------------------------------------------
# 6️⃣ Persist the result as a Delta table
# ------------------------------------------------------------
dest_tbl = "dbe_dbx_internships.switchschema.CostSheetNodeHierarchyTable"

# COMMAND ----------

# Drop any pre‑existing table – exactly like the T‑SQL DROP TABLE in the dynamic SQL
spark.sql(f"DROP TABLE IF EXISTS {dest_tbl}")

# COMMAND ----------

# Write the DataFrame to a managed Delta table
final_df.write.format("delta").mode("overwrite").saveAsTable(dest_tbl)

# COMMAND ----------

print(f"Table `{dest_tbl}` created with {final_df.count()} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
