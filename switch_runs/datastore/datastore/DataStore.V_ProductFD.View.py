# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductFD.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductFD.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks Notebook – Create a persistent view `V_ProductFD`
#  ------------------------------------------------------------------
#  Purpose
#  -------
#  Re‑create the T‑SQL view `DataStore.V_ProductFD` in Databricks
#  using fully‑qualified object names.
#
#  The original T‑SQL definition was:
#      SELECT DISTINCT DFTS.FinancialTagRecId AS ProductFDId,
#                     DFTS.[Value]            AS ProductFDCode,
#                     DFTS.[Description]      AS ProductFDName,
#                     DFTS.Value + ' '+DFTS.DESCRIPTION
#                                        AS ProductFDCodeName,
#                     DimensionName = DAS.DimensionName
#        FROM dbo.SMRBIDimensionFinancialTagStaging   DFTS
#        INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
#               ON DFTS.FinancialTagCategory = DADCS.DirCategory
#        INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
#               ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
#        WHERE DAS.DimensionName = 'Product';
#
#  Translation Notes
#  -----------------
#  * All table names are prefixed with the configured dbe_dbx_internships and datastore.
#  * Square brackets used in T‑SQL (e.g. [Description]) are not required
#    in Spark; we reference the column directly as `Description`.
#  * The string concatenation `Value + ' ' + Description` is expressed with
#    Spark's `concat` function.
#  * The `DimensionName = DAS.DimensionName` alias is replaced by
#    `DAS.DimensionName AS DimensionName`.
#  * The view is created as a **persistent** view so it can be queried
#    like a regular table: `dbe_dbx_internships`.`datastore`.`V_ProductFD`.
#
# ------------------------------------------------------------------

# Replace these placeholders with your actual catalog and schema names.
catalog = "<catalog>"   # e.g., "my_warehouse"
schema  = "<schema>"    # e.g., "prod"

# COMMAND ----------

# Build the CREATE VIEW statement with fully‑qualified names.
view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductFD` AS
SELECT DISTINCT
    DFTS.FinancialTagRecId          AS ProductFDId,
    DFTS.Value                       AS ProductFDCode,
    DFTS.Description                AS ProductFDName,
    concat(DFTS.Value, ' ', DFTS.Description) AS ProductFDCodeName,
    DAS.DimensionName                AS DimensionName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIDimensionFinancialTagStaging`     AS DFTS
INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeDirCategoryStaging` AS DADCS
       ON DFTS.FinancialTagCategory = DADCS.DirCategory
INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeStaging` AS DAS
       ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
WHERE DAS.DimensionName = 'Product';
"""

# COMMAND ----------

# Execute the statement – this creates or replaces the view.
spark.sql(view_sql)

# COMMAND ----------

# ------------------------------------------------------------------
#  Verification – you can query the view to ensure it was created correctly.
# ------------------------------------------------------------------
df_view = spark.table(f"dbe_dbx_internships.datastore.V_ProductFD")
df_view.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
