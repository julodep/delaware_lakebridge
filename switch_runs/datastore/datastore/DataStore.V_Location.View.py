# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Location.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Location.View.sql`

# COMMAND ----------

# ------------------------------------------------------------
# CREATE VIEW V_Location  (Databricks / Spark SQL)
# ------------------------------------------------------------
# This view selects distinct location information from the
# staging tables, normalising the data and returning the
# following columns:
#   - LocationId
#   - LocationCode
#   - LocationName
#   - LocationCodeName  (concatenation of code and name)
#   - DimensionName
#
# The square–bracketed identifiers in T‑SQL (e.g. [Value])
# are replaced with the plain column names.  
# String concatenation is performed using Spark's CONCAT.
#
# Use the fully‑qualified names `dbe_dbx_internships`.`datastore`.`table_name`
# when replacing the placeholders with your actual catalog/schema.
# -----------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Location` AS
SELECT DISTINCT
    DFTS.FinancialTagRecId AS LocationId,
    DFTS.Value            AS LocationCode,
    DFTS.Description      AS LocationName,
    CONCAT(DFTS.Value, ' ', DFTS.Description) AS LocationCodeName,
    DAS.DimensionName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIDimensionFinancialTagStaging`   DFTS
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeDirCategoryStaging` DADCS
    ON DFTS.FinancialTagCategory = DADCS.DirCategory
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeStaging` DAS
    ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
WHERE DAS.DimensionName = 'Location'
""")

# COMMAND ----------

# ------------------------------------------------------------------
# If you want to verify that the view was created successfully,
# you can query it or list all views in the schema:
# ------------------------------------------------------------------

# List the view
views = spark.sql(f"SHOW VIEWS IN `dbe_dbx_internships`.`datastore`")
display(views)

# COMMAND ----------

# Quick sample query
sample = spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_Location` LIMIT 5")
display(sample)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
