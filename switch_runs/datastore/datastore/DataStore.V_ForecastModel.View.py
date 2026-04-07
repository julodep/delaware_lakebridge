# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ForecastModel.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ForecastModel.View.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Databricks notebook – Create the ForecastModel view
# ------------------------------------------------------------
# This script recreates the T‑SQL view:
#   DataStore.V_ForecastModel
# using fully‑qualified catalog and schema names
# (dbe_dbx_internships.datastore is applied to every table and view reference).
# ------------------------------------------------------------

# Replace dbe_dbx_internships and datastore with your actual catalog and schema names:
catalog = "my_catalog"   # <-- change to your catalog
schema  = "my_schema"    # <-- change to your schema

# COMMAND ----------

# 1.  Build the CREATE VIEW statement in Spark SQL
create_view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ForecastModel` AS
SELECT
    UPPER(FMS.DataAreaId)             AS CompanyCode,
    UPPER(FMS.ModelId)                AS ForecastModelCode,
    COALESCE(FMS.ModelName, '_N/A')   AS ForecastModelName,
    COALESCE(UPPER(FSMS.SubModelId), '_N/A') AS ForecastSubModelCode
FROM `dbe_dbx_internships`.`datastore`.`SMRBIForecastModelStaging` FMS
LEFT JOIN
    (SELECT DISTINCT * FROM `dbe_dbx_internships`.`datastore`.`SMRBIForecastSubModelStaging`) FSMS
ON FMS.ModelId = FSMS.ModelId
"""

# COMMAND ----------

# 2.  Execute the statement
spark.sql(create_view_sql)

# COMMAND ----------

# 3.  (Optional) Verify the view was created correctly
print("View 'V_ForecastModel' created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
