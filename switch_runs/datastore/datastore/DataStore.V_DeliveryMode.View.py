# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_DeliveryMode.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_DeliveryMode.View.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  View creation – V_DeliveryMode
# -------------------------------------------------------------
# This view mirrors the T‑SQL definition:
#   CREATE VIEW [DataStore].[V_DeliveryMode] AS
#     SELECT
#       UPPER(DDMS.DataAreaId)      AS CompanyCode,
#       UPPER(DDMS.ModeCode)        AS DeliveryModeCode,
#       ISNULL(DDMS.ModeDescription,'_N/A') AS DeliveryModeName
#     FROM dbo.SMRBIDeliveryModeStaging DDMS
#
# In Databricks the view is created with a fully qualified name
#   <catalog>.<schema>.V_DeliveryMode
# All object names are therefore prefixed with the catalog and schema
# that you specified for the target database.
#
# SQL functions have been mapped to their Databricks equivalents:
#   ISNULL → NVL
#   UPPER   → UPPER (unchanged)
#
# No native equivalent for `SET ANSI_NULLS` / `SET QUOTED_IDENTIFIER`
# is required in Databricks; they are vendor‑specific T‑SQL options
# and have been omitted with an explanatory comment above.

spark.sql(f"""
    CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_DeliveryMode` AS
    SELECT
        UPPER(DDMS.DataAreaId)                        AS CompanyCode,
        UPPER(DDMS.ModeCode)                          AS DeliveryModeCode,
        NVL(DDMS.ModeDescription, '_N/A')            AS DeliveryModeName
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIDeliveryModeStaging` DDMS
""")

# COMMAND ----------

# -------------------------------------------------------------
#  Optional: verify that the view has been created
# -------------------------------------------------------------

# Load a small sample to ensure the view works correctly
sample_df = spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_DeliveryMode` LIMIT 5")
display(sample_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
