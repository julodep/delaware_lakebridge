# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Markup.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Markup.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the view `V_Markup` in the specified catalog and schema
# ------------------------------------------------------------------
sql_create_view = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Markup` AS
SELECT
    CompanyId      AS CompanyCode,
    TransRecId,
    MarkupCategory,
    TransTableId   AS TransTableCode,
    COALESCE(`TRANSPORT`, 0)   AS SurchargeTransport,
    COALESCE(`aankoop`,   0)   AS SurchargePurchase,
    COALESCE(`DlvCost1`, 0)   AS SurchargeDelivery,
    COALESCE(`TRANSPORT`, 0) +
    COALESCE(`aankoop`,   0) +
    COALESCE(`DlvCost1`, 0)  AS SurchargeTotal
FROM
    (
        SELECT
            UPPER(DataAreaId) AS CompanyId,
            MarkupCode,
            TransRecId,
            `Value`,
            MarkupCategory,
            TransTableId
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIMarkupTransStaging`
        WHERE TransTableId IN
            (
                SELECT TableId FROM `dbe_dbx_internships`.`datastore`.`SqlDictionary`
            )
    ) SourceTable
PIVOT
    (
        MIN(`Value`) FOR MarkupCode IN ('TRANSPORT', 'aankoop', 'DlvCost1')
    )
"""

# COMMAND ----------

# Execute the view creation
spark.sql(sql_create_view)

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify the view by fetching a few rows
# ------------------------------------------------------------------
df = spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_Markup` LIMIT 10")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
