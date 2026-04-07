# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AnalyticalDimensionLedger.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_AnalyticalDimensionLedger.View.sql`

# COMMAND ----------

# Create or replace the analytical dimension ledger view in Databricks
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_AnalyticalDimensionLedger` AS

-- Main aggregation part
SELECT
  ValueCombinationRecId                                      AS LedgerDimensionId,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'MainAccount'  THEN EntityInstance END), -1) AS MainAccount,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Intercompany' THEN EntityInstance END), -1) AS Intercompany,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Business_segment' THEN EntityInstance END), -1) AS BusinessSegment,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'EndCustomer'  THEN EntityInstance END), -1) AS EndCustomer,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Department'  THEN EntityInstance END), -1) AS Department,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Local_account' THEN EntityInstance END), -1) AS LocalAccount,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Location'    THEN EntityInstance END), -1) AS Location,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Product'     THEN EntityInstance END), -1) AS Product,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Shipment_Contract' THEN EntityInstance END), -1) AS ShipmentContract,
  COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Vendor'      THEN EntityInstance END), -1) AS Vendor
FROM
  (
    SELECT
      DAVVS.ValueCombinationRecId,
      DAVVS.DisplayValue,
      DAVVS.DimensionAttribute,
      DAVVS.EntityInstance,
      DAS.DimensionName AS DimensionAttributeName
    FROM
      `dbe_dbx_internships`.`datastore`.SMRBIDimensionAttributeLevelValueAllViewStaging   DAVVS
    JOIN
      (
        SELECT DimensionAttributeRecId, DimensionName
        FROM `dbe_dbx_internships`.`datastore`.SMRBIDimensionAttributeStaging
        UNION
        SELECT 5637144587, 'MainAccount'
      ) DAS
      ON DAVVS.DimensionAttribute = DAS.DimensionAttributeRecId
  ) P
GROUP BY ValueCombinationRecId

-- UNION‑ALL row containing all -1s (the “empty” record in T‑SQL)
UNION ALL
SELECT
  -1 AS LedgerDimensionId,
  -1 AS MainAccount,
  -1 AS Intercompany,
  -1 AS BusinessSegment,
  -1 AS EndCustomer,
  -1 AS Department,
  -1 AS LocalAccount,
  -1 AS Location,
  -1 AS Product,
  -1 AS ShipmentContract,
  -1 AS Vendor
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2251)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_AnalyticalDimensionLedger` AS  -- Main aggregation part SELECT   ValueCombinationRecId                                      AS LedgerDimensionId,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'MainAccount'  THEN EntityInstance END), -1) AS MainAccount,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Intercompany' THEN EntityInstance END), -1) AS Intercompany,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Business_segment' THEN EntityInstance END), -1) AS BusinessSegment,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'EndCustomer'  THEN EntityInstance END), -1) AS EndCustomer,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Department'  THEN EntityInstance END), -1) AS Department,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Local_account' THEN EntityInstance END), -1) AS LocalAccount,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Location'    THEN EntityInstance END), -1) AS Location,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Product'     THEN EntityInstance END), -1) AS Product,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Shipment_Contract' THEN EntityInstance END), -1) AS ShipmentContract,   COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Vendor'      THEN EntityInstance END), -1) AS Vendor FROM   (     SELECT       DAVVS.ValueCombinationRecId,       DAVVS.DisplayValue,       DAVVS.DimensionAttribute,       DAVVS.EntityInstance,       DAS.DimensionName AS DimensionAttributeName     FROM       `_placeholder_`.`_placeholder_`.SMRBIDimensionAttributeLevelValueAllViewStaging   DAVVS     JOIN       (         SELECT DimensionAttributeRecId, DimensionName         FROM `_placeholder_`.`_placeholder_`.SMRBIDimensionAttributeStaging         UNION         SELECT 5637144587, 'MainAccount'       ) DAS       ON DAVVS.DimensionAttribute = DAS.DimensionAttributeRecId   ) P GROUP BY ValueCombinationRecId  -- UNION‑ALL row containing all -1s (the “empty” record in T‑SQL) UNION ALL SELECT   -1 AS LedgerDimensionId,   -1 AS MainAccount,   -1 AS Intercompany,   -1 AS BusinessSegment,   -1 AS EndCustomer,   -1 AS Department,   -1 AS LocalAccount,   -1 AS Location,   -1 AS Product,   -1 AS ShipmentContract,   -1 AS Vendor
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
