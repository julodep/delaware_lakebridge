# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_InventoryMovements.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_InventoryMovements.View.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Databricks notebook – create V_InventoryMovements view
# --------------------------------------------------------------
# This view aggregates inventory movement data from staging tables.
# All references are fully‑qualified to the target catalog/schema:
#   `dbe_dbx_internships`.`datastore`.`table_name`
# --------------------------------------------------------------

# Drop the view first if it already exists to avoid conflicts
spark.sql(f"""
DROP VIEW IF EXISTS `dbe_dbx_internships`.`datastore`.`V_InventoryMovements`
""")

# COMMAND ----------

# Create the view
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_InventoryMovements` AS
SELECT
    IVS1.TransRecId,
    IVS1.DataAreaId        AS CompanyCode,

    -- Replace empty string currency codes with a placeholder
    COALESCE(NULLIF(IT.CURRENCYCODE, ''), '_N/A') AS Currency,

    -- Aggregated cost metrics (negated sums)
    -(SUM(IT.CostAmountPhysical * IVS2.QTYSettled / IT.QTY)) AS CostPhysical,
    -(SUM(IT.CostAmountPosted   * IVS2.QTYSettled / IT.QTY)) AS CostFinancial,
    -(SUM(IT.CostAmountAdjustment * IVS2.QTYSettled / IT.QTY)) AS CostAdjustment

FROM
    `dbe_dbx_internships`.`datastore`.`SMRBIInventSettlementStaging` IVS1
    INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBIInventSettlementStaging` IVS2
        ON  IVS1.SettleTransId          = IVS2.SettleTransId
        AND UPPER(IVS1.DataAreaId)     = UPPER(IVS2.DataAreaId)
        AND IVS1.InventSettlementRecId <> IVS2.InventSettlementRecId

    INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBIInventTransStaging` IT
        ON IT.RecId                     = IVS2.TransRecId
        AND UPPER(IT.DataAreaId)        = UPPER(IVS2.DataAreaId)

-- The original query contained a dummy `WHERE 1=1` clause; it has no effect and is omitted.
-- Filtering for non‑zero quantities (in the original source) is retained:
WHERE
    IVS1.QtySettled <> 0
    AND IT.QTY <> 0

GROUP BY
    IVS1.TransRecId,
    IVS1.DataAreaId,
    IT.CURRENCYCODE
""")

# COMMAND ----------

# Verify that the view was created
spark.sql(f"SHOW CREATE VIEW `dbe_dbx_internships`.`datastore`.`V_InventoryMovements`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1417)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_InventoryMovements` AS SELECT     IVS1.TransRecId,     IVS1.DataAreaId        AS CompanyCode,      -- Replace empty string currency codes with a placeholder     COALESCE(NULLIF(IT.CURRENCYCODE, ''), '_N/A') AS Currency,      -- Aggregated cost metrics (negated sums)     -(SUM(IT.CostAmountPhysical * IVS2.QTYSettled / IT.QTY)) AS CostPhysical,     -(SUM(IT.CostAmountPosted   * IVS2.QTYSettled / IT.QTY)) AS CostFinancial,     -(SUM(IT.CostAmountAdjustment * IVS2.QTYSettled / IT.QTY)) AS CostAdjustment  FROM     `_placeholder_`.`_placeholder_`.`SMRBIInventSettlementStaging` IVS1     INNER JOIN `_placeholder_`.`_placeholder_`.`SMRBIInventSettlementStaging` IVS2         ON  IVS1.SettleTransId          = IVS2.SettleTransId         AND UPPER(IVS1.DataAreaId)     = UPPER(IVS2.DataAreaId)         AND IVS1.InventSettlementRecId <> IVS2.InventSettlementRecId      INNER JOIN `_placeholder_`.`_placeholder_`.`SMRBIInventTransStaging` IT         ON IT.RecId                     = IVS2.TransRecId         AND UPPER(IT.DataAreaId)        = UPPER(IVS2.DataAreaId)  -- The original query contained a dummy `WHERE 1=1` clause; it has no effect and is omitted. -- Filtering for non‑zero quantities (in the original source) is retained: WHERE     IVS1.QtySettled <> 0     AND IT.QTY <> 0  GROUP BY     IVS1.TransRecId,     IVS1.DataAreaId,     IT.CURRENCYCODE
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC Error in query 2: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'VIEW'. SQLSTATE: 42601 (line 1, pos 20)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN SHOW CREATE VIEW `_placeholder_`.`_placeholder_`.`V_InventoryMovements`
# MAGIC --------------------^^^
# MAGIC
# MAGIC ```
