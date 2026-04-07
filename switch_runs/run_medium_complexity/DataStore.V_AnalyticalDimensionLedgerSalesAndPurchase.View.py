# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AnalyticalDimensionLedgerSalesAndPurchase.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_medium/DataStore.V_AnalyticalDimensionLedgerSalesAndPurchase.View.sql`

# COMMAND ----------

# ---------------------------------------------------------
# Create a view that consolidates dimension attributes
# The view is defined in the target catalog `dbe_dbx_internships` and
# the target schema `switchschema`.  All table references are fully‑qualified
# and use the Delta Lake equivalent of T‑SQL functions.
# ---------------------------------------------------------

# Create the view using a pure Spark SQL statement.
# The query is wrapped in parentheses before the UNION ALL to avoid
# syntax errors in Spark SQL.
spark.sql("""
CREATE OR REPLACE VIEW dbe_dbx_internships.switchschema.V_AnalyticalDimensionLedgerSalesAndPurchase AS
(
    SELECT 
        DefaultDimension                                   AS DefaultDimensionId,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'MainAccount'          THEN DisplayValue END), '_N/A') AS MainAccount,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Intercompany'         THEN DisplayValue END), '_N/A') AS Intercompany,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Business_segment'    THEN DisplayValue END), '_N/A') AS BusinessSegment,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Customer'            THEN DisplayValue END), '_N/A') AS EndCustomer,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Department'          THEN DisplayValue END), '_N/A') AS Department,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Local_account'       THEN DisplayValue END), '_N/A') AS LocalAccount,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Location'            THEN DisplayValue END), '_N/A') AS Location,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Product'              THEN DisplayValue END), '_N/A') AS Product,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Shipment_Contract'   THEN DisplayValue END), '_N/A') AS ShipmentContract,
        COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Vendor'              THEN DisplayValue END), '_N/A') AS Vendor
    FROM (
        -- Source data: each row contains one attribute/value pair for a dimension
        SELECT
            DDVS.DefaultDimension                 AS DefaultDimension,
            DDVS.DisplayValue                    AS DisplayValue,
            DDVS.Name                            AS DimensionAttributeName
        FROM dbe_dbx_internships.switchschema.SMRBIDefaultDimensionViewStaging DDVS
    ) AS pivot_source
    GROUP BY DefaultDimension
)
UNION ALL
-- Add a dummy row that represents a “no dimension” case
SELECT 
    -1                                                     AS DefaultDimensionId,
    '_N/A'                                                 AS MainAccount,
    '_N/A'                                                 AS Intercompany,
    '_N/A'                                                 AS BusinessSegment,
    '_N/A'                                                 AS EndCustomer,
    '_N/A'                                                 AS Department,
    '_N/A'                                                 AS LocalAccount,
    '_N/A'                                                 AS Location,
    '_N/A'                                                 AS Product,
    '_N/A'                                                 AS ShipmentContract,
    '_N/A'                                                 AS Vendor
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2829)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW dbe_dbx_internships.switchschema.V_AnalyticalDimensionLedgerSalesAndPurchase AS (     SELECT          DefaultDimension                                   AS DefaultDimensionId,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'MainAccount'          THEN DisplayValue END), '_N/A') AS MainAccount,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Intercompany'         THEN DisplayValue END), '_N/A') AS Intercompany,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Business_segment'    THEN DisplayValue END), '_N/A') AS BusinessSegment,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Customer'            THEN DisplayValue END), '_N/A') AS EndCustomer,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Department'          THEN DisplayValue END), '_N/A') AS Department,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Local_account'       THEN DisplayValue END), '_N/A') AS LocalAccount,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Location'            THEN DisplayValue END), '_N/A') AS Location,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Product'              THEN DisplayValue END), '_N/A') AS Product,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Shipment_Contract'   THEN DisplayValue END), '_N/A') AS ShipmentContract,         COALESCE(MIN(CASE WHEN DimensionAttributeName = 'Vendor'              THEN DisplayValue END), '_N/A') AS Vendor     FROM (         -- Source data: each row contains one attribute/value pair for a dimension         SELECT             DDVS.DefaultDimension                 AS DefaultDimension,             DDVS.DisplayValue                    AS DisplayValue,             DDVS.Name                            AS DimensionAttributeName         FROM dbe_dbx_internships.switchschema.SMRBIDefaultDimensionViewStaging DDVS     ) AS pivot_source     GROUP BY DefaultDimension ) UNION ALL -- Add a dummy row that represents a “no dimension” case SELECT      -1                                                     AS DefaultDimensionId,     '_N/A'                                                 AS MainAccount,     '_N/A'                                                 AS Intercompany,     '_N/A'                                                 AS BusinessSegment,     '_N/A'                                                 AS EndCustomer,     '_N/A'                                                 AS Department,     '_N/A'                                                 AS LocalAccount,     '_N/A'                                                 AS Location,     '_N/A'                                                 AS Product,     '_N/A'                                                 AS ShipmentContract,     '_N/A'                                                 AS Vendor
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
