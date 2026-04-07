# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AnalyticalDimensionLedgerSalesAndPurchase.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_AnalyticalDimensionLedgerSalesAndPurchase.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – Transpiled view definition
# ------------------------------------------------------------------
# This script creates the equivalent of the T‑SQL view
# `[DataStore].[V_AnalyticalDimensionLedgerSalesAndPurchase]`
# in Spark SQL.  All object references use the fully‑qualified
# naming convention
#      `dbe_dbx_internships`.`datastore`.`object_name`
#
# The original view pivots a dimension table, then aggregates the
# pivoted columns with `MIN` and substitutes any NULL with the
# string `'_N/A'`.  In Spark SQL we rebuild this logic manually
# using a CTE and `COALESCE`, which is the Spark equivalent of
# `ISNULL`.  Spark can also perform pivot operations, but the
# manually‑assembled form keeps the logic explicit and is
# easier to read/debug.
#
# ------------------------------------------------------------------

# 1️⃣  Build a CTE that will pivot the dimension data
#     and then compute minimum values for each attribute.
with_sql = f"""
WITH PivotedData AS (
    /* 1. Gather all dimension rows */
    SELECT
        DefaultDimension,
        MIN(CASE WHEN DimensionAttributeName = 'MainAccount'     THEN DisplayValue END) AS MainAccount,
        MIN(CASE WHEN DimensionAttributeName = 'Intercompany'   THEN DisplayValue END) AS Intercompany,
        MIN(CASE WHEN DimensionAttributeName = 'Business_segment' THEN DisplayValue END) AS BusinessSegment,
        MIN(CASE WHEN DimensionAttributeName = 'Customer'       THEN DisplayValue END) AS Customer,
        MIN(CASE WHEN DimensionAttributeName = 'Department'     THEN DisplayValue END) AS Department,
        MIN(CASE WHEN DimensionAttributeName = 'Local_account'  THEN DisplayValue END) AS LocalAccount,
        MIN(CASE WHEN DimensionAttributeName = 'Location'       THEN DisplayValue END) AS Location,
        MIN(CASE WHEN DimensionAttributeName = 'Product'        THEN DisplayValue END) AS Product,
        MIN(CASE WHEN DimensionAttributeName = 'Shipment_Contract' THEN DisplayValue END) AS ShipmentContract,
        MIN(CASE WHEN DimensionAttributeName = 'Vendor'         THEN DisplayValue END) AS Vendor
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIDefaultDimensionViewStaging`
    GROUP BY DefaultDimension
),

/* 2️⃣  Add a row that represents “all data” (DefaultDimension = -1)
   and fills every column with the placeholder '_N/A'.  This imitates
   the UNION ALL clause of the original view. */
AllDataRow AS (
    SELECT
        -1 AS DefaultDimension,
        '_N/A' AS MainAccount,
        '_N/A' AS Intercompany,
        '_N/A' AS BusinessSegment,
        '_N/A' AS Customer,
        '_N/A' AS Department,
        '_N/A' AS LocalAccount,
        '_N/A' AS Location,
        '_N/A' AS Product,
        '_N/A' AS ShipmentContract,
        '_N/A' AS Vendor
)

SELECT
    /* Alias the dimension ID – the original view used DefaultDimensionId. */
    DefaultDimension AS DefaultDimensionId,

    /* Use COALESCE to replace any NULLs with the default string. */
    COALESCE(MainAccount, '_N/A')          AS MainAccount,
    COALESCE(Intercompany, '_N/A')         AS Intercompany,
    COALESCE(BusinessSegment, '_N/A')      AS BusinessSegment,
    COALESCE(Customer, '_N/A')             AS EndCustomer,
    COALESCE(Department, '_N/A')           AS Department,
    COALESCE(LocalAccount, '_N/A')         AS LocalAccount,
    COALESCE(Location, '_N/A')             AS Location,
    COALESCE(Product, '_N/A')             AS Product,
    COALESCE(ShipmentContract, '_N/A')    AS ShipmentContract,
    COALESCE(Vendor, '_N/A')               AS Vendor
FROM PivotedData

/* 3️⃣  Union the “all data” row so the view has the same semantics
      as the original T‑SQL version. */
UNION ALL
SELECT
    DefaultDimension AS DefaultDimensionId,
    MainAccount,
    Intercompany,
    BusinessSegment,
    Customer          AS EndCustomer,
    Department,
    LocalAccount,
    Location,
    Product,
    ShipmentContract,
    Vendor
FROM AllDataRow
"""

# COMMAND ----------

# 4️⃣  Create or replace the view in the specified catalog/schema.
spark.sql(f"CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_AnalyticalDimensionLedgerSalesAndPurchase` AS {with_sql}")

# COMMAND ----------

print("View `dbe_dbx_internships`.`datastore`.`V_AnalyticalDimensionLedgerSalesAndPurchase` created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
