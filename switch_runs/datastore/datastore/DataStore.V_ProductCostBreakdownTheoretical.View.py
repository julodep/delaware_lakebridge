# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductCostBreakdownTheoretical.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductCostBreakdownTheoretical.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create a persistent view in Unity Catalog
# ------------------------------------------------------------------
# The T‑SQL view `V_ProductCostBreakdownTheoretical` pulls a distinct
# set of columns from the staging table `SMRBIBOMCalcTransStaging`.
# In Databricks, we translate this to a **CREATE OR REPLACE VIEW** statement
# that fully qualifies every table name with `dbe_dbx_internships` and `datastore`.
#
# NOTE:
#   * Square brackets `[ ]` used in T‑SQL are not required in Spark SQL.
#   * Column names that are reserved words (e.g. `Level_`) are left as-is
#     because Spark tolerates the trailing underscore; if any column name
#     conflicts with a keyword you can wrap it in backticks.
#   * The view is independent of any session variables; all references
#     are static table names.
#

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductCostBreakdownTheoretical` AS
SELECT
    DISTINCT
    BOM,
    BOMCalcTransRecId,
    CalcGroupId AS CalcGroupCode,
    CalcType,
    ConsistOfPrice,
    ConsumptionConstant,
    ConsumptionVariable,
    ConsumpType,
    CostCalculationMethod,
    CostGroupId AS CostGroupCode,
    CostMarkup,
    CostMarkupQty,
    CostPrice,
    CostPriceModelUsed,
    CostPriceQty,
    CostPriceUnit,
    DataAreaId,
    InventDimId AS InventDimCode,
    Level_,
    LineNum,
    NetWeightQty,
    NumOfSeries,
    OprId,
    OprNum,
    OprNumNext,
    OprPriority,
    ParentBOMCalcTrans,
    PriceCalcId,
    Qty,
    Resource_,
    SalesMarkup,
    SalesMarkupQty,
    SalesPrice,
    SalesPriceQty,
    SalesPriceUnit,
    TransDate,
    UnitId AS UnitCode
FROM `dbe_dbx_internships`.`datastore`.`SMRBIBOMCalcTransStaging`;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
