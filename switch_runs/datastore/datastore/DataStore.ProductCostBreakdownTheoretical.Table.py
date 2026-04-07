# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownTheoretical.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductCostBreakdownTheoretical.Table.sql`

# COMMAND ----------

# ==============================================================
#  Create persistent Delta table `ProductCostBreakdownTheoretical`
#  in Unity Catalog with fully‑qualified name:
#      dbe_dbx_internships.datastore.ProductCostBreakdownTheoretical
#
#  Data type mapping:
#      INT          -> INT
#      BIGINT       -> BIGINT
#      NVARCHAR     -> STRING
#      NUMERIC(p,s) -> DECIMAL(p,s)
#      DATETIME     -> TIMESTAMP
#
#  The table is created as a Delta table so it can be queried
#  from any notebook in the same workspace.
# ==============================================================

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`ProductCostBreakdownTheoretical` (
    BOM INT NOT NULL,
    BOMCalcTransRecId BIGINT NOT NULL,
    CalcGroupCode STRING NOT NULL,
    CalcType INT NOT NULL,
    ConsistOfPrice STRING NOT NULL,
    ConsumptionConstant DECIMAL(32,16) NOT NULL,
    ConsumptionVariable DECIMAL(32,16) NOT NULL,
    ConsumpType INT NOT NULL,
    CostCalculationMethod INT NOT NULL,
    CostGroupCode STRING NOT NULL,
    CostMarkup DECIMAL(32,16) NOT NULL,
    CostMarkupQty DECIMAL(32,16) NOT NULL,
    CostPrice DECIMAL(32,16) NOT NULL,
    CostPriceModelUsed INT NOT NULL,
    CostPriceQty DECIMAL(32,16) NOT NULL,
    CostPriceUnit DECIMAL(32,12) NOT NULL,
    DataAreaId STRING NOT NULL,
    InventDimCode STRING NOT NULL,
    Level_ INT NOT NULL,
    LineNum DECIMAL(32,16) NOT NULL,
    NetWeightQty DECIMAL(32,12) NOT NULL,
    NumOfSeries DECIMAL(32,6) NOT NULL,
    OprId STRING NOT NULL,
    OprNum INT NOT NULL,
    OprNumNext INT NOT NULL,
    OprPriority INT NOT NULL,
    ParentBOMCalcTrans BIGINT NOT NULL,
    PriceCalcId STRING NOT NULL,
    Qty DECIMAL(32,6) NOT NULL,
    Resource_ STRING NOT NULL,
    SalesMarkup DECIMAL(32,6) NOT NULL,
    SalesMarkupQty DECIMAL(32,6) NOT NULL,
    SalesPrice DECIMAL(32,6) NOT NULL,
    SalesPriceQty DECIMAL(32,6) NOT NULL,
    SalesPriceUnit DECIMAL(32,12) NOT NULL,
    TransDate TIMESTAMP NOT NULL,
    UnitCode STRING NOT NULL
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
