# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore4.ProductionOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore4/datastore4_volume/datastore4/DataStore4.ProductionOrder.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the ProductionOrder table in Unity Catalog
# --------------------------------------------------------------
# All columns are mapped from T‑SQL types to Spark SQL types as follows:
#   NVARCHAR(*)          → STRING
#   INT                  → INT
#   NUMERIC(p, s)        → DECIMAL(p, s)
#   DATETIME             → TIMESTAMP
# --------------------------------------------------------------

# Ensure `catalog` and `schema` variables are defined before executing this code.
spark.sql(f"""
CREATE TABLE `{catalog}`.`{schema}`.`ProductionOrder` (
    ProductionOrderCode STRING NOT NULL,
    ProductionOrderStatus STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    RouteCode STRING NOT NULL,
    SiteCode STRING NOT NULL,
    OperationCode STRING NOT NULL,
    OperationNumber INT NOT NULL,
    ResourceCode STRING NOT NULL,
    CalculationType STRING NOT NULL,
    ProductionResourceCode STRING NOT NULL,
    CostGroupCode STRING NOT NULL,
    InventDimCode STRING NOT NULL,
    BOMCode STRING NOT NULL,
    AccountingCurrencyCode STRING NOT NULL,
    ReportingCurrencyCode STRING NOT NULL,
    GroupCurrencyCode STRING NOT NULL,
    DefaultExchangeRateTypeCode STRING NOT NULL,
    BudgetExchangeRateTypeCode STRING NOT NULL,
    AppliedExchangeRateAC DECIMAL(38, 6),          -- Nullable by default
    AppliedExchangeRateRC DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateGC DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateRC_Budget DECIMAL(38, 21) NOT NULL,
    AppliedExchangeRateGC_Budget DECIMAL(38, 21) NOT NULL,
    JournalPostingDate TIMESTAMP NOT NULL,
    RequestedDeliveryDate TIMESTAMP NOT NULL,
    ScheduledDeliveryDate TIMESTAMP NOT NULL,
    ActualDeliveryDate TIMESTAMP NOT NULL,
    ScheduledProductionStartDate TIMESTAMP NOT NULL,
    ScheduledProductionEndDate TIMESTAMP NOT NULL,
    ProductionStartDate TIMESTAMP,
    ProductionEndDate TIMESTAMP,
    OTIF DECIMAL(38, 6) NOT NULL,
    InventUnit STRING NOT NULL,
    OriginalEstimatedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    OriginalEstimatedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    OriginalEstimatedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    OriginalEstimatedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,
    OriginalEstimatedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    OriginalEstimatedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    EstimatedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    ProducedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,
    TotalEstimatedConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    TotalEstimatedConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    TotalEstimatedConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    NetEstimatedConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    NetEstimatedConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    NetEstimatedConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    RealConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    RealConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    RealConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,
    TotalScrapQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,
    TotalScrapQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,
    TotalScrapQuantity_SalesUnit DECIMAL(38, 6) NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 3490)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`ProductionOrder` (     ProductionOrderCode STRING NOT NULL,     ProductionOrderStatus STRING NOT NULL,     CompanyCode STRING NOT NULL,     ProductCode STRING NOT NULL,     RouteCode STRING NOT NULL,     SiteCode STRING NOT NULL,     OperationCode STRING NOT NULL,     OperationNumber INT NOT NULL,     ResourceCode STRING NOT NULL,     CalculationType STRING NOT NULL,     ProductionResourceCode STRING NOT NULL,     CostGroupCode STRING NOT NULL,     InventDimCode STRING NOT NULL,     BOMCode STRING NOT NULL,     AccountingCurrencyCode STRING NOT NULL,     ReportingCurrencyCode STRING NOT NULL,     GroupCurrencyCode STRING NOT NULL,     DefaultExchangeRateTypeCode STRING NOT NULL,     BudgetExchangeRateTypeCode STRING NOT NULL,     AppliedExchangeRateAC DECIMAL(38, 6),          -- Nullable by default     AppliedExchangeRateRC DECIMAL(38, 21) NOT NULL,     AppliedExchangeRateGC DECIMAL(38, 21) NOT NULL,     AppliedExchangeRateRC_Budget DECIMAL(38, 21) NOT NULL,     AppliedExchangeRateGC_Budget DECIMAL(38, 21) NOT NULL,     JournalPostingDate TIMESTAMP NOT NULL,     RequestedDeliveryDate TIMESTAMP NOT NULL,     ScheduledDeliveryDate TIMESTAMP NOT NULL,     ActualDeliveryDate TIMESTAMP NOT NULL,     ScheduledProductionStartDate TIMESTAMP NOT NULL,     ScheduledProductionEndDate TIMESTAMP NOT NULL,     ProductionStartDate TIMESTAMP,     ProductionEndDate TIMESTAMP,     OTIF DECIMAL(38, 6) NOT NULL,     InventUnit STRING NOT NULL,     OriginalEstimatedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     OriginalEstimatedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     OriginalEstimatedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     OriginalEstimatedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,     OriginalEstimatedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,     OriginalEstimatedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,     EstimatedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantityDetail_InventoryUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantityDetail_PurchaseUnit DECIMAL(38, 6) NOT NULL,     ProducedQuantityDetail_SalesUnit DECIMAL(38, 6) NOT NULL,     TotalEstimatedConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     TotalEstimatedConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     TotalEstimatedConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     NetEstimatedConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     NetEstimatedConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     NetEstimatedConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     RealConsumptionQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     RealConsumptionQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     RealConsumptionQuantity_SalesUnit DECIMAL(38, 6) NOT NULL,     TotalScrapQuantity_InventoryUnit DECIMAL(38, 6) NOT NULL,     TotalScrapQuantity_PurchaseUnit DECIMAL(38, 6) NOT NULL,     TotalScrapQuantity_SalesUnit DECIMAL(38, 6) NOT NULL );
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
