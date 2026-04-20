# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore4.V_ProductionOrder.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore4/datastore4_volume/datastore4/DataStore4.V_ProductionOrder.View.sql`

# COMMAND ----------

# Create or replace the Unity Catalog view V_ProductionOrder
spark.sql("""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`V_ProductionOrder` AS
SELECT
    PO.ProductionOrderCode,
    PO.ProductionOrderStatus,
    PO.CompanyCode,
    PO.ProductCode,
    PO.RouteCode,
    PO.SiteCode,
    PO.OperationCode,
    PO.OperationNumber,
    PO.ResourceCode,
    PO.CalculationType,
    PO.ProductionResourceCode,
    PO.CostGroupCode,
    PO.InventDimCode,
    PO.BOMCode,

    L.AccountingCurrency AS AccountingCurrencyCode,
    L.ReportingCurrency AS ReportingCurrencyCode,
    L.GroupCurrency AS GroupCurrencyCode,
    L.ExchangeRateType AS DefaultExchangeRateTypeCode,
    L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode,

    CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateAC,
    COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC,
    COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC,
    COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget,
    COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget,

    PO.JournalPostingDate,
    PO.RequestedDeliveryDate,
    PO.ScheduledDeliveryDate,
    PO.ActualDeliveryDate,
    PO.ScheduledProductionStartDate,
    PO.ScheduledProductionEndDate,
    PO.ProductionStartDate,
    PO.ProductionEndDate,
    PO.OTIF,
    PO.InventUnit,

    /* Quantity calculations in the three unit types */
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.OriginalEstimatedQuantity
             ELSE PO.OriginalEstimatedQuantity * UOM0.Factor END, 0) AS OriginalEstimatedQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.OriginalEstimatedQuantity
             ELSE PO.OriginalEstimatedQuantity * UOM1.Factor END, 0) AS OriginalEstimatedQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.OriginalEstimatedQuantity
             ELSE PO.OriginalEstimatedQuantity * UOM2.Factor END, 0) AS OriginalEstimatedQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.OriginalEstimatedQuantityDetail
             ELSE PO.OriginalEstimatedQuantityDetail * UOM0.Factor END, 0) AS OriginalEstimatedQuantityDetail_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.OriginalEstimatedQuantityDetail
             ELSE PO.OriginalEstimatedQuantityDetail * UOM1.Factor END, 0) AS OriginalEstimatedQuantityDetail_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.OriginalEstimatedQuantityDetail
             ELSE PO.OriginalEstimatedQuantityDetail * UOM2.Factor END, 0) AS OriginalEstimatedQuantityDetail_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.EstimatedQuantity
             ELSE PO.EstimatedQuantity * UOM0.Factor END, 0) AS EstimatedQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.EstimatedQuantity
             ELSE PO.EstimatedQuantity * UOM1.Factor END, 0) AS EstimatedQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.EstimatedQuantity
             ELSE PO.EstimatedQuantity * UOM2.Factor END, 0) AS EstimatedQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.EstimatedQuantityDetail
             ELSE PO.EstimatedQuantityDetail * UOM0.Factor END, 0) AS EstimatedQuantityDetail_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.EstimatedQuantityDetail
             ELSE PO.EstimatedQuantityDetail * UOM1.Factor END, 0) AS EstimatedQuantityDetail_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.EstimatedQuantityDetail
             ELSE PO.EstimatedQuantityDetail * UOM2.Factor END, 0) AS EstimatedQuantityDetail_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.ProducedQuantity
             ELSE PO.ProducedQuantity * UOM0.Factor END, 0) AS ProducedQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.ProducedQuantity
             ELSE PO.ProducedQuantity * UOM1.Factor END, 0) AS ProducedQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.ProducedQuantity
             ELSE PO.ProducedQuantity * UOM2.Factor END, 0) AS ProducedQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.ProducedQuantityDetail
             ELSE PO.ProducedQuantityDetail * UOM0.Factor END, 0) AS ProducedQuantityDetail_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.ProducedQuantityDetail
             ELSE PO.ProducedQuantityDetail * UOM1.Factor END, 0) AS ProducedQuantityDetail_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.ProducedQuantityDetail
             ELSE PO.ProducedQuantityDetail * UOM2.Factor END, 0) AS ProducedQuantityDetail_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.TotalEstimatedConsumptionQuantity
             ELSE PO.TotalEstimatedConsumptionQuantity * UOM0.Factor END, 0) AS TotalEstimatedConsumptionQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.TotalEstimatedConsumptionQuantity
             ELSE PO.TotalEstimatedConsumptionQuantity * UOM1.Factor END, 0) AS TotalEstimatedConsumptionQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.TotalEstimatedConsumptionQuantity
             ELSE PO.TotalEstimatedConsumptionQuantity * UOM2.Factor END, 0) AS TotalEstimatedConsumptionQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.NetEstimatedConsumptionQuantity
             ELSE PO.NetEstimatedConsumptionQuantity * UOM0.Factor END, 0) AS NetEstimatedConsumptionQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.NetEstimatedConsumptionQuantity
             ELSE PO.NetEstimatedConsumptionQuantity * UOM1.Factor END, 0) AS NetEstimatedConsumptionQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.NetEstimatedConsumptionQuantity
             ELSE PO.NetEstimatedConsumptionQuantity * UOM2.Factor END, 0) AS NetEstimatedConsumptionQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.TotalScrapQuantity
             ELSE PO.TotalScrapQuantity * UOM0.Factor END, 0) AS TotalScrapQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.TotalScrapQuantity
             ELSE PO.TotalScrapQuantity * UOM1.Factor END, 0) AS TotalScrapQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.TotalScrapQuantity
             ELSE PO.TotalScrapQuantity * UOM2.Factor END, 0) AS TotalScrapQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.NetScrapQuantity
             ELSE PO.NetScrapQuantity * UOM0.Factor END, 0) AS NetScrapQuantity_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.NetScrapQuantity
             ELSE PO.NetScrapQuantity * UOM1.Factor END, 0) AS NetScrapQuantity_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.NetScrapQuantity
             ELSE PO.NetScrapQuantity * UOM2.Factor END, 0) AS NetScrapQuantity_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.ReportRemainderAsFinished
             ELSE PO.ReportRemainderAsFinished * UOM0.Factor END, 0) AS ReportRemainderAsFinished_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.ReportRemainderAsFinished
             ELSE PO.ReportRemainderAsFinished * UOM1.Factor END, 0) AS ReportRemainderAsFinished_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.ReportRemainderAsFinished
             ELSE PO.ReportRemainderAsFinished * UOM2.Factor END, 0) AS ReportRemainderAsFinished_SalesUnit,

    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductInventoryUnit
             THEN PO.ReportRemainderAsFinishedDetail
             ELSE PO.ReportRemainderAsFinishedDetail * UOM0.Factor END, 0) AS ReportRemainderAsFinishedDetail_InventoryUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductPurchaseUnit
             THEN PO.ReportRemainderAsFinishedDetail
             ELSE PO.ReportRemainderAsFinishedDetail * UOM1.Factor END, 0) AS ReportRemainderAsFinishedDetail_PurchaseUnit,
    COALESCE(
        CASE WHEN PO.InventUnit = P.ProductSalesUnit
             THEN PO.ReportRemainderAsFinishedDetail
             ELSE PO.ReportRemainderAsFinishedDetail * UOM2.Factor END, 0) AS ReportRemainderAsFinishedDetail_SalesUnit,

    /* Cost calculations with currency conversion */
    PO.EstimatedCostAmountAC AS EstimatedCostAmountAC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.EstimatedCostAmountAC
             ELSE PO.EstimatedCostAmountAC * RC.ExchangeRate END,0) AS EstimatedCostAmountRC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.EstimatedCostAmountAC
             ELSE PO.EstimatedCostAmountAC * GC.ExchangeRate END,0) AS EstimatedCostAmountGC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.EstimatedCostAmountAC
             ELSE PO.EstimatedCostAmountAC * RC_Budget.ExchangeRate END,0) AS EstimatedCostAmountRC_Budget,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.EstimatedCostAmountAC
             ELSE PO.EstimatedCostAmountAC * GC_Budget.ExchangeRate END,0) AS EstimatedCostAmountGC_Budget,

    PO.RealCostAmountAC AS RealCostAmountAC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.RealCostAmountAC
             ELSE PO.RealCostAmountAC * RC.ExchangeRate END,0) AS RealCostAmountRC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.RealCostAmountAC
             ELSE PO.RealCostAmountAC * GC.ExchangeRate END,0) AS RealCostAmountGC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.RealCostAmountAC
             ELSE PO.RealCostAmountAC * RC_Budget.ExchangeRate END,0) AS RealCostAmountRC_Budget,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.RealCostAmountAC
             ELSE PO.RealCostAmountAC * GC_Budget.ExchangeRate END,0) AS RealCostAmountGC_Budget,

    PO.TotalScrapAmountAC AS TotalScrapAmountAC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.TotalScrapAmountAC
             ELSE PO.TotalScrapAmountAC * RC.ExchangeRate END,0) AS TotalScrapAmountRC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.TotalScrapAmountAC
             ELSE PO.TotalScrapAmountAC * GC.ExchangeRate END,0) AS TotalScrapAmountGC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.TotalScrapAmountAC
             ELSE PO.TotalScrapAmountAC * RC_Budget.ExchangeRate END,0) AS TotalScrapAmountRC_Budget,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.TotalScrapAmountAC
             ELSE PO.TotalScrapAmountAC * GC_Budget.ExchangeRate END,0) AS TotalScrapAmountGC_Budget,

    PO.NetScrapAmountAC AS NetScrapAmountAC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.NetScrapAmountAC
             ELSE PO.NetScrapAmountAC * RC.ExchangeRate END,0) AS NetScrapAmountRC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.NetScrapAmountAC
             ELSE PO.NetScrapAmountAC * GC.ExchangeRate END,0) AS NetScrapAmountGC,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.ReportingCurrency
             THEN PO.NetScrapAmountAC
             ELSE PO.NetScrapAmountAC * RC_Budget.ExchangeRate END,0) AS NetScrapAmountRC_Budget,
    COALESCE(
        CASE WHEN L.AccountingCurrency = L.GroupCurrency
             THEN PO.NetScrapAmountAC
             ELSE PO.NetScrapAmountAC * GC_Budget.ExchangeRate END,0) AS NetScrapAmountGC_Budget

FROM `{catalog}`.`{schema}`.`ProductionOrder` PO

LEFT JOIN `{catalog}`.`{schema}`.`Product` P
   ON P.CompanyCode = PO.CompanyCode
   AND P.ProductCode = PO.ProductCode

LEFT JOIN `{catalog}`.`{schema}`.`UnitOfMeasure` UOM0
   ON PO.ProductCode = UOM0.ItemNumber
   AND PO.CompanyCode = UOM0.CompanyCode
   AND UOM0.FromUOM = PO.InventUnit
   AND UOM0.ToUOM   = P.ProductInventoryUnit

LEFT JOIN `{catalog}`.`{schema}`.`UnitOfMeasure` UOM1
   ON PO.ProductCode = UOM1.ItemNumber
   AND PO.CompanyCode = UOM1.CompanyCode
   AND UOM1.FromUOM = PO.InventUnit
   AND UOM1.ToUOM   = P.ProductPurchaseUnit

LEFT JOIN `{catalog}`.`{schema}`.`UnitOfMeasure` UOM2
   ON PO.ProductCode = UOM2.ItemNumber
   AND PO.CompanyCode = UOM2.CompanyCode
   AND UOM2.FromUOM = PO.InventUnit
   AND UOM2.ToUOM   = P.ProductSalesUnit

LEFT JOIN (
    SELECT DISTINCT
        LES.AccountingCurrency,
        LES.ReportingCurrency,
        LES.`Name`,
        LES.ExchangeRateType,
        LES.BudgetExchangeRateType,
        G.GroupCurrencyCode AS GroupCurrency
    FROM `{catalog}`.`{schema}`.`SMRBILedgerStaging` LES
    CROSS JOIN (
        SELECT GroupCurrencyCode
        FROM `{catalog}`.`{schema}`.`GroupCurrency`
        LIMIT 1
    ) G
) L
   ON PO.CompanyCode = L.`Name`

LEFT JOIN `{catalog}`.`{schema}`.`ExchangeRate` RC
   ON RC.FromCurrencyCode = L.AccountingCurrency
   AND RC.ToCurrencyCode   = L.ReportingCurrency
   AND RC.ExchangeRateTypeCode = L.ExchangeRateType
   AND PO.JournalPostingDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN `{catalog}`.`{schema}`.`ExchangeRate` GC
   ON GC.FromCurrencyCode = L.AccountingCurrency
   AND GC.ToCurrencyCode   = L.GroupCurrency
   AND GC.ExchangeRateTypeCode = L.ExchangeRateType
   AND PO.JournalPostingDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN `{catalog}`.`{schema}`.`ExchangeRate` RC_Budget
   ON RC_Budget.FromCurrencyCode = L.AccountingCurrency
   AND RC_Budget.ToCurrencyCode   = L.ReportingCurrency
   AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
   AND PO.JournalPostingDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN `{catalog}`.`{schema}`.`ExchangeRate` GC_Budget
   ON GC_Budget.FromCurrencyCode = L.AccountingCurrency
   AND GC_Budget.ToCurrencyCode   = L.GroupCurrency
   AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
   AND PO.JournalPostingDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

WHERE 1 = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
