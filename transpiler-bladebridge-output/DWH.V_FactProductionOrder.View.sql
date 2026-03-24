/****** Object:  View [DWH].[V_FactProductionOrder]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DWH`.`V_FactProductionOrder` AS 


SELECT	  UPPER(ProductionOrderCode) AS ProductionOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(RouteCode) AS RouteCode
		, UPPER(ResourceCode) AS ResourceCode
		, UPPER(ProductionResourceCode) AS ProductionResourceCode
		, UPPER(InventDimCode) AS InventDimCode
		, UPPER(BOMCode) AS BOMCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(SiteCode) AS SiteCode
		, UPPER(OperationCode) AS OperationCode
		, OperationNumber

		, ETL.fn_DateKeyInt(JournalPostingDate) AS DimJournalPostingDateId
		, RequestedDeliveryDate
		, ScheduledDeliveryDate
		, ActualDeliveryDate
		, ETL.fn_DateKeyInt(ScheduledProductionStartDate) AS DimScheduledProductionStartDateId
		, ETL.fn_DateKeyInt(ScheduledProductionEndDate) AS DimScheduledProductionEndDateId
		, ETL.fn_DateKeyInt(ProductionStartDate) AS DimProductionStartDateId
		, ETL.fn_DateKeyInt(ProductionEndDate) AS DimProductionEndDateId


		, CalculationType
		, ProductionOrderStatus
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget
		, OTIF
		, InventUnit

		, ProducedQuantity_InventoryUnit
		, ProducedQuantity_PurchaseUnit
		, ProducedQuantity_SalesUnit
	
		, EstimatedQuantity_InventoryUnit
		, EstimatedQuantity_PurchaseUnit
		, EstimatedQuantity_SalesUnit

		, RealConsumptionQuantity_InventoryUnit
		, RealConsumptionQuantity_PurchaseUnit
		, RealConsumptionQuantity_SalesUnit

		, TotalScrapQuantity_InventoryUnit
		, TotalScrapQuantity_PurchaseUnit
		, TotalScrapQuantity_SalesUnit

		, NetScrapQuantity_InventoryUnit
		, NetScrapQuantity_PurchaseUnit
		, NetScrapQuantity_SalesUnit

		, EstimatedQuantityDetail_InventoryUnit
		, EstimatedQuantityDetail_PurchaseUnit
		, EstimatedQuantityDetail_SalesUnit

		, NetEstimatedConsumptionQuantity_InventoryUnit
		, NetEstimatedConsumptionQuantity_PurchaseUnit
		, NetEstimatedConsumptionQuantity_SalesUnit

		, OriginalEstimatedQuantity_InventoryUnit
		, OriginalEstimatedQuantity_PurchaseUnit
		, OriginalEstimatedQuantity_SalesUnit

		, OriginalEstimatedQuantityDetail_InventoryUnit
		, OriginalEstimatedQuantityDetail_PurchaseUnit
		, OriginalEstimatedQuantityDetail_SalesUnit

		, ProducedQuantityDetail_InventoryUnit
		, ProducedQuantityDetail_PurchaseUnit
		, ProducedQuantityDetail_SalesUnit

		, ReportRemainderAsFinished_InventoryUnit
		, ReportRemainderAsFinished_PurchaseUnit
		, ReportRemainderAsFinished_SalesUnit

		, ReportRemainderAsFinishedDetail_InventoryUnit
		, ReportRemainderAsFinishedDetail_PurchaseUnit
		, ReportRemainderAsFinishedDetail_SalesUnit

		, TotalEstimatedConsumptionQuantity_InventoryUnit
		, TotalEstimatedConsumptionQuantity_PurchaseUnit
		, TotalEstimatedConsumptionQuantity_SalesUnit

		, EstimatedCostAmountAC
		, EstimatedCostAmountRC
		, EstimatedCostAmountGC

		, EstimatedCostAmountRC_Budget
		, EstimatedCostAmountGC_Budget

		, RealCostAmountAC
		, RealCostAmountRC
		, RealCostAmountGC

		, RealCostAmountRC_Budget
		, RealCostAmountGC_Budget

		, TotalScrapAmountAC
		, TotalScrapAmountRC
		, TotalScrapAmountGC

		, TotalScrapAmountRC_Budget
		, TotalScrapAmountGC_Budget

		, NetScrapAmountAC
		, NetScrapAmountRC
		, NetScrapAmountGC

		, NetScrapAmountRC_Budget
		, NetScrapAmountGC_Budget

FROM DataStore4.ProductionOrder
;
