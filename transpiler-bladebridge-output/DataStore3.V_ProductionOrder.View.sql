/****** Object:  View [DataStore3].[V_ProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DataStore3`.`V_ProductionOrder` AS


SELECT 
	--Dimensions:	
		  PO.ProductionOrderCode
		, PO.ProductionOrderStatus
		, PO.CompanyCode
		, PO.ProductCode
		, PO.RouteCode
		, PO.SiteCode
		, PO.OperationCode
		, PO.OperationNumber
		, PO.ResourceCode
		, PO.CalculationType
		, PO.ProductionResourceCode

	--Technical Fields:
		, PO.CostGroupCode
		, PO.InventDimCode
		, PO.BOMCode
		, PO.TransactionType

	--Dates:
		, PO.JournalPostingDate
		, PO.RequestedDeliveryDate
		, PO.ScheduledDeliveryDate
		, PO.ActualDeliveryDate
		, PO.ScheduledProductionStartDate
		, PO.ScheduledProductionEndDate
		, PO.ProductionStartDate
		, PO.ProductionEndDate

	--Measures: OTIF
		, PO.OTIF

	--Measures: Volume	
		, PO.InventUnit
		, PO.OriginalEstimatedQuantity
		, PO.OriginalEstimatedQuantityDetail
		, PO.EstimatedQuantity
		, PO.EstimatedQuantityDetail
		, PO.ProducedQuantity
		, PO.ProducedQuantityDetail
		, PO.ReportRemainderAsFinished
		, PO.ReportRemainderAsFinishedDetail

	--Measures: €	 
		, PO.NetEstimatedConsumptionQuantity --Excl. scrap
		, PO.TotalEstimatedConsumptionQuantity
		, PO.RealConsumptionQuantity
		, COALESCE(CP1.Price, CP2.Price) AS Price_UsedBOMLines
		, PO.TotalEstimatedCostAmountAC
		, COALESCE(PO.NetEstimatedConsumptionQuantity * COALESCE(CP1.Price, CP2.Price), PO.TotalEstimatedCostAmountAC) AS EstimatedCostAmountAC
		, PO.RealCostAmountAC AS RealCostAmountAC
		
		, PO.RealConsumptionQuantity - PO.NetEstimatedConsumptionQuantity AS TotalScrapQuantity --Actual Used Quantity - Net Estimated Quantity (Inventory Unit)
		, PO.ConstantScrapQuantity
		, PO.VariableScrapQuantity
		, (PO.RealConsumptionQuantity - PO.NetEstimatedConsumptionQuantity) --Total Scrap Quantity  (Inventory Unit)
		  - (PO.ConstantScrapQuantity + PO.VariableScrapQuantity) AS NetScrapQuantity --Constant/Variable Scrap Quantity  (Inventory Unit)
		, (PO.RealConsumptionQuantity - PO.NetEstimatedConsumptionQuantity) * COALESCE(CP1.Price, CP2.Price, 0) AS TotalScrapAmountAC
		, ((PO.RealConsumptionQuantity - PO.NetEstimatedConsumptionQuantity)
		  - (PO.ConstantScrapQuantity + PO.VariableScrapQuantity)) * COALESCE(CP1.Price, CP2.Price, 0) AS NetScrapAmountAC

FROM DataStore2.ProductionOrder PO

--Step 1: Join on Product Configuration to retrieve the correct items configurations for the cost prices (see next join)
LEFT JOIN DataStore.ProductConfiguration PC1
ON PO.CompanyCode = PC1.CompanyCode
	and PO.InventDimCode_UsedBOMLines = PC1.InventDimCode

--Step 2a: Join directly on DataStore.CostPrice if a cost price exists for the inventory unit of the BOM lines
LEFT JOIN 
	(SELECT	CP.ItemNumber
			, CP.UnitCode
			, CP.CompanyCode
			, CP.Price
			, CP.StartValidityDate
			, CP.EndValidityDate
			--Configuration:
			, PC.ProductConfigurationCode
			, PC.SiteCode
		FROM DataStore.CostPrice CP
		LEFT JOIN DataStore.ProductConfiguration PC
		ON CP.CompanyCode = PC.CompanyCode
			and CP.InventDimCode = PC.InventDimCode) CP1
ON PO.CompanyCode = CP1.CompanyCode
	and PO.ProductionStartDate BETWEEN CP1.StartValidityDate and CP1.EndValidityDate
	and PO.ProductCode_UsedBOMLines = CP1.ItemNumber
	and PO.InventUnit_UsedBOMLines = CP1.UnitCode
	--Configuration (Depending on the level on which prices are calculated in AX)
	and PC1.ProductConfigurationCode = CP1.ProductConfigurationCode
	and PC1.SiteCode = CP1.SiteCode

--Step 2b: In case no cost price exists for the inventory unit, convert the cost price unit to the applicable inventory units
LEFT JOIN 
	(SELECT	CP.CompanyCode
			, CP.ItemNumber
			, CP.InventDimCode
			, CP.StartValidityDate
			, CP.EndValidityDate
			, CostPrice = CP.Price
			, CostPriceUnit = CP.UnitCode
			, Separator = '***'
			, Price = CP.Price / UOM1.Factor
			, UOM1.Factor
			, UOM1.FromUOM
			, ConversionUnit = UOM1.ToUOM
			--Configuration:
			, PC.ProductConfigurationCode
			, PC.SiteCode
		FROM DataStore.CostPrice CP
		LEFT JOIN DataStore.ProductConfiguration PC
		ON CP.CompanyCode = PC.CompanyCode
			and CP.InventDimCode = PC.InventDimCode
		LEFT JOIN DataStore.UnitOfMeasure UOM1
		ON CP.ItemNumber = UOM1.ItemNumber
			and CP.CompanyCode = UOM1.CompanyCode
			and CP.UnitCode = UOM1.FromUOM) CP2
ON PO.CompanyCode = CP2.CompanyCode
	and PO.ProductionStartDate BETWEEN CP2.StartValidityDate and CP2.EndValidityDate
	and PO.ProductCode_UsedBOMLines = CP2.ItemNumber
	and PO.InventUnit_UsedBOMLines = CP2.ConversionUnit
	--Configuration (Depending on the level on which prices are calculated in AX)
	and PC1.ProductConfigurationCode = CP2.ProductConfigurationCode
	and PC1.SiteCode = CP2.SiteCode

WHERE 1=1
;
