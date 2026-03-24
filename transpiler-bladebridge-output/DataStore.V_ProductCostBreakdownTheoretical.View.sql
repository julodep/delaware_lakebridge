/****** Object:  View [DataStore].[V_ProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_ProductCostBreakdownTheoretical` AS


SELECT DISTINCT BOM AS BOM
		, BOMCalcTransRecId AS BOMCalcTransRecId
		, CalcGroupId AS CalcGroupCode
		, CalcType AS CalcType
		, ConsistOfPrice AS ConsistOfPrice
		, ConsumptionConstant AS ConsumptionConstant   
		, ConsumptionVariable AS ConsumptionVariable   
		, ConsumpType AS ConsumpType   
		, CostCalculationMethod AS CostCalculationMethod   
		, CostGroupId AS CostGroupCode
		, CostMarkup AS CostMarkup   
		, CostMarkupQty AS CostMarkupQty   
		, CostPrice AS CostPrice   
		, CostPriceModelUsed AS CostPriceModelUsed   
		, CostPriceQty AS CostPriceQty   
		, CostPriceUnit AS CostPriceUnit   
		, DataAreaId AS DataAreaId   
		, InventDimId AS InventDimCode   
		, Level_ AS Level_   
		, LineNum AS LineNum   
		, NetWeightQty AS NetWeightQty   
		, NumOfSeries AS NumOfSeries   
		, OprId AS OprId   
		, OprNum AS OprNum   
		, OprNumNext AS OprNumNext   
		, OprPriority AS OprPriority   
		, ParentBOMCalcTrans AS ParentBOMCalcTrans   
		, PriceCalcId AS PriceCalcId   
		, Qty AS Qty   
		, Resource_ AS Resource_   
		, SalesMarkup AS SalesMarkup   
		, SalesMarkupQty AS SalesMarkupQty   
		, SalesPrice AS SalesPrice   
		, SalesPriceQty AS SalesPriceQty   
		, SalesPriceUnit AS SalesPriceUnit   
		, TransDate AS TransDate   
		, UnitId AS UnitCode   

FROM dbo.SMRBIBOMCalcTransStaging
;
