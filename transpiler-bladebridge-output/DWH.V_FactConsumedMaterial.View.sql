/****** Object:  View [DWH].[V_FactConsumedMaterial]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DWH`.`V_FactConsumedMaterial` AS


SELECT    UPPER(PO.CompanyCode) AS CompanyCode
		, UPPER(PO.InventDimCode) AS InventDimCode
		, UPPER(PO.ProductionResourceCode) AS ProductCode
		, ETL.fn_MonthKeyInt(PO.ProductionStartDate) AS DimMonthId
		, SUM(PO.RealConsumptionQuantity_InventoryUnit) AS RealConsumptionQuantity_InventoryUnit
		, SUM(PO.RealCostAmountGC) AS RealCostAmountGC
		, SUM(PO.RealCostAmountAC) AS RealCostAmountAC

FROM DataStore4.ProductionOrder PO

--Filter machines/... out
INNER JOIN DataStore.Product P
ON PO.ProductionResourceCode = P.ProductCode
AND PO.CompanyCode = P.CompanyCode

WHERE P.ProductCode != '_N/A'

GROUP BY PO.ResourceCode
, PO.InventDimCode
, PO.CompanyCode
, PO.ProductionResourceCode
, ETL.fn_MonthKeyInt(PO.ProductionStartDate)
;
