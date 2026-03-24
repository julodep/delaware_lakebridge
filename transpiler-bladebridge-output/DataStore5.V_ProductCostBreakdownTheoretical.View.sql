/****** Object:  View [DataStore5].[V_ProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore5`.`V_ProductCostBreakdownTheoretical` AS


SELECT * 
FROM (

SELECT	  COALESCE(BCT.BOMCalcTransRecId, -1) AS BOMCalcTransRecId
		, COALESCE(CP.CompanyCode, '_N/A') AS CompanyCode 
		, COALESCE(NULLIF(UPPER(CP.ItemNumber), ''), '_N/A') AS FinishedProductCode 
		, COALESCE(UPPER(CP2.ItemNumber), '_N/A') AS ProductCode 
		, COALESCE(CP.InventDimCode, '_N/A') AS InventDimCode
		, COALESCE(SM.Name, '_N/A') AS CalculationType 
		, COALESCE(CP.PriceType, '_N/A') AS PriceType 
		, COALESCE(NULLIF(CP.PriceCalcId, ''), '_N/A') AS CalculationCode 
		, CAST(CASE WHEN COALESCE(NULLIF(CP.PriceCalcId, ''), '_N/A') = '_N/A' 
					THEN 'No' ELSE 'Yes' 
			   END AS STRING) AS IsCalculatedPrice 
		, CAST(COALESCE(CP.IsMaxCalculation, '_N/A') AS STRING) AS IsMaxCalculation 
		, CAST(COALESCE(CP.IsMaxPrice, '_N/A') AS STRING) AS IsMaxPrice 
		, CAST(COALESCE(PPCB.Levelling, '_N/A') AS STRING) AS Levelling 
		, COALESCE(NULLIF(BCT.Resource_, ''), '_N/A') AS Resource_ 
		, COALESCE(NULLIF(BCT.CostGroupCode, ''), 'N/A') AS CostGroupCode 
		, COALESCE(CP.UnitCode, '_N/A') AS CostPriceUnitSymbol 
		, COALESCE(NULLIF(CP.CalculationNr, ''), '_N/A') AS CostPriceCalculationNumber
		, CAST('Actual' AS STRING) AS CostPriceType 
		, COALESCE(NULLIF(CP.VersionCode, ''), '_N/A') AS CostPriceVersion 
		, CAST(COALESCE(CASE WHEN BCT.CostPriceModelUsed = 0 THEN 'None'
WHEN BCT.CostPriceModelUsed = 1 THEN 'Item Cost Price'
WHEN BCT.CostPriceModelUsed = 2 THEN 'Item Purchase Price'
WHEN BCT.CostPriceModelUsed = 3 THEN 'Trade Agreement'
WHEN BCT.CostPriceModelUsed = 4 THEN 'Inventory Price'
END, '_N/A') AS STRING)  AS CostPriceModel 
	--Date dimensions:
		, COALESCE(NULLIF(BCT.TransDate, ''), '1900-01-01') AS CalculationDate 
		, CAST(COALESCE(CP.IsActivePrice, '_N/A') AS STRING) AS IsActivePrice 
	--Measures:
		, COALESCE(CP.Price, 0) AS ProductCostPrice 
		, CAST(COALESCE(BCT.CostPriceQty, 0) AS decimal(20,10)) * CAST(COALESCE(TQR.TotalQtyRatio, 1) AS DECIMAL(17,7)) AS ComponentCostPrice --Cover for NULL values in Level 0
	--Components:
		, BCT.CostPriceQty AS CostPriceQty 
		, TQR.TotalQtyRatio AS TotalQtyRatio 
		, CASE WHEN BCT.ConsistOfPrice != '' and BCT.CalcType = 3 
			   THEN 'Exclude' 
			   ELSE '' 
		  END AS ExcludedItems

FROM DataStore.ProductCostBreakdownPrice CP

INNER JOIN (SELECT DISTINCT CompanyCode
					, ItemNumber
					, InventDimCode
					, StartValidityDate
					, EndValidityDate
					, LowestLevelCalc
					, PriceCalcId
					, Levelling
					, LevellingNr
			FROM DataStore2.ProductCostBreakdownLevelling) PPCB
ON CP.CompanyCode = PPCB.CompanyCode
	and CP.InventDimCode = PPCB.InventDimCode
	and CP.ItemNumber = PPCB.ItemNumber
	and CP.PriceCalcId = PPCB.LowestLevelCalc

LEFT JOIN LATERAL (SELECT *
				FROM DataStore.ProductCostBreakdownPrice CP2
				WHERE 1=1
					and CP2.CompanyCode = PPCB.CompanyCode
					and CP2.PriceCalcId = PPCB.PriceCalcId
			) CP2

LEFT JOIN DataStore.ProductCostBreakdownTheoretical BCT
ON BCT.DataAreaId = PPCB.CompanyCode
	and BCT.PriceCalcId = PPCB.PriceCalcId --No further filtering allowed (e.g. on BOMCalcTransRecId), as this join expands the used PCIDs for the different components

LEFT JOIN DataStore2.ProductCostBreakdownLevelling PPCB2
ON PPCB.CompanyCode = PPCB2.CompanyCode
	and PPCB.ItemNumber = PPCB2.ItemNumber
	and PPCB.LowestLevelCalc = PPCB2.LowestLevelCalc
	and PPCB.PriceCalcId = PPCB2.PriceCalcId
	and PPCB.LevellingNr = PPCB2.LevellingNr
	and PPCB.InventDimCode = PPCB2.InventDimCode

LEFT JOIN DataStore4.ProductCostBreakdownQuantityRatio TQR
ON TQR.DataAreaId = PPCB.CompanyCode
	and TQR.PriceCalcId = PPCB.PriceCalcId
	and TQR.ItemNumber = PPCB.ItemNumber
	and TQR.CalculationNr = PPCB2.CalculationNr

LEFT JOIN ETL.StringMap SM
ON SM.Enum = BCT.Calctype
	and SM.SourceTable = 'SMRBIBOMCalcTransStaging'
	and SM.SourceColumn = 'CalcType'

WHERE 1=1
	and NULLIF(CP.PriceCalcId, '') is not NULL --Take only calculated prices
	and BCT.BOM = 0 --Exclude BOM Consumption lines as they will lead to duplication
	and NULLIF(BCT.ParentBOMCalcTrans, '') is NULL --Exclude second-level values in the upper layers
	--and NULLIF(BCT.ConsistOfPrice, '') is NULL --Breakdown of items results in items already being broken down to be removed

) PCB

WHERE 1=1
	and ExcludedItems != 'Exclude'

--Also take into account the non-calculated prices

UNION ALL

SELECT	  ROW_NUMBER() OVER (PARTITION BY CP.CompanyCode,CP.ItemNumber,CP.UnitCode,CP.InventDimCode, CP.Price, CP.StartValidityDate, CP.EndValidityDate ORDER BY CP.CompanyCode) AS BOMCalcTransRecId 
		, COALESCE(CP.CompanyCode, '_N/A') AS CompanyCode
		, COALESCE(UPPER(CP.ItemNumber), '_N/A') AS FinishedProductCode  
		, COALESCE(UPPER(CP.ItemNumber), '_N/A') AS ProductCode  
		, COALESCE(CP.InventDimCode, '_N/A') AS InventDimCode  
		, CAST('_N/A' AS STRING) AS CalculationType  
		, COALESCE(CP.PriceType, '_N/A') AS PriceType  
		, COALESCE(NULLIF(CP.PriceCalcId, ''), '_N/A') AS CalculationCode  
		, CAST(CASE WHEN COALESCE(NULLIF(CP.PriceCalcId, ''), '_N/A') = '_N/A' 
				    THEN 'No' 
					ELSE 'Yes' 
			   END AS STRING) AS IsCalculatedPrice  
		, CAST(COALESCE(CP.IsMaxCalculation, '_N/A') AS STRING) AS IsMaxCalculation  
		, CAST(COALESCE(CP.IsMaxPrice, '_N/A') AS STRING) AS IsMaxPrice  
		, CAST('Level 0' AS STRING) AS Levelling  
		, CAST('_N/A' AS STRING) AS Resource_  
		, CAST('_N/A' AS STRING) AS CostGroupCode  
		, COALESCE(CP.UnitCode, '_N/A') AS CostPriceUnitSymbol  
		, COALESCE(NULLIF(CP.CalculationNr, ''), '_N/A') AS CostPriceCalculationNumber  
		, CAST('Actual' AS STRING) AS CostPriceType  
		, COALESCE(NULLIF(CP.VersionCode, ''), '_N/A') AS CostPriceVersion  
		, CAST('_N/A' AS STRING) AS CostPriceModel  
	--Date dimensions:
		, COALESCE(CP.StartValidityDate, '1900-01-01') AS CalculationDate  
		, CAST(COALESCE(CP.IsActivePrice, '_N/A') AS STRING) AS IsActivePrice  
	--Measures:
		, CP.Price AS ProductCostPrice  
		, CAST(CP.Price AS DECIMAL(38,17))  AS ComponentCostPrice  
	--Components:
		, CAST(0 AS DECIMAL(38,17)) AS CostPriceQty  
		, CAST(0 AS DECIMAL(38,17)) AS TotalQtyRatio  
		, '' AS ExcludedItems  

FROM DataStore.ProductCostBreakdownPrice CP

WHERE 1=1
	and NULLIF(CP.PriceCalcId, '') IS NULL --Take only NON-CALCULATED PRICES
;
