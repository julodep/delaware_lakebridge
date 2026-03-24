/****** Object:  View [DataStore3].[V_ProductCostBreakdownQuantityRatioTMP]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore3`.`V_ProductCostBreakdownQuantityRatioTMP` AS

--Step 4: Calculate the QtyRatio per PriceCalcId
;
SELECT DISTINCT BCT.DataAreaId
		, PPCB.ItemNumber
		, BCT.PriceCalcId
		, BCT.ConsistOfPrice
		, QtyRatio = CAST((CAST(BCT.ConsumptionConstant AS DOUBLE) + CAST(BCT.ConsumptionVariable AS DOUBLE)) / CAST(COALESCE(NULLIF(BCT.Qty, 0), 1) AS DOUBLE) AS FLOAT)
		--Components:
		, BCT.ConsumptionConstant
		, BCT.ConsumptionVariable
		, BCT.Qty

FROM DataStore.ProductCostBreakdownTheoretical BCT
JOIN (SELECT DISTINCT CompanyCode
				, ItemNumber
				, InventDimCode
				, StartValidityDate
				, EndValidityDate
				, LowestLevelCalc
				, PriceCalcId
				, Levelling
				, LevellingNr
		FROM DataStore2.ProductCostBreakdownLevelling) PPCB
ON PPCB.CompanyCode = BCT.DataAreaId
	and PPCB.PriceCalcId = BCT.PriceCalcId
WHERE 1=1
	and NULLIF(ConsistOfPrice, '') is not NULL --Lowest level should not be taken into account for the calculation of the Required Quantity Ratio
;
