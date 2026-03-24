/****** Object:  View [DataStore4].[V_ProductCostBreakdownQuantityRatio]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore4`.`V_ProductCostBreakdownQuantityRatio` AS

--Step 5: Construct the layers for the REVERSE Quantity Ratio
;
SELECT DISTINCT T1.DataAreaId
		, T1.ItemNumber AS ItemNumber
		--CalculationNr is added to correctly filter on the breakdown level in the next step: it is possible that a PriceCalcId is used multiple times, we need to be able to correctly link the level to the TQR
		, CAST(COALESCE(SUBSTRING(SUBSTRING(T1.ConsistOfPrice,3,999),REGEXP_INSTR(SUBSTRING(T1.ConsistOfPrice,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T1.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T1.PriceCalcId,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T2.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T2.PriceCalcId,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T3.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T3.PriceCalcId,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T4.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T4.PriceCalcId,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T5.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T5.PriceCalcId,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(T6.PriceCalcId,3,999),REGEXP_INSTR(SUBSTRING(T6.PriceCalcId,3,999), '%`^0`%'),999), '') 
			AS DECIMAL(38,0)) AS CalculationNr
		
		, T1.ConsistOfPrice AS PriceCalcId
		, T1.PriceCalcId AS T1_PriceCalcId
		, T2.PriceCalcId AS T2_PriceCalcId
		, T3.PriceCalcId AS T3_PriceCalcId
		, T4.PriceCalcId AS T4_PriceCalcId
		, T5.PriceCalcId AS T5_PriceCalcId
		, T6.PriceCalcId AS T6_PriceCalcId

		, COALESCE(T1.QtyRatio, 1) AS QtyRatioP0
		, COALESCE(T2.QtyRatio, 1) AS QtyRatioP1
		, COALESCE(T3.QtyRatio, 1) AS QtyRatioP2
		, COALESCE(T4.QtyRatio, 1) AS QtyRatioP3
		, COALESCE(T5.QtyRatio, 1) AS QtyRatioP4
		, COALESCE(T6.QtyRatio, 1) AS QtyRatioP5
		, CAST(CAST(COALESCE(T1.QtyRatio, 1) AS DOUBLE) * CAST(COALESCE(T2.QtyRatio, 1) AS DOUBLE) * CAST(COALESCE(T3.QtyRatio, 1) AS DOUBLE) * CAST(COALESCE(T4.QtyRatio, 1) AS DOUBLE) * CAST(COALESCE(T5.QtyRatio, 1) AS DOUBLE) * CAST(COALESCE(T6.QtyRatio, 1) AS DOUBLE) AS DECIMAL(38,17)) TotalQtyRatio

FROM DataStore3.ProductCostBreakdownQuantityRatioTMP T1

LEFT JOIN DataStore3.ProductCostBreakdownQuantityRatioTMP T2
ON T1.DataAreaId = T2.DataAreaId
	and T1.ItemNumber = T2.ItemNumber
	and T1.PriceCalcId = T2.ConsistOfPrice

LEFT JOIN DataStore3.ProductCostBreakdownQuantityRatioTMP T3
ON T2.DataAreaId = T3.DataAreaId
	and T2.ItemNumber = T3.ItemNumber
	and T2.PriceCalcId = T3.ConsistOfPrice

LEFT JOIN DataStore3.ProductCostBreakdownQuantityRatioTMP T4
ON T3.DataAreaId = T4.DataAreaId
	and T3.ItemNumber = T4.ItemNumber
	and T3.PriceCalcId = T4.ConsistOfPrice

LEFT JOIN DataStore3.ProductCostBreakdownQuantityRatioTMP T5
ON T4.DataAreaId = T5.DataAreaId
	and T4.ItemNumber = T5.ItemNumber
	and T4.PriceCalcId = T5.ConsistOfPrice

LEFT JOIN DataStore3.ProductCostBreakdownQuantityRatioTMP T6
ON T5.DataAreaId = T6.DataAreaId
	and T5.ItemNumber = T6.ItemNumber
	and T5.PriceCalcId = T6.ConsistOfPrice

WHERE 1=1
;
