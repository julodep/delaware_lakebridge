/****** Object:  View [DataStore].[V_ProductCostBreakdownCalculations]    Script Date: 03/03/2026 16:26:08 ******/




CREATE OR REPLACE VIEW `DataStore`.`V_ProductCostBreakdownCalculations` AS

--Step 1: Retrieve only distinct price calculations and their corresponding linked calculations
;
WITH TEMP0 AS (

	SELECT DISTINCT BCT1.DataAreaId
			, BCT1.PriceCalcId
			, BCT1.ConsistOfPrice
			, BOMCalcTransRecId = CASE WHEN NULLIF(BCT1.ConsistOfPrice, '') IS NULL THEN -1 ELSE BCT2.BOMCalcTransRecId END
	FROM 
		(SELECT DISTINCT DataAreaId, PriceCalcId, ConsistOfPrice
		FROM dbo.SMRBIBOMCalcTransStaging) BCT1
		
		JOIN (SELECT DISTINCT * FROM dbo.SMRBIBOMCalcTransStaging) BCT2
		ON BCT1.DataAreaId = BCT2.DataAreaId
			and BCT1.PriceCalcId = BCT2.PriceCalcId
			and BCT1.ConsistOfPrice = BCT2.ConsistOfPrice

),

TEMP1 AS (
	
	SELECT DISTINCT DataAreaId, PriceCalcId FROM TEMP0 WHERE BOMCalcTransRecId != -1

)

	SELECT T0.*
	FROM TEMP0 T0
	LEFT JOIN TEMP1 T1
	ON T0.DataAreaId = T1.DataAreaId
		and T0.PriceCalcId = T1.PriceCalcId
	WHERE 1=1
		and ((T0.BOMCalcTransRecId = -1 and T1.DataAreaId is NULL)
			OR T0.BOMCalcTransRecId != -1)
;
