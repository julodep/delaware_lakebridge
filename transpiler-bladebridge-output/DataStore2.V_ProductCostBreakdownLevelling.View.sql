/****** Object:  View [DataStore2].[V_ProductCostBreakdownLevelling]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore2`.`V_ProductCostBreakdownLevelling` AS

--Step 1: Retrieve only distinct price calculations and their corresponding linked calculations
;
WITH TEMP1 AS (

	SELECT DISTINCT DataAreaId = BCT.DataAreaId
			, PriceCalcId = BCT.PriceCalcId
			, ConsistOfPrice = BCT.ConsistOfPrice
	FROM dbo.SMRBIBOMCalcTransStaging BCT
	
),

--Step 2: Create waterfall for prices (similar to BOM explosion)
TEMP2 AS (

	--CalculationId is added since a PriceCalcId can be used in multiple locations in the BOM, each resulting in a different TQR (see next DataStore). Therefore, we need to be able to distinct the different layers
	SELECT  CalculationNr = CAST(COALESCE(SUBSTRING(SUBSTRING(P5,3,999),REGEXP_INSTR(SUBSTRING(P5,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(P4,3,999),REGEXP_INSTR(SUBSTRING(P4,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(P3,3,999),REGEXP_INSTR(SUBSTRING(P3,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(P2,3,999),REGEXP_INSTR(SUBSTRING(P2,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(P1,3,999),REGEXP_INSTR(SUBSTRING(P1,3,999), '%`^0`%'),999), '')
								+ COALESCE(SUBSTRING(SUBSTRING(P0,3,999),REGEXP_INSTR(SUBSTRING(P0,3,999), '%`^0`%'),999), '') AS DECIMAL(38,0))
			, *
	
	FROM (

		SELECT DISTINCT CP.ItemNumber
				, CP.CompanyCode
				, CP.InventDimCode
				, CP.VersionCode
				, CP.StartValidityDate
				, CP.EndValidityDate
				, T1.PriceCalcId AS P0
				, 'Level 0' AS P0L
				, 0 AS P0LT
				, T2.PriceCalcId AS P1
				, 'Level 1' AS P1L
				, 1 AS P1LT
				, T3.PriceCalcId AS P2
				, 'Level 2' AS P2L
				, 2 AS P2LT
				, T4.PriceCalcId AS P3
				, 'Level 3' AS P3L
				, 3 AS P3LT
				, T5.PriceCalcId AS P4
				, 'Level 4' AS P4L
				, 4 AS P4LT
				, T6.PriceCalcId AS P5
				, 'Level 5' AS P5L
				, 5 AS P5LT
		FROM TEMP1 T1
		LEFT JOIN DataStore.ProductCostBreakdownPrice CP
		ON T1.DataAreaId = CP.CompanyCode
			and CP.PriceCalcId = T1.PriceCalcId
		LEFT JOIN DataStore.Product P
		ON CP.CompanyCode = P.CompanyCode
			and CP.ItemNumber = P.ProductCode
		LEFT JOIN TEMP1 T2
		ON NULLIF(T1.PriceCalcId, '') is not NULL
			and T1.ConsistOfPrice = T2.PriceCalcId
			and T1.DataAreaId = T2.DataAreaId
		LEFT JOIN TEMP1 T3
		ON NULLIF(T2.PriceCalcId, '') is not NULL
			and T2.ConsistOfPrice = T3.PriceCalcId
			and T2.DataAreaId = T3.DataAreaId
		LEFT JOIN TEMP1 T4
		ON NULLIF(T3.PriceCalcId, '') is not NULL
			and T3.ConsistOfPrice = T4.PriceCalcId
			and T3.DataAreaId = T4.DataAreaId
		LEFT JOIN TEMP1 T5
		ON NULLIF(T4.PriceCalcId, '') is not NULL
			and T4.ConsistOfPrice = T5.PriceCalcId
			and T4.DataAreaId = T5.DataAreaId
		LEFT JOIN TEMP1 T6
		ON NULLIF(T5.PriceCalcId, '') is not NULL
			and T5.ConsistOfPrice = T6.PriceCalcId
			and T5.DataAreaId = T6.DataAreaId

		WHERE 1=1
			--and P.ProductType IN ('Component', 'Semi-Finished', 'Finished') --Filter on Finished Products only is removed to also incorporate the semi-finished and component product cost price breakdown
			--and P. IN ('Finished') --Filter on Finished Products only is removed to also incorporate the semi-finished and component product cost price breakdown

			--and ItemNumber = '508249'
			--and T1.PriceCalcId = 'CN0010459'
			
		) A

)

--Step 3: Pivot data to enable easy querying in the SELECT

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P0
		, Levelling = T.P0L
		, LevellingNr = T.P0LT
FROM TEMP2 T
WHERE 1=1
	and T.P1 IS NULL

UNION ALL

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P1
		, Levelling = T.P1L
		, LevellingNr = T.P1LT
FROM TEMP2 T
WHERE 1=1
	and T.P1 IS NOT NULL
	and T.P2 IS NULL

UNION ALL

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P2
		, Levelling = T.P2L
		, LevellingNr = T.P2LT
FROM TEMP2 T
WHERE 1=1
	and T.P2 IS NOT NULL
	and T.P3 IS NULL

UNION ALL

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P3
		, Levelling = T.P3L
		, LevellingNr = T.P3LT
FROM TEMP2 T
WHERE 1=1
	and T.P3 IS NOT NULL
	and T.P4 IS NULL

UNION ALL

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P4
		, Levelling = T.P4L
		, LevellingNr = T.P4LT
FROM TEMP2 T
WHERE 1=1
	and T.P4 IS NOT NULL
	and T.P5 IS NULL

UNION ALL

SELECT	DISTINCT T.CompanyCode
		, T.ItemNumber
		, T.InventDimCode
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr
		, LowestLevelCalc = T.P0
		, PriceCalcId = T.P5
		, Levelling = T.P5L
		, LevellingNr = T.P5LT
FROM TEMP2 T
WHERE 1=1
	and T.P5 IS NOT NULL

/*

SELECT DISTINCT T.CompanyId
		, T.ItemNumber
		, T.InventDimId
		, T.StartValidityDate
		, T.EndValidityDate
		, T.CalculationNr --Additional field
		, LowestLevelCalc = T.P0
		, CA.PriceCalcId
		, CA.Levelling
		, CA.LevellingNr

FROM TEMP2 T
OUTER APPLY (VALUES (P0, P0L, P0LT),
					(P1, P1L, P1LT),
					(P2, P2L, P2LT),
					(P3, P3L, P3LT),
					(P4, P4L, P4LT),
					(P5, P5L, P5LT)) 
			CA (PriceCalcId, Levelling, LevellingNr)

WHERE 1=1
	and CA.PriceCalcId is not NULL

*/
;
