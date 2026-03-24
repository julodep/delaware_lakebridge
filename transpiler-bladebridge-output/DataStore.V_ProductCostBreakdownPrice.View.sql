/****** Object:  View [DataStore].[V_ProductCostBreakdownPrice]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_ProductCostBreakdownPrice` AS 

/* This query is constructed for both PENDING prices and NORMAL prices */
;
WITH 

/*StartEndValidityDate_Pending AS (

SELECT	RankNr = ROW_NUMBER() OVER (PARTITION BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.UnitId COLLATE DATABASE_DEFAULT,IIP.InventDimId COLLATE DATABASE_DEFAULT, LEFT(IIP.PriceCalcId COLLATE DATABASE_DEFAULT, 2), IIP.VersionId ORDER BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT, IIP.CalcOrder ASC)
		, RankNrIsMaxPrice = ROW_NUMBER() OVER (PARTITION BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.UnitId COLLATE DATABASE_DEFAULT,IIP.InventDimId COLLATE DATABASE_DEFAULT, IIP.VersionId, LEFT(IIP.PriceCalcId COLLATE DATABASE_DEFAULT, 2) ORDER BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT, IIP.CalcOrder ASC)
		, ActivationDate = IIP.ActivationDate
		, ItemNumber = IIP.ItemId COLLATE DATABASE_DEFAULT
		, InventDimId = IIP.InventDimId COLLATE DATABASE_DEFAULT
		, CompanyId = IIP.DataAreaId COLLATE DATABASE_DEFAULT
		, PriceCalcId = IIP.PriceCalcId COLLATE DATABASE_DEFAULT
		, VersionId = IIP.VersionId COLLATE DATABASE_DEFAULT
		, Price = (CAST(IIP.Price AS numeric(15,8)) + CAST(IIP.Markup AS numeric(15,8))) 
					/ (CAST(CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END AS numeric(8,1))) --Divide by the number of items to obtain the SINGLE item price
		, UnitId = IIP.UnitId COLLATE DATABASE_DEFAULT
		, CalcOrder = IIP.CalcOrder
		, CreatedDateTime = '1900-01-01'

FROM 

	(SELECT	IIP2.ActivationDate
			, IIP2.ItemId
			, IIP2.InventDimId
			, IIP2.DataAreaId
			, IIP1.Price
			, IIP1.Markup
			, IIP1.PriceUnit
			, IIP1.PriceCalcId
			, IIP1.PriceQty
			, IIP2.UnitId
			, IIP1.VersionId
			, IIP2.CalcOrder
	
	FROM (SELECT DISTINCT * FROM dbo.SMRBIInventItemPriceSimStaging) IIP1

	JOIN (SELECT ActivationDate = FromDate
					, ItemId
					, InventDimId
					, DataAreaId
					, CostingType = 2 --Standard
					, PriceType
					, VersionId
					, PriceCalcId
					, UnitId
					, CalcOrder = SUBSTRING(REPLACE(PriceCalcId,'CN', ''), PATINDEX('%[^0]%', REPLACE(PriceCalcId,'CN', '')+'.'), LEN(REPLACE(PriceCalcId,'CN', '')))

			FROM (SELECT DISTINCT * FROM dbo.SMRBIInventItemPriceSimStaging) IIP
			WHERE 1=1
				--and CostingType = 2		--Filter on "Standard" Cost (Alternatives: 0 = Default | 1 = Planned | 2 = Standard | 3 = Actual)
				and PriceType = 0			--Filter on "Cost" Price type (Alternatives: 0 = Cost | 1 = Purchase | 2 = Sales )

			GROUP BY FromDate
					, ItemId
					, InventDimId
					, DataAreaId
					, PriceType
					, VersionId
					, PriceCalcId
					, UnitId
		 ) IIP2

	ON IIP1.FromDate = IIP2.ActivationDate
		and IIP1.ItemId = IIP2.ItemId
		and IIP1.InventDimId = IIP2.InventDimId
		and IIP1.DataAreaId = IIP2.DataAreaId
		and IIP1.UnitId = IIP2.UnitId
		--and IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime
		and IIP1.PriceType = IIP2.PriceType
		and IIP1.PriceCalcId = IIP2.PriceCalcId
		and IIP1.VersionId = IIP2.VersionId

	) IIP

	WHERE 1=1
),
*/

StartEndValidityDate_Active AS (

SELECT	RankNr = ROW_NUMBER() OVER (PARTITION BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.UnitId COLLATE DATABASE_DEFAULT,IIP.InventDimId COLLATE DATABASE_DEFAULT, IIP.VersionId COLLATE DATABASE_DEFAULT, LEFT(IIP.PriceCalcId COLLATE DATABASE_DEFAULT, 2) ORDER BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.CreatedDateTime ASC)
		, RankNrIsMaxPrice = ROW_NUMBER() OVER (PARTITION BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.UnitId COLLATE DATABASE_DEFAULT,IIP.InventDimId COLLATE DATABASE_DEFAULT, IIP.VersionId COLLATE DATABASE_DEFAULT ORDER BY IIP.DataAreaId COLLATE DATABASE_DEFAULT,IIP.ItemId COLLATE DATABASE_DEFAULT,IIP.CreatedDateTime ASC)
		, ActivationDate = IIP.ActivationDate
		, ItemNumber = IIP.ItemId COLLATE DATABASE_DEFAULT
		, InventDimId = IIP.InventDimId COLLATE DATABASE_DEFAULT
		, CompanyId = IIP.DataAreaId COLLATE DATABASE_DEFAULT
		, PriceCalcId = IIP.PriceCalcId COLLATE DATABASE_DEFAULT
		, VersionId = IIP.VersionId COLLATE DATABASE_DEFAULT
		, Price = (CAST(IIP.Price AS DECIMAL(15,8)) + CAST(IIP.Markup AS DECIMAL(15,8))) 
					/ (CAST(CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END AS DECIMAL(8,1))) --Divide by the number of items to obtain the SINGLE item price
		, UnitId = IIP.UnitId COLLATE DATABASE_DEFAULT
		, CalcOrder = 1
		, CreatedDateTime = IIP.CreatedDateTime

FROM 

	(SELECT	IIP2.ActivationDate
			, IIP2.ItemId
			, IIP2.InventDimId
			, IIP2.DataAreaId
			, IIP1.Price
			, IIP1.Markup
			, IIP1.PriceUnit
			, IIP1.PriceCalcId
			, IIP1.PriceQty
			, IIP2.UnitId
			, IIP1.VersionId
			, IIP2.CreatedDateTime
	
	FROM (SELECT DISTINCT * FROM dbo.SMRBIInventItemPriceStaging) IIP1

	JOIN (SELECT ActivationDate
					, ItemId
					, InventDimId
					, DataAreaId
					, CostingType
					, PriceType
					, VersionId
					, PriceCalcId
					, UnitId
					, CreatedDateTime = MAX(ItemPriceCreatedDateTime) --Solve Data Error: when there are multiple Prices on the same day for the set dimensions (which should not happen), take the last created!

			FROM (SELECT DISTINCT * FROM dbo.SMRBIInventItemPriceStaging) IIP
			WHERE 1=1
				and CostingType = 2			--Filter on "Standard" Cost (Alternatives: 0 = Default | 1 = Planned | 2 = Standard | 3 = Actual)
				and PriceType = 0			--Filter on "Cost" Price type (Alternatives: 0 = Cost | 1 = Purchase | 2 = Sales )

			GROUP BY ActivationDate
					, ItemId
					, InventDimId
					, DataAreaId
					, CostingType
					, PriceType
					, VersionId
					, PriceCalcId
					, UnitId
		 ) IIP2

	ON IIP1.ActivationDate = IIP2.ActivationDate
		and IIP1.ItemId = IIP2.ItemId
		and IIP1.InventDimId = IIP2.InventDimId
		and IIP1.DataAreaId = IIP2.DataAreaId
		and IIP1.UnitId = IIP2.UnitId
		and IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime
		and IIP1.CostingType = IIP2.CostingType
		and IIP1.PriceType = IIP2.PriceType
		and IIP1.PriceCalcId = IIP2.PriceCalcId
		and IIP1.VersionId = IIP2.VersionId

	) IIP

	WHERE 1=1
)
/*
SELECT	SEVD1.ItemNumber
		, SEVD1.InventDimId
		, SEVD1.UnitId
		, SEVD1.CompanyId
		, SEVD1.PriceCalcId
		, Price = CAST(SEVD1.Price AS numeric(38,17))
		, SEVD1.VersionId
		, PriceType = CAST('Pending' AS nvarchar(10))
		, StartValidityDate = SEVD1.ActivationDate
		, EndValidityDate = CASE WHEN SEVD2.ItemNumber is null THEN '9999-12-31'
									WHEN SEVD2.ActivationDate = SEVD1.ActivationDate THEN SEVD2.ActivationDate
									ELSE SEVD2.ActivationDate - 1 END
		--Include for MaxCalculation (= Last available price calculation) and IsActivePrice (= Current active price)
		, CalculationNr = CAST(CASE WHEN NULLIF(SEVD1.PriceCalcId, '') is NULL THEN 'Not Calculated' ELSE 'Calculation ' + CAST(SEVD1.RankNr AS nvarchar(10)) END AS nvarchar(20))
		, CalculationNrTech = SEVD1.RankNr
		, IsMaxCalculation = CASE WHEN ROW_NUMBER() OVER (PARTITION BY SEVD1.CompanyId,SEVD1.ItemNumber,SEVD1.VersionId,SEVD1.UnitId,SEVD1.InventDimId, LEFT(SEVD1.PriceCalcId, 2) ORDER BY SEVD1.CompanyId,SEVD1.ItemNumber, SEVD1.CalcOrder DESC) = 1 THEN 'Yes' ELSE 'No' END
		, IsActivePrice = CASE WHEN SEVD2.ItemNumber is null THEN 'Yes' ELSE 'No' END
		, IsMaxPrice = CASE 
						WHEN SEVD1.PriceCalcId = '' THEN 'No'
						WHEN ROW_NUMBER() OVER (PARTITION BY SEVD1.CompanyId, SEVD1.ItemNumber, SEVD1.InventDimId, SEVD1.VersionId ORDER BY SEVD1.CompanyId, SEVD1.ItemNumber, SEVD1.InventDimId, SEVD1.RankNrIsMaxPrice DESC) = 1 THEN 'Yes' ELSE 'No' END
FROM StartEndValidityDate_Pending SEVD1
LEFT JOIN StartEndValidityDate_Pending SEVD2
ON SEVD1.RankNr = SEVD2.RankNr - 1
	and SEVD1.ItemNumber = SEVD2.ItemNumber
	and SEVD1.InventDimId = SEVD2.InventDimId
	and SEVD1.CompanyId = SEVD2.CompanyId
	and LEFT(SEVD1.PriceCalcId, 2) = LEFT(SEVD2.PriceCalcId, 2)
	and SEVD1.VersionId = SEVD2.VersionId

WHERE 1=1


UNION ALL
*/

SELECT	SEVD1.ItemNumber
		, SEVD1.InventDimId AS InventDimCode
		, SEVD1.UnitId AS UnitCode
		, SEVD1.CompanyId AS CompanyCode
		, SEVD1.PriceCalcId 
		, CAST(SEVD1.Price AS DECIMAL(38,17)) AS Price
		, SEVD1.VersionId AS VersionCode
		, CAST('Active' AS STRING) AS PriceType
		, StartValidityDate = SEVD1.ActivationDate
		, EndValidityDate = CASE WHEN SEVD2.ItemNumber is null THEN '9999-12-31'
									WHEN SEVD2.ActivationDate = SEVD1.ActivationDate THEN SEVD2.ActivationDate
									ELSE SEVD2.ActivationDate - 1 END
		--Include for MaxCalculation (= Last available price calculation) and IsActivePrice (= Current active price)
		, CalculationNr = CAST(CASE WHEN NULLIF(SEVD1.PriceCalcId, '') is NULL THEN 'Not Calculated' ELSE 'Calculation ' || CAST(SEVD1.RankNr AS STRING) END AS STRING)
		, CalculationNrTech = SEVD1.RankNr
		, IsMaxCalculation = CASE WHEN ROW_NUMBER() OVER (PARTITION BY SEVD1.CompanyId,SEVD1.ItemNumber,SEVD1.VersionId,SEVD1.UnitId,SEVD1.InventDimId, LEFT(SEVD1.PriceCalcId, 2) ORDER BY SEVD1.CompanyId,SEVD1.ItemNumber,SEVD1.CreatedDateTime DESC) = 1 THEN 'Yes' ELSE 'No' END
		, IsActivePrice = CASE WHEN SEVD2.ItemNumber is null THEN 'Yes' ELSE 'No' END
		, IsMaxPrice = CASE WHEN ROW_NUMBER() OVER (PARTITION BY SEVD1.CompanyId,SEVD1.ItemNumber,SEVD1.InventDimId ORDER BY SEVD1.CompanyId,SEVD1.ItemNumber,SEVD1.InventDimId,SEVD1.RankNrIsMaxPrice DESC) = 1 THEN 'Yes' ELSE 'No' END
FROM StartEndValidityDate_Active SEVD1
LEFT JOIN StartEndValidityDate_Active SEVD2
ON SEVD1.RankNr = SEVD2.RankNr - 1
	and SEVD1.ItemNumber = SEVD2.ItemNumber
	and SEVD1.InventDimId = SEVD2.InventDimId
	and SEVD1.CompanyId = SEVD2.CompanyId
	and LEFT(SEVD1.PriceCalcId, 2) = LEFT(SEVD2.PriceCalcId, 2)
	and SEVD1.VersionId = SEVD2.VersionId

WHERE 1=1
;
