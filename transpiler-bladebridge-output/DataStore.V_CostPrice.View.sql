/****** Object:  View [DataStore].[V_CostPrice]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_CostPrice` AS 

/* Note: all prices are denominated in ACCOUNTING currency */
/* Note: all prices are denominated in INVENTORY unit */
;
WITH StartEndValidityDate AS (

SELECT	  ROW_NUMBER() OVER (PARTITION BY IIP.DataAreaId,IIP.ItemId,IIP.UnitId,IIP.InventDimId 
							 ORDER BY IIP.DataAreaId,IIP.ItemId,IIP.ActivationDate ASC,IIP.CreatedDateTime DESC) AS RankNr
		, IIP.ActivationDate AS ActivationDate
		, IIP.ItemId AS ItemNumber
		, IIP.InventDimId AS InventDimId
		, IIP.DataAreaId AS CompanyId
		, (IIP.Price + IIP.Markup) 
		  --/ (CASE WHEN IIP.PriceQty = 0 THEN 1 ELSE IIP.PriceQty END) --Divide by the number of items to obtain the SINGLE item price
		  / (CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END) AS Price --Divide by the number of items to obtain the SINGLE item price
		, IIP.UnitId AS UnitId
		, IIP.CreatedDateTime AS CreatedDateTime

FROM 
	(SELECT	IIP2.ActivationDate
		  , IIP2.ItemId
		  , IIP2.InventDimId
		  , IIP2.DataAreaId
		  , IIP1.Price
		  , IIP1.Markup
		  , IIP1.PriceUnit
		  , IIP1.PriceQty
		  , IIP2.UnitId
		  , IIP2.CreatedDateTime	
	FROM dbo.SMRBIInventItemPriceStaging IIP1
	JOIN (SELECT ActivationDate
			   , ItemId
			   , InventDimId
			   , DataAreaId
			   , CostingType
			   , PriceType
			   , UnitId
			   , MAX(ItemPriceCreatedDateTime) AS CreatedDateTime --Solve Data Error: when there are multiple Prices on the same day for the set dimensions (which should not happen), take the last created!
			FROM dbo.SMRBIInventItemPriceStaging
			WHERE 1=1
			AND CostingType = 2			--Filter on "Standard" Cost (Alternatives: 0 = Default | 1 = Planned | 2 = Standard | 3 = Actual)
			AND PriceType = 0			--Filter on "Cost" Price type (Alternatives: 0 = Cost | 1 = Purchase | 2 = Sales )

			GROUP BY ActivationDate
				   , ItemId
				   , InventDimId
				   , DataAreaId
				   , CostingType
				   , PriceType
				   , UnitId
		 ) IIP2

	ON  IIP1.ActivationDate = IIP2.ActivationDate
	AND IIP1.ItemId = IIP2.ItemId
	AND IIP1.InventDimId = IIP2.InventDimId
	AND IIP1.DataAreaId = IIP2.DataAreaId
	AND IIP1.UnitId = IIP2.UnitId
	AND IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime
	AND IIP1.CostingType = IIP2.CostingType
	AND IIP1.PriceType = IIP2.PriceType

	) IIP
	)
SELECT	SEVD1.ItemNumber
	  , SEVD1.InventDimId AS InventDimCode
	  , SEVD1.UnitId AS UnitCode 
	  , SEVD1.CompanyId AS CompanyCode
	  , SEVD1.Price
	  , SEVD1.ActivationDate AS StartValidityDate
	  , CASE WHEN SEVD2.ItemNumber is null then '9999-12-31' ELSE SEVD2.ActivationDate - 1 END AS EndValidityDate

FROM StartEndValidityDate SEVD1

LEFT JOIN StartEndValidityDate SEVD2 
ON SEVD1.RankNr = SEVD2.RankNr - 1
AND SEVD1.ItemNumber = SEVD2.ItemNumber
AND SEVD1.InventDimId = SEVD2.InventDimId
AND SEVD1.CompanyId = SEVD2.CompanyId

WHERE 1=1

/* Create unknown member */

UNION ALL

SELECT	'_N/A'
	  , '_N/A'
	  , '_N/A'
	  , CompanyId
	  , 0
	  , '1900-01-01'
	  , '9999-12-31'
FROM dbo.SMRBIOfficeAddinLegalEntityStaging
;
