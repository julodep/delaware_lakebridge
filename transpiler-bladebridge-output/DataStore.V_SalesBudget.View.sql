/****** Object:  View [DataStore].[V_SalesBudget]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_SalesBudget` AS 


SELECT --Information on fields
	   COALESCE(CASE WHEN FDFS.Comment_ = '' THEN NULL ELSE FDFS.Comment_ END, '_N/A') AS Comment

	   --Dimensions
	 , UPPER(FDFS.DataAreaId) AS CompanyCode
	 , COALESCE(CASE WHEN FDFS.ItemNumber = '' THEN NULL ELSE UPPER(FDFS.ItemNumber) END, '_N/A') AS ProductCode
	 , COALESCE(CASE WHEN FDFS.ItemGroupId = '' THEN NULL ELSE UPPER(FDFS.ItemGroupId) END, '_N/A')  AS ProductGroupCode
	 , COALESCE(CASE WHEN FDFS.CustomerAccountNumber = '' THEN NULL ELSE UPPER(FDFS.CustomerAccountNumber) END, '_N/A')  AS CustomerCode
	 , COALESCE(CASE WHEN FDFS.CustomerGroupId = '' THEN NULL ELSE  FDFS.CustomerGroupId END, '_N/A') AS CustomerGroupCode
	 , COALESCE(CASE WHEN FDFS.ForecastModelId = '' THEN NULL ELSE UPPER(FDFS.ForecastModelId) END, '_N/A') AS ForecastModelCode
	 , COALESCE(FDFS.InventDimId, '_N/A') AS InventDimCode
	 , COALESCE(FDFS.ForecastDemandForecastDimension, -1) AS DefaultDimension
	 , COALESCE(NULLIF(L.ExchangeRateType, ''), 'N/A') AS DefaultExchangeRateTypeCode
	 , COALESCE(NULLIF(L.BudgetExchangeRateType, ''), 'N/A') AS BudgetExchangeRateTypeCode
	 , UPPER(FDFS.PricingCurrencyCode) AS TransactionCurrencyCode
	 , COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), N'_N/A') AS AccountingCurrencyCode
	 , COALESCE(CAST(UPPER(NULLIF(L.ReportingCurrency,'')) AS STRING), N'_N/A') AS ReportingCurrencyCode
	 , COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), N'_N/A') AS GroupCurrencyCode

	   --Dates
	 , COALESCE(FDFS.ForecastStartDate, '1900-01-01') AS ForecastDate

	   --Measures
	 , COALESCE(NULLIF(FDFS.QuantityUnitSymbol,''), '_N/A') AS SalesUnit
	 , COALESCE(FDFS.ForecastedQuantity, 0) AS ForecastQuantity

	   /* GrossSalesAmount */
	 , COALESCE(FDFS.ForecastedRevenue, 0) AS GrossSalesAmountTC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency 
THEN FDFS.ForecastedRevenue 
ELSE FDFS.ForecastedRevenue * AC.ExchangeRate
END, 0) AS GrossSalesAmountAC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency 
THEN FDFS.ForecastedRevenue  
ELSE FDFS.ForecastedRevenue * RC.ExchangeRate 
END, 0) AS GrossSalesAmountRC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency 
THEN FDFS.ForecastedRevenue  
ELSE FDFS.ForecastedRevenue * GC.ExchangeRate 
END, 0) AS GrossSalesAmountGC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency 
THEN FDFS.ForecastedRevenue 
ELSE FDFS.ForecastedRevenue * AC_Budget.ExchangeRate 
END, 0) AS GrossSalesAmountAC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency 
THEN FDFS.ForecastedRevenue  
ELSE FDFS.ForecastedRevenue * RC_Budget.ExchangeRate 
END, 0) AS GrossSalesAmountRC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency 
THEN FDFS.ForecastedRevenue 
ELSE FDFS.ForecastedRevenue * GC_Budget.ExchangeRate 
END, 0) AS GrossSalesAmountGC_Budget

	   /* CostPrice */
	 , COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0) AS CostPriceTC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency 
THEN FDFS.ForecastedQuantity * IPS.Price 
ELSE (FDFS.ForecastedQuantity *  IPS.Price)  * AC.ExchangeRate 
END, 0) AS CostPriceAC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency 
THEN FDFS.ForecastedQuantity * IPS.Price  
ELSE (FDFS.ForecastedQuantity *  IPS.Price) * RC.ExchangeRate 
END, 0) AS CostPriceRC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency 
THEN FDFS.ForecastedQuantity *  IPS.Price  
ELSE (FDFS.ForecastedQuantity *  IPS.Price) * GC.ExchangeRate 
END, 0) AS CostPriceGC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency 
THEN FDFS.ForecastedQuantity * IPS.Price 
ELSE (FDFS.ForecastedQuantity *  IPS.Price)  * AC_Budget.ExchangeRate 
END, 0) AS CostPriceAC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency 
THEN FDFS.ForecastedQuantity * IPS.Price  
ELSE (FDFS.ForecastedQuantity *  IPS.Price) * RC_Budget.ExchangeRate 
END, 0) AS CostPriceRC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency THEN FDFS.ForecastedQuantity *  IPS.Price  ELSE (FDFS.ForecastedQuantity *  IPS.Price) * GC_Budget.ExchangeRate END, 0) AS CostPriceGC_Budget
	   
	   /* GrossMargin */
	 , (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) AS GrossMarginTC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency 
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  * AC.ExchangeRate 
END, 0) AS GrossMarginAC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency 
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) * RC.ExchangeRate 
END, 0) AS GrossMarginRC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency 
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) * GC.ExchangeRate 
END, 0) AS GrossMarginGC
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.AccountingCurrency
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) 
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  * AC_Budget.ExchangeRate 
END, 0) AS GrossMarginAC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.ReportingCurrency
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) * RC_Budget.ExchangeRate 
END, 0) AS GrossMarginRC_Budget
	 , COALESCE(CASE WHEN FDFS.PricingCurrencyCode = L.GroupCurrency 
THEN (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0))  
ELSE (COALESCE(FDFS.ForecastedRevenue, 0) - COALESCE(FDFS.ForecastedQuantity *  IPS.Price, 0)) * GC_Budget.ExchangeRate 
END, 0) AS GrossMarginGC_Budget
	 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
	 , COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
	 , COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
	 , COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
	 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIForecastDemandForecastStaging FDFS

LEFT JOIN
	(SELECT	I1.ItemNumber AS ProductId
		  , I1.FromDate AS FromDate
		  , COALESCE(I2.FromDate, '99991231') AS EndDate
		  , I1.CostingVersionId AS CostingVersionId
		  , I1.Price AS Price
		  , I1.DataAreaId AS CompanyId
	FROM dbo.SMRBIInventItemPendingPriceStaging I1

	LEFT JOIN dbo.SMRBIInventItemPendingPriceStaging I2
	ON I1.ItemNumber = I2.ItemNumber
	AND I1.DataAreaId = I2.DataAreaId
	AND I1.CostingVersionId = I2.CostingVersionId
	AND I1.PriceType = I2.PriceType
	AND I2.FromDate =
		(SELECT MIN(FromDate)
		 FROM dbo.SMRBIInventItemPendingPriceStaging 
		 WHERE 1=1
		 AND ItemNumber = I1.ItemNumber
		 AND DataAreaId = I1.DataAreaId
		 AND CostingVersionId = I1.CostingVersionId
		 AND PriceType = I1.PriceType
		 AND FromDate > I1.FromDate
		 )
	WHERE 1=1
	AND I1.PriceType = 0  /* Type = Cost; see enum field CostingVersionPriceType in AX */
	) IPS 
ON FDFS.ItemNumber = IPS.ProductId 
AND FDFS.DataAreaId = IPS.CompanyId 
AND FDFS.ForecastStartDate >= IPS.FromDate 
AND FDFS.ForecastStartDate < IPS.EndDate
AND IPS.CostingVersionId = COALESCE(CAST(YEAR(FDFS.ForecastStartDate) AS STRING), '1900') --Determine which costing version ID is required

--Necessary for the Default exchange rates:
JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			       , LES.AccountingCurrency
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , LES.`Name`
			       , G.GroupCurrencyCode AS GroupCurrency
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON FDFS.DataAreaId = L.`Name`

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = FDFS.PricingCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND FDFS.ForecastStartDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = FDFS.PricingCurrencyCode 
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND FDFS.ForecastStartDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = FDFS.PricingCurrencyCode  
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND FDFS.ForecastStartDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for the Budget exchange rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND FDFS.ForecastStartDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode 
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND FDFS.ForecastStartDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode  
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND FDFS.ForecastStartDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

WHERE 1=1
AND (FDFS.ExpandID <> 0 
		OR (FDFS.ExpandID = 0 AND FDFS.KeyId = ''))
;
