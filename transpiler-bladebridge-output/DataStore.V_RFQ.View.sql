/****** Object:  View [DataStore].[V_RFQ]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_RFQ` AS


SELECT    COALESCE(NULLIF(PurchRFQCaseTable.RFQCaseId,''), '_N/A') AS RFQCaseCode
		, COALESCE(NULLIF(PurchRFQCaseTable.Name,''), '_N/A') AS RFQCaseName 
		, COALESCE(PurchRFQCaseLine.LineNumber, -1)AS RFQCaseLineNumber  
		, COALESCE(NULLIF(PurchRFQTable.RFQId,''), '_N/A') AS RFQCode
		, COALESCE(NULLIF(PurchRFQTable.RFQName,''), '_N/A') AS RFQName  
		, COALESCE(PurchRFQLine.LineNum, -1) AS RFQLineNumber  
 
		/*Dimensions*/
		, COALESCE(NULLIF(PurchRFQTable.VendAccount,''), '_N/A') AS SupplierCode  
		, COALESCE(NULLIF(PurchRFQCaseTable.DataAreaId,''), '_N/A') AS CompanyCode  
		, COALESCE(NULLIF( PurchRFQLine.InventDimId,''), '_N/A') AS ProductConfigurationCode
		, COALESCE(NULLIF(PurchRFQLine.ItemId,''), '_N/A') AS ProductCode  
		, COALESCE(NULLIF(PurchRFQTable.DLVMode,''), '_N/A') AS DeliveryModeCode  
		, COALESCE(NULLIF(PurchRFQTable.DLVTerm,''), '_N/A') AS DeliveryTermsCode  
		, COALESCE(NULLIF(L.ExchangeRateType,''), '_N/A') AS DefaultExchangeRateTypeCode  
		, COALESCE(NULLIF(L.BudgetExchangeRateType,''), '_N/A') AS BudgetExchangeRateTypeCode  
		, COALESCE(NULLIF(CAST(PurchRFQLine.CurrencyCode AS STRING),''), '_N/A') AS TransactionCurrencyCode   
		, COALESCE(NULLIF(L.AccountingCurrency,''), '_N/A') AS AccountingCurrencyCode 
		, COALESCE(NULLIF(L.ReportingCurrency,''), '_N/A') AS ReportingCurrencyCode 
		, COALESCE(NULLIF(L.GroupCurrency,''), '_N/A') AS GroupCurrencyCode 

		/*Dates*/
		, CAST(COALESCE(PurchRFQLine.DeliveryDate, '1900-01-01') AS DATE) AS DeliveryDate  
		, CAST(COALESCE(PurchRFQLine.ExpiryDateTime, '1900-01-01') AS DATE) AS ExpiryDate  
		, CAST(COALESCE(PurchRFQReplyLine.PURCHRFQCREATEDDATETIME, '1900-01-01') AS DATE) AS CreatedDate  

		/*RFQ Details*/
		, CAST(COALESCE(NULLIF(StringMapPurchRFQStatusLow.Name,''), '_N/A') AS STRING) AS RFQCaseStatusLow   
		, CAST(COALESCE(NULLIF(StringMapPurchRFQStatusHigh.Name,''), '_N/A') AS STRING) AS RFQCaseStatusHigh   
		, CAST(COALESCE(NULLIF(StringMapPurchRFQStatus.Name,''), '_N/A') AS STRING) AS RFQLineStatus   

		/*Key Figures*/
		, COALESCE(NULLIF(PurchRFQReplyLine.PurchUnit,''), '_N/A') AS PurchaseUnit  
		, COALESCE(PurchRFQReplyLine.PurchQTY, 0) AS PurchQuantity  
		, COALESCE(PurchRFQReplyLine.PurchPrice, 0) AS PurchPriceTC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * AC.ExchangeRate 
END, 0) AS PurchPriceAC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * RC.ExchangeRate 
END, 0) AS PurchPriceRC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * GC.ExchangeRate 
END, 0) AS PurchPriceGC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * AC_Budget.ExchangeRate 
END, 0) AS PurchPriceAC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * RC_Budget.ExchangeRate 
END, 0) AS PurchPriceRC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchRFQReplyLine.PurchPrice 
ELSE PurchRFQReplyLine.PurchPrice * GC_Budget.ExchangeRate 
END, 0) AS PurchPriceGC_Budget  
		, COALESCE(PurchRFQReplyLine.LineAmount, 0) AS RFQLineAmountTC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * AC.ExchangeRate 
END, 0) AS RFQLineAmountAC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * RC.ExchangeRate 
END, 0) AS RFQLineAmountRC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * GC.ExchangeRate 
END, 0) AS RFQLineAmountGC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * AC_Budget.ExchangeRate 
END, 0) AS RFQLineAmountAC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * RC_Budget.ExchangeRate 
END, 0) AS RFQLineAmountRC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchRFQReplyLine.LineAmount 
ELSE PurchRFQReplyLine.LineAmount * GC_Budget.ExchangeRate 
END, 0) AS RFQLineAmountGC_Budget
		, COALESCE((PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount), 0) AS CostAvoidanceAmountTC
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * AC.ExchangeRate 
END, 0) AS CostAvoidanceAmountAC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * RC.ExchangeRate 
END, 0) AS CostAvoidanceAmountRC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * GC.ExchangeRate 
END, 0) AS CostAvoidanceAmountGC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * AC_Budget.ExchangeRate 
END, 0) AS CostAvoidanceAmountAC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * RC_Budget.ExchangeRate 
END, 0) AS CostAvoidanceAmountRC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) 
ELSE (PurchPriceMinMax.MaxLineAmount - PurchPriceMinMax.MinLineAmount) * GC_Budget.ExchangeRate 
END, 0) AS CostAvoidanceAmountGC_Budget  
		, COALESCE(PurchPriceMinMax.MaxPurchPrice, 0) AS MaxPurchPriceTC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * AC.ExchangeRate 
END, 0) AS MaxPurchPriceAC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * RC.ExchangeRate 
END, 0) AS MaxPurchPriceRC   
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * GC.ExchangeRate 
END, 0) AS MaxPurchPriceGC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * AC_Budget.ExchangeRate 
END, 0) AS MaxPurchPriceAC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * RC_Budget.ExchangeRate 
END, 0) AS MaxPurchPriceRC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchPriceMinMax.MaxPurchPrice 
ELSE PurchPriceMinMax.MaxPurchPrice * GC_Budget.ExchangeRate 
END, 0) AS MaxPurchPriceGC_Budget  
		, COALESCE(PurchPRiceMinMax.MinPurchPrice, 0) AS MinPurchPriceTC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * AC.ExchangeRate 
END, 0) AS MinPurchPriceAC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * RC.ExchangeRate 
END, 0) AS MinPurchPriceRC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * GC.ExchangeRate 
END, 0) AS MinPurchPriceGC  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.AccountingCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * AC_Budget.ExchangeRate 
END, 0) AS MinPurchPriceAC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.ReportingCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * RC_Budget.ExchangeRate 
END, 0) AS MinPurchPriceRC_Budget  
		, COALESCE(CASE WHEN PurchRFQLine.CurrencyCode = L.GroupCurrency 
THEN PurchPRiceMinMax.MinPurchPrice 
ELSE PurchPRiceMinMax.MinPurchPrice * GC_Budget.ExchangeRate 
END, 0) AS MinPurchPriceGC_Budget  
		, CAST(1 as DECIMAL(38,6)) AS AppliedExchangeRateTC  
		, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC  
		, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC  
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC  

FROM dbo.SMRBIPurchRFQCaseTableStaging AS PurchRFQCaseTable

LEFT JOIN dbo.SMRBIPurchRFQCaseLineStaging AS PurchRFQCaseLine
	ON PurchRFQCaseTable.RFQCaseId = PurchRFQCaseLine.RFQCaseId
	AND PurchRFQCaseTable.DataAreaId = PurchRFQCaseLine.DataAreaId

LEFT JOIN dbo.SMRBIPurchRFQTableStaging AS PurchRFQTable 
	ON PurchRFQTable.RFQCaseId = PurchRFQCaseTable.RFQCaseId
	AND PurchRFQTable.DataAreaId = PurchRFQCaseTable.DataAreaId

LEFT JOIN dbo.SMRBIPurchRFQLineStaging AS PurchRFQLine
	ON  PurchRFQLine.RFQId = PurchRFQTable.RFQId
	AND PurchRFQLine.DataAreaId = PurchRFQTable.DataAreaId
	AND PurchRFQLine.ItemId = PurchRFQCaseLine.ItemId
	AND PurchRFQLine.RFQCaseLineLineNumber = PurchRFQCaseLine.LineNumber

LEFT JOIN 
	(SELECT VendRFQJour.RFQCaseId,
		VendRFQJour.RFQId,
		VendRFQJour.DataAreaId,
		VendRFQJour.VendAccount,
		VendRFQTrans.ItemID,
		MinPurchPrice = MIN(VendRFQTrans.PurchPrice),
		MinLineAmount = MIN(VendRFQTrans.LineAmount),
		MinPurchQuantity = MIN(VendRFQTrans.PurchQTY),
		MaxPurchPrice = MAX(VendRFQTrans.PurchPrice),
		MaxLineAmount = MAX(VendRFQTrans.LineAmount),
		MaxPurchQuantity = MAX(VendRFQTrans.PurchQTY)

	FROM dbo.SMRBIVendRFQJourStaging AS VendRFQJour

	LEFT JOIN dbo.SMRBIVendRFQTransStaging AS VendRFQTrans
		ON VendRFQTrans.RFQId = VendRFQJour.RFQId
		AND VendRFQTrans.InternalRFQId = VendRFQJour.InternalRFQId
		AND VendRFQTrans.RFQDate = VendRFQJour.RFQDate
		AND VendRFQTrans.DataAreaId = VendRFQJour.DataAreaId

	WHERE VendRFQTrans.Status IN ('2','3','4','5','6') --Only select when Status is not on Created (=0) or Sent (=1)

	GROUP BY VendRFQJour.RFQCaseId,VendRFQJour.RFQId,VendRFQJour.DataAreaId,VendRFQJour.VendAccount,VendRFQTrans.ItemID) AS PurchPriceMinMax
		ON  PurchPriceMinMax.RFQId = PurchRFQTable.RFQId
		AND PurchPriceMinMax.DataAreaId = PurchRFQTable.DataAreaId
		AND PurchPriceMinMax.ItemId = PurchRFQLine.ItemId
		AND PurchPriceMinMax.RFQCaseId = PurchRFQCaseLine.RFQCaseId

LEFT JOIN dbo.SMRBIPurchRFQReplyLineStaging AS PurchRFQReplyLine
	ON PurchRFQReplyLine.DataAreaId = PurchRFQCaseTable.DataAreaId
	AND PurchRFQReplyLine.RFQId = PurchRFQTable.RFQId
	AND PurchRFQReplyLine.LineNum = PurchRFQLine.LineNum

JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			, LES.AccountingCurrency
			, LES.ExchangeRateType
			, LES.BudgetExchangeRateType
			, LES.`Name`
			, GroupCurrency = G.GroupCurrencyCode
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON PurchRFQCaseTable.DataAreaId = L.Name

--Required for the Actual exchange rates:

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = PurchRFQLine.CurrencyCode
	and RC.ToCurrencyCode = L.ReportingCurrency
	and RC.ExchangeRateTypeCode = L.ExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = PurchRFQLine.CurrencyCode
	and AC.ToCurrencyCode = L.AccountingCurrency
	and AC.ExchangeRateTypeCode = L.ExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = PurchRFQLine.CurrencyCode
	and GC.ToCurrencyCode = L.GroupCurrency
	and GC.ExchangeRateTypeCode = L.ExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for the Budget exchange rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = PurchRFQLine.CurrencyCode
	and RC_Budget.ToCurrencyCode = L.ReportingCurrency
	and RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = PurchRFQLine.CurrencyCode
	and AC_Budget.ToCurrencyCode = L.AccountingCurrency
	and AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = PurchRFQLine.CurrencyCode  
	and GC_Budget.ToCurrencyCode = L.GroupCurrency
	and GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and PurchRFQReplyLine.PURCHRFQCREATEDDATETIME BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

--/*To get enum labels*/
LEFT JOIN ETL.StringMap StringMapPurchRFQStatusLow
	ON StringMapPurchRFQStatusLow.SourceTable = 'PurchRFQStatus'
	AND StringMapPurchRFQStatusLow.Enum = Cast(PurchRFQCaseLine.StatusLow as STRING)

LEFT JOIN ETL.StringMap StringMapPurchRFQStatusHigh
	ON StringMapPurchRFQStatusHigh.SourceTable = 'PurchRFQStatus'
	AND StringMapPurchRFQStatusHigh.Enum = Cast(PurchRFQCaseLine.StatusHigh as STRING)

LEFT JOIN ETL.StringMap StringMapPurchRFQStatus
	ON StringMapPurchRFQStatus.SourceTable = 'PurchRFQStatus'
	AND StringMapPurchRFQStatus.Enum = Cast(PurchRFQLine.Status as STRING)
;
