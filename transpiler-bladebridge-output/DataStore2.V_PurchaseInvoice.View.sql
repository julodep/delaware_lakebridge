/****** Object:  View [DataStore2].[V_PurchaseInvoice]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore2`.`V_PurchaseInvoice` AS 

/* Discounts and costs are calculated in view

Calculations:

GROSS PURCHASE
-	DiscountAmount

=	LineAmount
-	Markup

=	NETPURCHASE

*/


/*Only use this first two temp tables when one need to report on the markups independently*/
/* !!! Adapt these two temp tables with the correct names of the Markups !!! */
;
WITH TEMP AS
(
	SELECT CompanyCode
		 , PurchaseInvoiceCode
		 , MIN(PurchaseInvoiceLineNumber) AS MinLineNumber
	FROM DataStore.PurchaseInvoice
	GROUP BY CompanyCode, PurchaseInvoiceCode
)

, TEMP1 AS
(
	SELECT  MT1.TransRecId AS LineRecId
			, MT2.TransRecId AS HeaderRecId
			, CASE 
				WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTransport, 0) -- Fixed surcharge
				WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTransport, 0) * PCHI.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
				WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTransport, 0) /100.0 * (PCHI.GrossPurchaseTC) -- Surcharge is % of gross Purchase
			END 
			+ 
			CASE 
				WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTransport, 0) -- Fixed surcharge
				WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTransport, 0) /100.0 * (PCHI.GrossPurchaseTC) -- Surcharge is % of gross Purchase
			END AS SurchargeTransport

			, CASE	 
				WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTotal, 0) -- Fixed surcharge
				WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTransport, 0) * PCHI.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
				WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTotal, 0) /100.0 * (PCHI.GrossPurchaseTC) -- Surcharge is % of gross Purchase
			END 
			+ 
			CASE 
				WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTotal, 0) -- Fixed surcharge
				WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTotal, 0) /100.0 * (PCHI.GrossPurchaseTC) -- Surcharge is % of gross Purchase
			END AS SurchargeTotal
	
	FROM DataStore.PurchaseInvoice PCHI

	--Necessary for determining the surcharges on line level
	LEFT JOIN DataStore.Markup AS MT1 
	ON PCHI.HeaderRecId = MT1.TransRecId 
	AND MT1.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('VendInvoiceTrans'))

	-- Necessary for determining surcharges on header level
	LEFT JOIN TEMP T
	ON PCHI.CompanyCode = T.CompanyCode
	AND PCHI.PurchaseInvoiceCode = T.PurchaseInvoiceCode

	--Necessary for determining surcharges on header level
	LEFT JOIN DataStore.Markup AS MT2
	ON PCHI.HeaderRecId = MT2.TransRecId 
	AND MT2.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('VendInvoiceJour'))
	AND PCHI.PurchaseInvoiceLineNumber = T.MinLineNumber
)

, TEMP2 AS (
	
	SELECT	LineRecId
		, SurchargeTransport = SUM(SurchargeTransport)
		, SurchargeTotal = SUM(SurchargeTotal)

	FROM TEMP1
	GROUP BY LineRecId
)

, TEMP3 AS (
	
	SELECT	HeaderRecId
		, SurchargeTransport = SUM(SurchargeTransport)
		, SurchargeTotal = SUM(SurchargeTotal)

	FROM TEMP1
	GROUP BY HeaderRecId
)

SELECT PurchaseInvoiceIdScreening = Concat(PCHI.PurchaseInvoiceCode,PCHI.PurchaseInvoiceLineNumber,PCHI.CompanyCode)
	  
	   --Information on fields
	 , PCHI.PurchaseInvoiceCode
	 , PCHI.PurchaseInvoiceLineNumber
	 , PCHI.InvoiceLineNumberCombination
	  
	   --Dimensions
	 , PCHI.CompanyCode
	 , PCHI.PurchaseOrderCode
	 , PCHI.InventTransCode --Necessary for link between orders and invoices
	 , PCHI.InventDimCode
	 , PCHI.PurchaseOrderStatus AS PurchaseOrderStatus
	 , PCHI.SupplierCode
	 , PCHI.ProductCode
	 , PCHI.PaymentTermsCode
	 , PCHI.DeliveryModeCode
	 , PCHI.DeliveryTermsCode
	 , PCHI.InternalInvoiceCode		
	 , PCHI.TaxWriteCode
	 , PCHI.TransactionType
	 , L.ExchangeRateType AS DefaultExchangeRateTypeCode
	 , L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
	 , PCHI.TransactionCurrencyCode AS TransactionCurrencyCode
	 , COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
	 , COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
	 , COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
	  
	   --Financial Dimensions
	 , COALESCE(ADL.MainAccount, '_N/A') AS GLAccountCode /* !!!KEEP!!! */
	 , COALESCE(ADL.Intercompany, '_N/A') AS IntercompanyCode
	 --, ISNULL(ADL.CostCenter, N'_N/A') AS CostCenterCode
	 , COALESCE(ADL.BusinessSegment, '_N/A') AS BusinessSegmentCode
	 , COALESCE(ADL.Department, '_N/A') AS DepartmentCode
	 , COALESCE(ADL.EndCustomer, '_N/A') AS EndCustomerCode
	 , COALESCE(ADL.`Location`, '_N/A') AS LocationCode
	 , COALESCE(ADL.ShipmentContract, '_N/A') AS ShipmentContractCode
	 , COALESCE(ADL.LocalAccount, '_N/A') AS LocalAccountCode
	 , COALESCE(ADL.Product, '_N/A') AS ProductFDCode
	   
	   --Dates
	 , PCHI.InvoiceDate AS InvoiceDate
	   
	   --Measures: Volume
	 , PCHI.PurchaseUnit AS PurchaseUnit
	 , COALESCE(CASE WHEN PCHI.PurchaseUnit = P.ProductInventoryUnit 
THEN PCHI.InvoicedQuantity 
ELSE PCHI.InvoicedQuantity * UOM0.Factor 
END, 0) AS InvoicedQuantity_InventoryUnit
	 , COALESCE(CASE WHEN PCHI.PurchaseUnit = P.ProductPurchaseUnit 
THEN PCHI.InvoicedQuantity 
ELSE PCHI.InvoicedQuantity * UOM1.Factor 
END, 0) AS InvoicedQuantity_PurchaseUnit
	 , COALESCE(CASE WHEN PCHI.PurchaseUnit = P.ProductSalesUnit 
THEN PCHI.InvoicedQuantity 
ELSE PCHI.InvoicedQuantity * UOM2.Factor 
END, 0) AS InvoicedQuantity_SalesUnit

	   
	   --Measures: €
	   /* PurchasePricePerUnit */
	 , COALESCE(PCHI.PurchasePricePerUnitTC, 0) AS PurchasePricePerUnitTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN (PCHI.PurchasePricePerUnitTC) 
ELSE (PCHI.PurchasePricePerUnitTC) * AC.ExchangeRate END, 0) AS PurchasePricePerUnitAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN (PCHI.PurchasePricePerUnitTC) 
ELSE (PCHI.PurchasePricePerUnitTC) * RC.ExchangeRate END, 0) AS PurchasePricePerUnitRC

	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN (PCHI.PurchasePricePerUnitTC) 
ELSE (PCHI.PurchasePricePerUnitTC) * GC.ExchangeRate END, 0) AS PurchasePricePerUnitGC
	 
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.PurchasePricePerUnitTC 
ELSE PCHI.PurchasePricePerUnitTC * AC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.PurchasePricePerUnitTC 
ELSE PCHI.PurchasePricePerUnitTC * RC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.PurchasePricePerUnitTC 
ELSE PCHI.PurchasePricePerUnitTC * GC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitGC_Budget
	   /*GrossPurchase*/
	 , COALESCE(PCHI.GrossPurchaseTC, 0) AS GrossPurchaseTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * AC.ExchangeRate END, 0) AS GrossPurchaseAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * RC.ExchangeRate END, 0) AS GrossPurchaseRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * GC.ExchangeRate END, 0)	AS GrossPurchaseGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * AC_Budget.ExchangeRate END, 0) AS GrossPurchaseAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * RC_Budget.ExchangeRate END, 0) AS GrossPurchaseRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.GrossPurchaseTC 
ELSE PCHI.GrossPurchaseTC * GC_Budget.ExchangeRate END, 0) AS GrossPurchaseGC_Budget
	   /* DiscountAmount */
	 , COALESCE(PCHI.DiscountAmountTC, 0) AS DiscountAmountTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * AC.ExchangeRate END, 0) AS DiscountAmountAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * RC.ExchangeRate END, 0) AS DiscountAmountRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * GC.ExchangeRate END, 0) AS DiscountAmountGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * AC_Budget.ExchangeRate END, 0) AS DiscountAmountAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * RC_Budget.ExchangeRate END, 0) AS DiscountAmountRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.DiscountAmountTC 
ELSE PCHI.DiscountAmountTC * GC_Budget.ExchangeRate END, 0) AS DiscountAmountGC_Budget
	   /* LineAmount */
	 , COALESCE(PCHI.InvoicedPurchaseAmountTC, 0) AS InvoicedPurchaseAmountTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * AC.ExchangeRate END, 0) AS InvoicedPurchaseAmountAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * RC.ExchangeRate END, 0) AS InvoicedPurchaseAmountRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * GC.ExchangeRate END, 0) AS InvoicedPurchaseAmountGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * AC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * RC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.InvoicedPurchaseAmountTC 
ELSE PCHI.InvoicedPurchaseAmountTC * GC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountGC_Budget
	   /*MarkupAmount*/
	 , COALESCE(PCHI.MarkupAmountTC, 0) AS MarkupAmountTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * AC.ExchangeRate END, 0) AS MarkupAmountAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * RC.ExchangeRate END, 0) AS MarkupAmountRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * GC.ExchangeRate END, 0) AS MarkupAmountGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * AC_Budget.ExchangeRate END, 0) AS MarkupAmountAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * RC_Budget.ExchangeRate END, 0) AS MarkupAmountRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.MarkupAmountTC 
ELSE PCHI.MarkupAmountTC * GC_Budget.ExchangeRate END, 0) AS MarkupAmountGC_Budget
	   /* NetPurchase */
	 , COALESCE(PCHI.NetPurchaseTC, 0) AS NetPurchaseAmountTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * AC.ExchangeRate END, 0) AS NetPurchaseAmountAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * RC.ExchangeRate END, 0) AS NetPurchaseAmountRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * GC.ExchangeRate END, 0) AS NetPurchaseAmountGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * AC_Budget.ExchangeRate END, 0) AS NetPurchaseAmountAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * RC_Budget.ExchangeRate END, 0) AS NetPurchaseAmountRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN PCHI.NetPurchaseTC 
ELSE PCHI.NetPurchaseTC * GC_Budget.ExchangeRate END, 0)	AS NetPurchaseAmountGC_Budget
	 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
	 , COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
	 , COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
	 , COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
	 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget
	   /* Only use this join when one need to report on the markups independently */
	 , COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) AS SurchargeTransportTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0)
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * AC.ExchangeRate END, 0) AS SurchargeTransportAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) 
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * RC.ExchangeRate END, 0) AS SurchargeTransportRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0)
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * GC.ExchangeRate END, 0) AS SurchargeTransportGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0)
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * AC_Budget.ExchangeRate END, 0) AS SurchargeTransportAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0)
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * RC_Budget.ExchangeRate END, 0) AS SurchargeTransportRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0)
ELSE COALESCE(T2.SurchargeTransport, T3.SurchargeTransport, 0) * GC_Budget.ExchangeRate END, 0) AS SurchargeTransportGC_Budget
	 , COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) AS SurchargeTotalTC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * AC.ExchangeRate END, 0) AS SurchargeTotalAC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * RC.ExchangeRate END, 0) AS SurchargeTotalRC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * GC.ExchangeRate END, 0) AS SurchargeTotalGC
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * AC_Budget.ExchangeRate END, 0) AS SurchargeTotalAC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.ReportingCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * RC_Budget.ExchangeRate END, 0) AS SurchargeTotalRC_Budget
	 , COALESCE(CASE WHEN PCHI.TransactionCurrencyCode = L.GroupCurrency THEN COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) 
ELSE COALESCE(T2.SurchargeTotal, T3.SurchargeTotal, 0) * GC_Budget.ExchangeRate END, 0) AS SurchargeTotalGC_Budget           

FROM DataStore.PurchaseInvoice PCHI

/* Only use this join when one need to report on the markups independently */
/* Link with TEMP2 or TEMP3 table for knowing the markups */
LEFT JOIN TEMP2 T2
ON T2.LineRecId = PCHI.LineRecId

LEFT JOIN TEMP3 T3
ON T3.HeaderRecId = PCHI.HeaderRecId

--Required for Currencies:
INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			       , LES.ReportingCurrency
			       , LES.`Name`
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON PCHI.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurreny
ON RC.FromCurrencyCode = PCHI.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND PCHI.InvoiceDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = PCHI.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND PCHI.InvoiceDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = PCHI.TransactionCurrencyCode 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND PCHI.InvoiceDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for analytical dimensions:
LEFT JOIN DataStore.AnalyticalDimensionLedgerSalesAndPurchase ADL
ON PCHI.DefaultDimension = ADL.DefaultDimensionId

--Required for budget rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = PCHI.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PCHI.InvoiceDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = PCHI.TransactionCurrencyCode
AND AC_Budget.ToCurrencyCode = L.ReportingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PCHI.InvoiceDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = PCHI.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.ReportingCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PCHI.InvoiceDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

LEFT JOIN DataStore.Product P
ON PCHI.CompanyCode = P.CompanyCode
	and PCHI.ProductCode = P.ProductCode

/* ALTER/ADD if Required, for financial measures*/ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON PCHI.ProductCode = UOM0.ItemNumber
AND PCHI.CompanyCode = UOM0.CompanyCode
AND PCHI.PurchaseUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON PCHI.ProductCode = UOM1.ItemNumber
AND PCHI.CompanyCode = UOM1.CompanyCode
AND PCHI.PurchaseUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON PCHI.ProductCode = UOM2.ItemNumber
AND PCHI.CompanyCode = UOM2.CompanyCode
AND PCHI.PurchaseUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit
;
