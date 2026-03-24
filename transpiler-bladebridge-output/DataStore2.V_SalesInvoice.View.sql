/****** Object:  View [DataStore2].[V_SalesInvoice]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore2`.`V_SalesInvoice` AS 

/* Discount(s and costs are calculated in view
Calculations:
GROSS SALES
-	DiscountAmount
=	LineAmount
-	Markup
-	Rebates

=	NET SALES
-	ProductionCostAmount (COGS)
=	GROSSMARGIN
*/

/* Only use this first two temp tables when one need to report on the markups independently */
/* !!! Adapt these two temp tables with the correct names of the Markups !!! */
;
WITH TEMP AS
(
SELECT CompanyCode
	 , SalesInvoiceCode
	 , MIN(SalesInvoiceLineNumber) AS MinLineNumber
FROM DataStore.SalesInvoice

GROUP BY CompanyCode, SalesInvoiceCode
),

TEMP1 AS
(
SELECT SI.LineRecId
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTransport, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTransport, 0) * SI.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTransport, 0) /100.0 * (SI.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTransport, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTransport, 0) /100.0 * (SI.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargeTransport

	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTotal, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTotal, 0) * SI.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTotal, 0) /100.0 * (SI.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTotal, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTotal, 0) /100.0 * (SI.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargeTotal

FROM DataStore.SalesInvoice SI

--Necessary for determining the surcharges on line level
LEFT JOIN DataStore.Markup AS MT1 
ON SI.LineRecId = MT1.TransRecId 
AND MT1.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('CUSTINVOICETRANS'))

-- Necessary for determining surcharges on header level
LEFT JOIN TEMP T
ON SI.CompanyCode = T.CompanyCode
AND SI.SalesInvoiceCode = T.SalesInvoiceCode

--Necessary for determining surcharges on header level
LEFT JOIN DataStore.Markup AS MT2
ON SI.HeaderRecId = MT2.TransRecId 
AND MT2.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('CUSTINVOICEJOUR'))
AND SI.SalesInvoiceLineNumber = T.MinLineNumber
)
,
TEMP2 AS
(
SELECT	LineRecId
	  , SUM(SurchargeTransport) AS SurchargeTransport
	  , SUM(SurchargeTotal) AS SurchargeTotal
FROM TEMP1
GROUP BY LineRecId
)

SELECT 
	--Information on fields
			SI.SalesInvoiceCode
		, SI.TransactionType
		, SI.SalesInvoiceLineNumber
		, SI.SalesInvoiceLineNumberCombination

	--Dimensions
		, SI.CompanyCode		
		, SI.SalesOrderCode
		, CAST(SI.SalesOrderStatus AS STRING) AS SalesOrderStatus
		, SI.InventTransCode --Required for link between orders and invoices
		, SI.InventDimCode --Required for link between orders and prices
		, CAST(SI.TaxWriteCode AS INT) AS TaxWriteCode
		, SI.CustomerCode
		, SI.ProductCode		
		, SI.PaymentTermsCode
		, SI.DeliveryModeCode
		, SI.DeliveryTermsCode	
		, SI.OrderCustomerCode 	
		, L.ExchangeRateType AS DefaultExchangeRateTypeCode
		, L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
		, SI.TransactionCurrencyCode AS TransactionCurrencyCode
		, COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
		, COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
		, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode

	--Financial Dimensions
		, COALESCE(ADL.GLAccountCode, '_N/A') AS GLAccountCode
		, '_N/A'AS IntercompanyCode
		, '_N/A' AS BusinessSegmentCode
		, '_N/A' AS DeparmentCode
		, '_N/A' AS EndCustomerCode
		, '_N/A' AS LocationCode
		, '_N/A' AS ShipmentContractCode
		, '_N/A' AS LocalAccountCode
		, '_N/A' AS ProductFDCode
		--, ISNULL(ADL.CostCenter, '_N/A') AS CostCenterCode	 

	--Dates
		, SI.InvoiceDate AS InvoiceDate
		, SI.RequestedDeliveryDate AS RequestedDeliveryDate
		, SI.ConfirmedDeliveryDate AS ConfirmedDeliveryDate

	--Measures: Volume
		, SI.SalesUnit
		/* ADD/ALTER if required */
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductInventoryUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM0.Factor 
END, 0) AS InvoicedQuantity_InventoryUnit
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductPurchaseUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM1.Factor 
END, 0) AS InvoicedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductSalesUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM2.Factor 
END, 0) AS InvoicedQuantity_SalesUnit

	--Measures: €
		/* SalesPricePerUnit */
		, COALESCE(SI.SalesPricePerUnitTC, 0) AS SalesPricePerUnitTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN (SI.SalesPricePerUnitTC) 
ELSE (SI.SalesPricePerUnitTC) * AC.ExchangeRate END, 0) AS SalesPricePerUnitAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN (SI.SalesPricePerUnitTC) 
ELSE (SI.SalesPricePerUnitTC) * RC.ExchangeRate END, 0) AS SalesPricePerUnitRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN (SI.SalesPricePerUnitTC) 
ELSE (SI.SalesPricePerUnitTC) * GC.ExchangeRate END, 0) AS SalesPricePerUnitGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.SalesPricePerUnitTC 
ELSE SI.SalesPricePerUnitTC * AC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitAC_Budget 
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.SalesPricePerUnitTC 
ELSE SI.SalesPricePerUnitTC * RC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitRC_Budget 
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.SalesPricePerUnitTC 
ELSE SI.SalesPricePerUnitTC * GC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitGC_Budget

		/* GrossSales */
		, COALESCE(SI.GrossSalesTC, 0) AS GrossSalesTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * AC.ExchangeRate END, 0) AS GrossSalesAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * RC.ExchangeRate END, 0) AS GrossSalesRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * GC.ExchangeRate END, 0) AS GrossSalesGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * AC_Budget.ExchangeRate END, 0) AS GrossSalesAC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * RC_Budget.ExchangeRate END, 0) AS GrossSalesRC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.GrossSalesTC 
ELSE SI.GrossSalesTC * GC_Budget.ExchangeRate END, 0) AS GrossSalesGC_Budget

		/* DiscountAmount */
		, COALESCE(SI.DiscountAmountTC, 0) AS DiscountAmountTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * AC.ExchangeRate END, 0) AS DiscountAmountAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * RC.ExchangeRate END, 0) AS DiscountAmountRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * GC.ExchangeRate END, 0) AS DiscountAmountGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * AC_Budget.ExchangeRate END, 0) AS DiscountAmountAC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * RC_Budget.ExchangeRate END, 0) AS DiscountAmountRC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.DiscountAmountTC 
ELSE SI.DiscountAmountTC * GC_Budget.ExchangeRate END, 0) AS DiscountAmountGC_Budget

		/* LineAmount */
		, COALESCE(SI.InvoicedSalesAmountTC, 0) AS InvoicedSalesAmountTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * AC.ExchangeRate END, 0) AS InvoicedSalesAmountAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * RC.ExchangeRate END, 0) AS InvoicedSalesAmountRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * GC.ExchangeRate END, 0) AS InvoicedSalesAmountGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * AC_Budget.ExchangeRate END, 0) AS InvoicedSalesAmountAC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * RC_Budget.ExchangeRate END, 0) AS InvoicedSalesAmountRC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.InvoicedSalesAmountTC 
ELSE SI.InvoicedSalesAmountTC * GC_Budget.ExchangeRate END, 0)	AS InvoicedSalesAmountGC_Budget

		/* NetSales */
		--TotalNetSalesAmount (incl. rebate amounts) will be calculated in DataStore3 !!!
		, COALESCE(SI.NetSalesTC, 0) AS NetSalesAmountTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * AC.ExchangeRate END, 0) AS NetSalesAmountAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * RC.ExchangeRate END, 0) AS NetSalesAmountRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * GC.ExchangeRate END, 0) AS NetSalesAmountGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * AC_Budget.ExchangeRate END, 0) AS NetSalesAmountAC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * RC_Budget.ExchangeRate END, 0) AS NetSalesAmountRC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN SI.NetSalesTC 
ELSE SI.NetSalesTC * GC_Budget.ExchangeRate END, 0) AS NetSalesAmountGC_Budget

		/* Rebates */
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalTC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalAC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalRC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalGC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountOriginalGC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedTC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedAC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedRC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedGC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCompletedGC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedTC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedAC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedRC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedGC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountMarkedGC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledTC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledAC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledRC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledGC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountCancelledGC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceTC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceAC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceRC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceGC
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS RebateAmountVarianceGC_Budget

		/* Cost of Goods Sold */
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * TC_CP.ExchangeRate END * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldTC
		, COALESCE(COALESCE(CP1.Price, CP2.Price) * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldAC --Cost Prices are always denominated in Accounting Currency
		, COALESCE(CASE WHEN L.ReportingCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * RC_CP.ExchangeRate END * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldRC
		, COALESCE(CASE WHEN L.GroupCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * GC_CP.ExchangeRate END * SI.InvoicedQuantity, 0)	AS CostOfGoodsSoldGC
		, COALESCE(COALESCE(CP1.Price, CP2.Price) * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldAC_Budget --Cost Prices are always denominated in Accounting Currency
		, COALESCE(CASE WHEN L.ReportingCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * RC_CP_Budget.ExchangeRate END * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldRC_Budget
		, COALESCE(CASE WHEN L.GroupCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * GC_CP_Budget.ExchangeRate END * SI.InvoicedQuantity, 0) AS CostOfGoodsSoldGC_Budget
		
		/* Gross Margin */
		--Will be added in DataStore3 !!!
		/* Only use this join when one need to report on the markups independently */
		, COALESCE(T2.SurchargeTotal, 0) AS SurchargeTotalTC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC.ExchangeRate END, 0) AS SurchargeTotalAC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC.ExchangeRate END, 0) AS SurchargeTotalRC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC.ExchangeRate END, 0) AS SurchargeTotalGC
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC_Budget.ExchangeRate END, 0) AS SurchargeTotalAC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC_Budget.ExchangeRate END, 0) AS SurchargeTotalRC_Budget
		, COALESCE(CASE WHEN SI.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC_Budget.ExchangeRate END, 0) AS SurchargeTotalGC_Budget
	
	--Exchange rates:	
		, CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
		, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
		, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
		, COALESCE(AC_Budget.ExchangeRate, 0)	AS AppliedExchangeRateAC_Budget
		, COALESCE(RC_Budget.ExchangeRate, 0)	AS AppliedExchangeRateRC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 0)	AS AppliedExchangeRateGC_Budget	

FROM DataStore.SalesInvoice SI

/* Only use this join when one needs to report on the markups independently */
/* Link with TEMP2 table for knowing the markups */

INNER JOIN TEMP2 T2
ON T2.LineRecId = SI.LineRecId

--Required for currency conversion in ACTUAL RATES:
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
ON SI.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = SI.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurreny
ON RC.FromCurrencyCode = SI.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = SI.TransactionCurrencyCode 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for currency conversion in BUDGET RATES:

LEFT JOIN DataStore.ExchangeRate AC_Budget -- TransactionCurrency: As CostPrices are denominated in AC, conversion needs to be done towards TC
ON AC_Budget.FromCurrencyCode = L.AccountingCurrency
AND AC_Budget.ToCurrencyCode = SI.TransactionCurrencyCode
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate TC_Budget -- AccountingCurrency
ON TC_Budget.FromCurrencyCode = SI.TransactionCurrencyCode
AND TC_Budget.ToCurrencyCode = L.AccountingCurrency
AND TC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN TC_Budget.ValidFrom AND TC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = SI.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = SI.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

--Required for currencies: conversion from AC > xC for Cost Price calculation (as cost prices are denominated in AC):
LEFT JOIN DataStore.ExchangeRate TC_CP -- TransactionCurrency
ON TC_CP.FromCurrencyCode = L.AccountingCurrency
AND TC_CP.ToCurrencyCode = SI.TransactionCurrencyCode
AND TC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN TC_CP.ValidFrom AND TC_CP.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_CP -- ReportingCurrency
ON RC_CP.FromCurrencyCode = L.AccountingCurrency
AND RC_CP.ToCurrencyCode = L.ReportingCurrency
AND RC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN RC_CP.ValidFrom AND RC_CP.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_CP -- GroupCurrency
ON GC_CP.FromCurrencyCode = L.AccountingCurrency
AND GC_CP.ToCurrencyCode = L.GroupCurrency
AND GC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN GC_CP.ValidFrom AND GC_CP.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_CP_Budget -- ReportingCurrency
ON RC_CP_Budget.FromCurrencyCode = L.AccountingCurrency
AND RC_CP_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_CP_Budget.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN RC_CP_Budget.ValidFrom AND RC_CP_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_CP_Budget -- GroupCurrency
ON GC_CP_Budget.FromCurrencyCode = L.AccountingCurrency
AND GC_CP_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_CP_Budget.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN GC_CP_Budget.ValidFrom AND GC_CP_Budget.ValidTo

--Required for analytical dimensions:
--LEFT JOIN DataStore.AnalyticalDimensionLedgerSalesAndPurchase ADL
--ON SI.DefaultDimension = ADL.DefaultDimensionId
LEFT JOIN DataStore.GLAccount ADL
ON SI.LedgerCode = ADL.GLAccountCode
AND SI.CompanyCode = ADL.CompanyCode

LEFT JOIN DataStore.Customer C
ON C.CompanyCode = SI.CompanyCode
AND C.CustomerCode = SI.CustomerCode

LEFT JOIN DataStore.Product P
ON P.CompanyCode = SI.CompanyCode
AND P.ProductCode = SI.ProductCode

--Required for Unit of Measure:
/* ALTER/ADD if Required */ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON SI.ProductCode = UOM0.ItemNumber
AND SI.CompanyCode = UOM0.CompanyCode
AND SI.SalesUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON SI.ProductCode = UOM1.ItemNumber
AND SI.CompanyCode = UOM1.CompanyCode
AND SI.SalesUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON SI.ProductCode = UOM2.ItemNumber
AND SI.CompanyCode = UOM2.CompanyCode
AND SI.SalesUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit

--Join on DataStore.ProductConfiguration to retrieve the InventDim characteristics for the Sales Invoice
LEFT JOIN DataStore.ProductConfiguration PC
ON SI.InventDimCode = PC.InventDimCode
AND SI.CompanyCode = PC.CompanyCode
AND SI.InventDimCode != 'AllBlank' --Exclude AllBlank in the joins

--Join again on DataStore.ProductConfiguration to retrieve the characteristics of the related InventDim (required for cost prices)
LEFT JOIN DataStore.ProductConfiguration PC2
ON PC.ProductConfigurationCode = PC2.ProductConfigurationCode
AND PC2.InventDimCode != SI.InventDimCode --Exclude the InventDimCode from the Sales invoice, as there will not be a determined price at this level
AND PC2.CompanyCode = PC.CompanyCode
AND PC2.SiteCode = PC.SiteCode
AND PC2.InventBatchCode = '_N/A'
AND PC2.InventColorCode = '_N/A'
AND PC2.InventSizeCode = '_N/A'
AND PC2.InventStyleCode = '_N/A'
AND PC2.InventStatusCode = '_N/A'
AND PC2.WarehouseCode = '_N/A'
AND PC2.WarehouseLocationCode = '_N/A'
	--For SMART, this needs to be changed (where applicable) as there can be multiple dimensions used for prices (e.g. Size, Color, Style,...)

--Required for Cost Of Goods Sold (cost prices are denominated in Inventory Unit and Accounting Currency):
--Join to get the inventory unit from the sales unit
--LEFT JOIN dbo.EcoResReleasedProductStaging ERRPS
--ON ERRPS.DataAreaId = SI.CompanyCode
--	and ERRPS.ItemNumber = SI.ProductCode

--Join directly on DataStore.CostPrice if a cost price exists for the sales unit
LEFT JOIN
	(SELECT DISTINCT ItemNumber
				   , UnitCode
				   , CP.CompanyCode
				   , ProductConfigurationCode
				   , Price
				   , StartValidityDate
				   , EndValidityDate
	FROM DataStore.CostPrice CP
	JOIN DataStore.ProductConfiguration PC
	ON CP.CompanyCode = PC.CompanyCode
	AND CP.InventDimCode = PC.InventDimCode) CP1
ON SI.CompanyCode = CP1.CompanyCode
AND SI.InvoiceDate BETWEEN CP1.StartValidityDate AND CP1.EndValidityDate
AND SI.ProductCode = CP1.ItemNumber
AND PC2.ProductConfigurationCode = CP1.ProductConfigurationCode --No join on INVENTDIM, as prices are not at this level
--AND ERRPS.InventoryUnitSymbol = CP1.UnitCode
AND SI.SalesUnit = CP1.UnitCode

--In case no cost price exists for the sales unit, convert the cost price unit to the applicable sales units
LEFT JOIN 
	(SELECT	DISTINCT CP.CompanyCode
				   , CP.ItemNumber
				   , CP.ProductConfigurationCode
				   , CP.StartValidityDate
				   , CP.EndValidityDate
				   , CP.Price AS CostPrice
				   , CP.UnitCode AS CostPriceUnit
				   , '***' AS Separator
				   , CP.Price / UOM1.Factor AS Price
				   , UOM1.Factor
				   , UOM1.FromUOM
				   , UOM1.ToUOM AS ConversionUnit
		FROM
			(SELECT DISTINCT ItemNumber
						   , UnitCode
						   , CP.CompanyCode
						   , ProductConfigurationCode
						   , Price
						   , StartValidityDate
						   , EndValidityDate
			FROM DataStore.CostPrice CP
			JOIN DataStore.ProductConfiguration PC
			ON CP.CompanyCode = PC.CompanyCode
			AND CP.InventDimCode = PC.InventDimCode) CP
		LEFT JOIN DataStore.UnitOfMeasure UOM1
		ON CP.ItemNumber = UOM1.ItemNumber
		AND CP.CompanyCode = UOM1.CompanyCode
		AND CP.UnitCode = UOM1.FromUOM
	) CP2
ON SI.CompanyCode = CP2.CompanyCode
AND SI.InvoiceDate BETWEEN CP2.StartValidityDate AND CP2.EndValidityDate
AND SI.ProductCode = CP2.ItemNumber
AND PC2.ProductConfigurationCode = CP2.ProductConfigurationCode --No join on INVENTDIM, as prices are not at this level
--AND ERRPS.InventoryUnitSymbol = CP2.ConversionUnit
AND SI.SalesUnit = CP2.ConversionUnit
WHERE 1=1

/* Add an extra bit of logic to avoid duplication of lines when there are multiple rebate customers for a single line */

UNION ALL

	
SELECT
	--Information on fields
		  SI.SalesInvoiceCode
		, SI.TransactionType
		, SI.SalesInvoiceLineNumber
		, SI.SalesInvoiceLineNumberCombination
		--Dimensions
		, SI.CompanyCode	 
		, SI.SalesOrderCode
		, CAST(SI.SalesOrderStatus AS STRING) AS SalesOrderStatus
		, SI.InventTransCode --Required for link between orders and invoices
		, SI.InventDimCode --Required for link between orders and prices
		, 0 AS TaxWriteCode
		--, CAST(SI.TaxWriteCode AS INT) AS TaxWriteCode
		, SI.CustomerCode
		, SI.ProductCode	 
		, SI.PaymentTermsCode
		, SI.DeliveryModeCode
		, SI.DeliveryTermsCode	 
		, SI.OrderCustomerCode
		, L.ExchangeRateType AS DefaultExchangeRateTypeCode
		, L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
		, SI.TransactionCurrencyCode AS TransactionCurrencyCode
		, COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
		, COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
		, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
		--Financial Dimensions
		, '_N/A' AS GLAccountCode
		, '_N/A' AS IntercompanyCode
		, '_N/A' AS BusinessSegmentCode
		, '_N/A' AS DeparmentCode
		, '_N/A' AS EndCustomerCode
		, '_N/A' AS LocationCode
		, '_N/A' AS ShipmentContractCode
		, '_N/A' AS LocalAccountCode
		, '_N/A' AS ProductFDCode
		--, '_N/A' AS CostCenterCode
		--Dates
		, SI.InvoiceDate AS InvoiceDate
		, SI.RequestedDeliveryDate AS RequestedDeliveryDate
		, SI.ConfirmedDeliveryDate AS ConfirmedDeliveryDate
		--Measures: Volume
		, SI.SalesUnit

		/* ADD/ALTER if required */	
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductInventoryUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM0.Factor 
END, 0) AS InvoicedQuantity_InventoryUnit
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductPurchaseUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM1.Factor 
END, 0) AS InvoicedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SI.SalesUnit = P.ProductSalesUnit 
THEN SI.InvoicedQuantity 
ELSE SI.InvoicedQuantity * UOM2.Factor 
END, 0) AS InvoicedQuantity_SalesUnit

			--Measures: €
		/* SalesPricePerUnit */
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitTC
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitAC
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitRC
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitGC
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS SalesPricePerUnitGC_Budget
		/* GrossSales */
		, CAST(0 AS DECIMAL(38,6))	AS GrossSalesTC
		, CAST(0 AS DECIMAL(38,6))	AS GrossSalesAC
		, CAST(0 AS DECIMAL(38,6))	AS GrossSalesRC
		, CAST(0 AS DECIMAL(38,6))	AS GrossSalesGC
		, CAST(0 AS DECIMAL(38,6)) AS GrossSalesAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS GrossSalesRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS GrossSalesGC_Budget
		/* DiscountAmount */
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountTC
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountAC
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountRC
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountGC
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS DiscountAmountGC_Budget
		/* LineAmount */
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountTC
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountAC
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountRC
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountGC
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS InvoicedSalesAmountGC_Budget	 
		/* NetSales */
		--TotalNetSalesAmount (incl. rebate amounts) will be calculated in DataStore3 !!!
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountTC
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountAC
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountRC
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountGC
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS NetSalesAmountGC_Budget 
		, CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
		, COALESCE(AC_Rebate.ExchangeRate, 0) AS AppliedExchangeRateAC
		, COALESCE(RC_Rebate.ExchangeRate, 0) AS AppliedExchangeRateRC
		, COALESCE(GC_Rebate.ExchangeRate, 0) AS AppliedExchangeRateGC	 
		, COALESCE(AC_RebBudget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
		, COALESCE(RC_RebBudget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
		, COALESCE(GC_RebBudget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget	 
		/* Rebates */
		, COALESCE(SR.RebateAmountOriginal, 0) AS RebateAmountOriginalTC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * AC_Rebate.ExchangeRate END, 0) AS RebateAmountOriginalAC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * RC_Rebate.ExchangeRate END, 0) AS RebateAmountOriginalRC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * GC_Rebate.ExchangeRate END, 0) AS RebateAmountOriginalGC	 
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * AC_RebBudget.ExchangeRate END, 0) AS RebateAmountOriginalAC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * RC_RebBudget.ExchangeRate END, 0) AS RebateAmountOriginalRC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountOriginal 
ELSE SR.RebateAmountOriginal * GC_RebBudget.ExchangeRate END, 0) AS RebateAmountOriginalGC_Budget
		, COALESCE(SR.RebateAmountCompleted, 0) AS RebateAmountCompletedTC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * AC_Rebate.ExchangeRate END, 0) AS RebateAmountCompletedAC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * RC_Rebate.ExchangeRate END, 0) AS RebateAmountCompletedRC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * GC_Rebate.ExchangeRate END, 0)	AS RebateAmountCompletedGC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * AC_RebBudget.ExchangeRate END, 0) AS RebateAmountCompletedAC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * RC_RebBudget.ExchangeRate END, 0) AS RebateAmountCompletedRC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountCompleted 
ELSE SR.RebateAmountCompleted * GC_RebBudget.ExchangeRate END, 0) AS RebateAmountCompletedGC_Budget
		, COALESCE(SR.RebateAmountMarked, 0) AS RebateAmountMarkedTC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * AC_Rebate.ExchangeRate END, 0) AS RebateAmountMarkedAC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * RC_Rebate.ExchangeRate END, 0) AS RebateAmountMarkedRC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * GC_Rebate.ExchangeRate END, 0) AS RebateAmountMarkedGC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * AC_RebBudget.ExchangeRate END, 0) AS RebateAmountMarkedAC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * RC_RebBudget.ExchangeRate END, 0) AS RebateAmountMarkedRC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountMarked 
ELSE SR.RebateAmountMarked * GC_RebBudget.ExchangeRate END, 0) AS RebateAmountMarkedGC_Budget	 
		, COALESCE(SR.RebateAmountCancelled, 0) AS RebateAmountCancelledTC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * AC_Rebate.ExchangeRate END, 0) AS RebateAmountCancelledAC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * RC_Rebate.ExchangeRate END, 0) AS RebateAmountCancelledRC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * GC_Rebate.ExchangeRate END, 0) AS RebateAmountCancelledGC	 
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * AC_RebBudget.ExchangeRate END, 0) AS RebateAmountCancelledAC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * RC_RebBudget.ExchangeRate END, 0) AS RebateAmountCancelledRC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountCancelled 
ELSE SR.RebateAmountCancelled * GC_RebBudget.ExchangeRate END, 0) AS RebateAmountCancelledGC_Budget	 
		, COALESCE(SR.RebateAmountVariance, 0) AS RebateAmountVarianceTC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * AC_Rebate.ExchangeRate END, 0) AS RebateAmountVarianceAC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * RC_Rebate.ExchangeRate END, 0) AS RebateAmountVarianceRC
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * GC_Rebate.ExchangeRate END, 0) AS RebateAmountVarianceGC	 
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.AccountingCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * AC_RebBudget.ExchangeRate END, 0) AS RebateAmountVarianceAC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.ReportingCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * RC_RebBudget.ExchangeRate END, 0) AS RebateAmountVarianceRC_Budget
		, COALESCE(CASE WHEN SR.RebateCurrencyCode = L.GroupCurrency THEN SR.RebateAmountVariance 
ELSE SR.RebateAmountVariance * GC_RebBudget.ExchangeRate END, 0) AS RebateAmountVarianceGC_Budget
		/* Cost of Goods Sold */
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldTC
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldAC
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldRC
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldGC 
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS CostOfGoodsSoldGC_Budget 
		/* Gross Margin */
		--Will be added in DataStore3 !!!	 
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalTC
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalAC
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalRC
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalGC	 
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalAC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalRC_Budget
		, CAST(0 AS DECIMAL(38,6)) AS SurchargeTotalGC_Budget
FROM DataStore.SalesInvoice SI

--Required for currency conversion in ACTUAL RATES:
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
ON SI.CompanyCode = L.`Name`

--Required for rebates
JOIN 
	(
	SELECT	CompanyCode
		  , SalesInvoiceCode
		  , SalesInvoiceLineId
		  , ProductCode
		  , RebateCustomerCode
		  , RebateCurrencyCode
		  , SUM(RebateAmountOriginal) AS RebateAmountOriginal
		  , SUM(RebateAmountCompleted) AS RebateAmountCompleted
		  , SUM(RebateAmountMarked) AS RebateAmountMarked
		  , SUM(RebateAmountCancelled) AS RebateAmountCancelled
		  , SUM(RebateAmountVariance) AS RebateAmountVariance
		FROM DataStore.SalesRebate
		GROUP BY CompanyCode
			   , SalesInvoiceCode
			   , SalesInvoiceLineId
			   , ProductCode
			   , RebateCustomerCode
			   , RebateCurrencyCode
	) SR
ON SR.CompanyCode = SI.CompanyCode
AND SR.SalesInvoiceCode = SI.SalesInvoiceCode
AND SR.ProductCode = SI.ProductCode
AND SR.SalesInvoiceLineId = SI.LineRecId --Sales rebates are calculated on line level
AND SR.RebateCustomerCode != SI.CustomerCode --Avoid duplication by filtering on the INVOICE customer

LEFT JOIN DataStore.ExchangeRate RC_Rebate -- ReportingCurreny
ON RC_Rebate.FromCurrencyCode = SR.RebateCurrencyCode
AND RC_Rebate.ToCurrencyCode = L.ReportingCurrency
AND RC_Rebate.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN RC_Rebate.ValidFrom AND RC_Rebate.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Rebate -- AccountingCurrency
ON AC_Rebate.FromCurrencyCode = SR.RebateCurrencyCode
AND AC_Rebate.ToCurrencyCode = L.AccountingCurrency
AND AC_Rebate.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN AC_Rebate.ValidFrom AND AC_Rebate.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Rebate -- GroupCurrency
ON GC_Rebate.FromCurrencyCode = SR.RebateCurrencyCode 
AND GC_Rebate.ToCurrencyCode = L.GroupCurrency
AND GC_Rebate.ExchangeRateTypeCode = L.ExchangeRateType
AND SI.InvoiceDate BETWEEN GC_Rebate.ValidFrom AND GC_Rebate.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_RebBudget -- ReportingCurrency
ON RC_RebBudget.FromCurrencyCode = SI.TransactionCurrencyCode
AND RC_RebBudget.ToCurrencyCode = L.ReportingCurrency
AND RC_RebBudget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN RC_RebBudget.ValidFrom AND RC_RebBudget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_RebBudget -- AccountingCurrency
ON AC_RebBudget.FromCurrencyCode = SI.TransactionCurrencyCode
AND AC_RebBudget.ToCurrencyCode = L.ReportingCurrency
AND AC_RebBudget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN AC_RebBudget.ValidFrom AND AC_RebBudget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_RebBudget -- GroupCurrency
ON GC_RebBudget.FromCurrencyCode = SI.TransactionCurrencyCode
AND GC_RebBudget.ToCurrencyCode = L.ReportingCurrency
AND GC_RebBudget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SI.InvoiceDate BETWEEN GC_RebBudget.ValidFrom AND GC_RebBudget.ValidTo

LEFT JOIN DataStore.Product P
ON SI.CompanyCode = P.CompanyCode
	and SI.ProductCode = P.ProductCode

/* ALTER/ADD if Required, for financial measures*/ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON SI.ProductCode = UOM0.ItemNumber
AND SI.CompanyCode = UOM0.CompanyCode
AND SI.SalesUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON SI.ProductCode = UOM1.ItemNumber
AND SI.CompanyCode = UOM1.CompanyCode
AND SI.SalesUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON SI.ProductCode = UOM2.ItemNumber
AND SI.CompanyCode = UOM2.CompanyCode
AND SI.SalesUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit
;
