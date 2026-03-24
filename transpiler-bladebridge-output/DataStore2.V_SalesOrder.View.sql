/****** Object:  View [DataStore2].[V_SalesOrder]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore2`.`V_SalesOrder` AS


/* Only use this first two temp tables when one needs to report on the markups independently */
/* !!! Adapt these two temp tables with the correct names of the Markups !!! */
;
WITH TEMP1 AS
(
SELECT LineRecId
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTransport, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTransport, 0) * SO.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTransport, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTransport, 0) -- Fixed surcharge
	   		 WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTransport, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargeTransport
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargePurchase, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargePurchase, 0) * SO.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargePurchase, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargePurchase, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargePurchase, 0) /100.0 *(SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargePurchase
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeDelivery, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeDelivery, 0) * SO.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeDelivery, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeDelivery, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeDelivery, 0) /100.0 *(SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargeDelivery
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTotal, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTotal, 0) * SO.SalesPricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTotal, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTotal, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTotal, 0) /100.0 * (SO.GrossSalesTC) -- Surcharge is % of gross Sales
	   END AS SurchargeTotal
FROM DataStore.SalesOrder SO

--Necessary for determining the surcharges on order line level
LEFT JOIN DataStore.Markup AS MT1 --SELECT * FROM DataStore.Markup WHERE TransRecId = 5637172334 AND CompanyCode = 'NI' 
ON SO.LineRecId = MT1.TransRecId 
AND MT1.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('SALESLINE'))

--Necessary for determining surcharges on order header level
LEFT JOIN DataStore.Markup AS MT2
ON SO.HeaderRecId = MT2.TransRecId 
AND MT2.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('SALESTABLE'))
AND SO.SalesOrderLineNumber = 1 --header surcharges are taken into account on the first sales order line only
),

TEMP2 AS
(
SELECT	LineRecId
	  , SUM(SurchargeTransport) AS SurchargeTransport
	  , SUM(SurchargePurchase) AS SurchargePurchase
	  , SUM(SurchargeDelivery) AS SurchargeDelivery
	  , SUM(SurchargeTotal) AS SurchargeTotal
FROM TEMP1
GROUP BY LineRecId
)

SELECT 
	--Information on fields
		CONCAT(SO.SalesOrderCode,SO.SalesOrderLineNumber,SO.CompanyCode) AS SalesOrderIdScreening --Field is added for screening	
		, SO.SalesOrderCode
		, SO.SalesOrderLineNumber
		, SO.SalesOrderLineNumberCombination
		, CAST(SO.DeliveryAddress AS STRING) AS DeliveryAddress
		, SO.OrderTransaction
		, SO.DocumentStatus
	
	--Dimensions
		, SO.CompanyCode
		, SO.InventTransCode --Required for link between orders and invoices
		, SO.InventDimCode --Required for link between orders and prices
		, SO.DeliveryTermsCode
		, SO.PaymentTermsCode
		, SO.OrderCustomerCode
		, SO.CustomerCode
		, SO.ProductCode
		, SO.SalesOrderStatus
		, SO.DeliveryModeCode

	--Currencies	
		, L.ExchangeRateType AS DefaultExchangeRateType
		, L.BudgetExchangeRateType AS BudgetExchangeRateType
		, SO.TransactionCurrencyCode
		, COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
		, COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
		, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode

	--Dates
		, SO.CreationDate AS CreationDate
		, SO.RequestedShippingDate AS RequestedShippingDate
		, SO.ConfirmedShippingDate AS ConfirmedShippingDate
		, SO.RequestedDeliveryDate AS RequestedDeliveryDate
		, SO.ConfirmedDeliveryDate AS ConfirmedDeliveryDate
		, SO.FirstShipmentDate AS FirstShipmentDate
		, SO.LastShipmentDate AS LastShipmentDate

	--Measures: Volume
		, SO.SalesUnit
		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductInventoryUnit 
THEN SO.OrderedQuantity
ELSE SO.OrderedQuantity * UOM0.Factor 
END, 0) AS OrderedQuantity_InventoryUnit
		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductPurchaseUnit 
THEN SO.OrderedQuantity
ELSE SO.OrderedQuantity * UOM1.Factor 
END, 0) AS OrderedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductSalesUnit 
THEN SO.OrderedQuantity 
ELSE SO.OrderedQuantity * UOM2.Factor 
END, 0) AS OrderedQuantity_SalesUnit

		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductInventoryUnit 
THEN SO.DeliveredQuantity
ELSE SO.DeliveredQuantity * UOM0.Factor 
END, 0) AS DeliveredQuantity_InventoryUnit
		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductPurchaseUnit 
THEN SO.DeliveredQuantity
ELSE SO.DeliveredQuantity * UOM1.Factor 
END, 0) AS DeliveredQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SO.SalesUnit = P.ProductSalesUnit 
THEN SO.DeliveredQuantity 
ELSE SO.DeliveredQuantity * UOM2.Factor 
END, 0) AS DeliveredQuantity_SalesUnit
	--Measures= €
		/* SalesPricePerUnit */
		, COALESCE(SO.SalesPricePerUnitTC, 0) AS SalesPricePerUnitTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * AC.ExchangeRate END, 0) AS SalesPricePerUnitAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * RC.ExchangeRate END, 0) AS SalesPricePerUnitRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * GC.ExchangeRate END, 0) AS SalesPricePerUnitGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * AC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * RC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.SalesPricePerUnitTC 
ELSE SO.SalesPricePerUnitTC * GC_Budget.ExchangeRate END, 0) AS SalesPricePerUnitGC_Budget		
		/* GrossSales */
		, COALESCE(GrossSalesTC, 0) AS GrossSalesTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * AC.ExchangeRate END, 0) AS GrossSalesAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * RC.ExchangeRate END, 0) AS GrossSalesRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * GC.ExchangeRate END, 0) AS GrossSalesGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * AC_Budget.ExchangeRate END, 0) AS GrossSalesAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * RC_Budget.ExchangeRate END, 0) AS GrossSalesRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN GrossSalesTC 
ELSE GrossSalesTC * GC_Budget.ExchangeRate END, 0) AS GrossSalesGC_Budget
		/* DiscountAmount */
		, COALESCE(SO.DiscountAmountTC, 0) AS DiscountAmountTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC * AC.ExchangeRate END, 0) AS DiscountAmountAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC* RC.ExchangeRate END, 0) AS DiscountAmountRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC * GC.ExchangeRate END, 0)	AS DiscountAmountGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC * AC_Budget.ExchangeRate END, 0) AS DiscountAmountAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC* RC_Budget.ExchangeRate END, 0) AS DiscountAmountRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.DiscountAmountTC 
ELSE SO.DiscountAmountTC * GC_Budget.ExchangeRate END, 0) AS DiscountAmountGC_Budget
		/* LineAmount */
		, COALESCE(SO.InvoicedSalesAmountTC, 0) AS InvoicedSalesAmountTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * AC.ExchangeRate END, 0) AS InvoicedSalesAmountAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * RC.ExchangeRate END, 0) AS InvoicedSalesAmountRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * GC.ExchangeRate END, 0) AS InvoicedSalesAmountGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * AC_Budget.ExchangeRate END, 0) AS InvoicedSalesAmountAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * RC_Budget.ExchangeRate END, 0) AS InvoicedSalesAmountRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.InvoicedSalesAmountTC 
ELSE SO.InvoicedSalesAmountTC * GC_Budget.ExchangeRate END, 0) AS InvoicedSalesAmountGC_Budget
		/* NetSales */
		, COALESCE(SO.NetSalesTC, 0) AS NetSalesAmountTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * AC.ExchangeRate END, 0) AS NetSalesAmountAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * RC.ExchangeRate END, 0) AS NetSalesAmountRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * GC.ExchangeRate END, 0)	AS NetSalesAmountGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * AC_Budget.ExchangeRate END, 0) AS NetSalesAmountAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * RC_Budget.ExchangeRate END, 0) AS NetSalesAmountRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN SO.NetSalesTC 
ELSE SO.NetSalesTC * GC_Budget.ExchangeRate END, 0) AS NetSalesAmountGC_Budget
		/* Cost of Goods Sold */
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * TC_CP.ExchangeRate END * SO.OrderedQuantity, 0) AS CostOfGoodsSoldTC
		, COALESCE(COALESCE(CP1.Price, CP2.Price) * SO.OrderedQuantity, 0) AS CostOfGoodsSoldAC --Cost Prices are always denominated in Accounting Currency
		, COALESCE(CASE WHEN L.ReportingCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * RC_CP.ExchangeRate END * SO.OrderedQuantity, 0) AS CostOfGoodsSoldRC
		, COALESCE(CASE WHEN L.GroupCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * GC_CP.ExchangeRate END * SO.OrderedQuantity, 0) AS CostOfGoodsSoldGC
		, COALESCE(COALESCE(CP1.Price, CP2.Price) * SO.OrderedQuantity, 0) AS CostOfGoodsSoldAC_Budget --Cost Prices are always denominated in Accounting Currency
		, COALESCE(CASE WHEN L.ReportingCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * RC_CP_Budget.ExchangeRate END * SO.OrderedQuantity, 0) AS CostOfGoodsSoldRC_Budget
		, COALESCE(CASE WHEN L.GroupCurrency = L.AccountingCurrency THEN COALESCE(CP1.Price, CP2.Price) 
ELSE COALESCE(CP1.Price, CP2.Price) * GC_CP_Budget.ExchangeRate END * SO.OrderedQuantity, 0) AS CostOfGoodsSoldGC_Budget
		/* Gross Margin */
		--Will be added in DataStore3 !!!	 
		, CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
		, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
		, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC	 
		, COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
		, COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget
		/* Only use this join when one need to report on the markups independently */
		, T2.SurchargeTransport AS SurchargeTransportTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * AC.ExchangeRate END, 0) AS SurchargeTransportAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * RC.ExchangeRate END, 0) AS SurchargeTransportRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * GC.ExchangeRate END, 0) AS SurchargeTransportGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * AC_Budget.ExchangeRate END, 0) AS SurchargeTransportAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * RC_Budget.ExchangeRate END, 0) AS SurchargeTransportRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * GC_Budget.ExchangeRate END, 0) AS SurchargeTransportGC_Budget
		, T2.SurchargePurchase AS SurchargePurchaseTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * AC.ExchangeRate END, 0) AS SurchargePurchaseAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * RC.ExchangeRate END, 0) AS SurchargePurchaseRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * GC.ExchangeRate END, 0) AS SurchargePurchaseGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * AC_Budget.ExchangeRate END, 0) AS SurchargePurchaseAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * RC_Budget.ExchangeRate END, 0) AS SurchargePurchaseRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * GC_Budget.ExchangeRate END, 0) AS SurchargePurchaseGC_Budget
		, T2.SurchargeDelivery AS SurchargeDeliveryTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * AC.ExchangeRate END, 0) AS SurchargeDeliveryAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * RC.ExchangeRate END, 0) AS SurchargeDeliveryRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * GC.ExchangeRate END, 0) AS SurchargeDeliveryGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * AC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * RC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * GC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryGC_Budget
		, T2.SurchargeTotal AS SurchargeTotalTC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC.ExchangeRate END, 0) AS SurchargeTotalAC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC.ExchangeRate END, 0) AS SurchargeTotalRC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC.ExchangeRate END, 0) AS SurchargeTotalGC
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC_Budget.ExchangeRate END, 0) AS SurchargeTotalAC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC_Budget.ExchangeRate END, 0) AS SurchargeTotalRC_Budget
		, COALESCE(CASE WHEN SO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC_Budget.ExchangeRate END, 0) AS SurchargeTotalGC_Budget

FROM DataStore.SalesOrder SO

/* Only use this join when one need to report on the markups independently */
--Link with TEMP2 table for knowing the markups
JOIN TEMP2 T2
ON T2.LineRecId = SO.LineRecId

--Required for currency conversion
INNER JOIN
	(
	SELECT DISTINCT LES.AccountingCurrency
				  , LES.ReportingCurrency
				  , LES.`Name`
				  , LES.ExchangeRateType
				  , LES.BudgetExchangeRateType
				  , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON SO.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = SO.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrency
ON RC.FromCurrencyCode = SO.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = SO.TransactionCurrencyCode 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for currencies: conversion from AC > xC for Cost Price calculation (as cost prices are denominated in AC):
LEFT JOIN DataStore.ExchangeRate TC_CP -- TransactionCurrency
ON TC_CP.FromCurrencyCode = L.AccountingCurrency
AND TC_CP.ToCurrencyCode = SO.TransactionCurrencyCode
AND TC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN TC_CP.ValidFrom AND TC_CP.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_CP -- ReportingCurrency
ON RC_CP.FromCurrencyCode = L.AccountingCurrency
AND RC_CP.ToCurrencyCode = L.ReportingCurrency
AND RC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN RC_CP.ValidFrom AND RC_CP.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_CP -- GroupCurrency
ON GC_CP.FromCurrencyCode = L.AccountingCurrency
AND GC_CP.ToCurrencyCode = L.GroupCurrency
AND GC_CP.ExchangeRateTypeCode = L.ExchangeRateType
AND SO.CreationDate BETWEEN GC_CP.ValidFrom AND GC_CP.ValidTo

--Required for budget rates:
LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = SO.TransactionCurrencyCode
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SO.CreationDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = SO.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SO.CreationDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = SO.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SO.CreationDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

--Required for Cost Price calculation in BUDGET RATES (Cost Prices are denonominated in Accounting Currency)
LEFT JOIN DataStore.ExchangeRate RC_CP_Budget -- ReportingCurrency
ON RC_CP_Budget.FromCurrencyCode = L.AccountingCurrency
AND RC_CP_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_CP_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SO.CreationDate BETWEEN RC_CP_Budget.ValidFrom AND RC_CP_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_CP_Budget -- GroupCurrency
ON GC_CP_Budget.FromCurrencyCode = L.AccountingCurrency
AND GC_CP_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_CP_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND SO.CreationDate BETWEEN GC_CP_Budget.ValidFrom AND GC_CP_Budget.ValidTo

--Required for Cost Of Goods Sold (cost prices are denominated in Accounting Currency):
--Join to get the inventory unit from the sales unit
--LEFT JOIN dbo.EcoResReleasedProductStaging ERRPS
--ON ERRPS.DataAreaId = SO.CompanyCode
--	AND ERRPS.ItemNumber = SO.ProductCode

--Join directly on DataStore.CostPrice if a cost price exists for the sales unit
LEFT JOIN DataStore.CostPrice CP1
ON SO.CompanyCode = CP1.CompanyCode
AND SO.CreationDate BETWEEN CP1.StartValidityDate AND CP1.EndValidityDate
AND SO.ProductCode = CP1.ItemNumber
AND SO.InventDimCode = CP1.InventDimCode
--AND ERRPS.InventoryUnitSymbol = CP1.UnitCode
AND SO.SalesUnit = CP1.UnitCode

--In case no cost price exists for the sales unit, convert the cost price unit to the applicable sales units
LEFT JOIN 
	(SELECT	CP.CompanyCode
		  , CP.ItemNumber
		  , CP.InventDimCode
		  , CP.StartValidityDate
		  , CP.EndValidityDate
		  , CP.Price AS CostPrice
		  , CP.UnitCode AS CostPriceUnit
		  , '***' AS Separator
		  , CP.Price / UOM1.Factor AS Price
		  , UOM1.Factor
		  , UOM1.FromUOM
		  , UOM1.ToUOM AS ConversionUnit
		FROM DataStore.CostPrice CP
		LEFT JOIN DataStore.UnitOfMeasure UOM1
		ON CP.ItemNumber = UOM1.ItemNumber
		AND CP.CompanyCode = UOM1.CompanyCode
		AND CP.UnitCode = UOM1.FromUOM
	) CP2
ON SO.CompanyCode = CP2.CompanyCode
AND SO.CreationDate BETWEEN CP2.StartValidityDate AND CP2.EndValidityDate
AND SO.ProductCode = CP2.ItemNumber
AND SO.InventDimCode = CP2.InventDimCode
--AND ERRPS.InventoryUnitSymbol = CP2.ConversionUnit
AND SO.SalesUnit = CP2.ConversionUnit

LEFT JOIN DataStore.Product P
ON SO.CompanyCode = P.CompanyCode
	and SO.ProductCode = P.ProductCode

/* ALTER/ADD if Required */ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON SO.ProductCode = UOM0.ItemNumber
AND SO.CompanyCode = UOM0.CompanyCode
AND SO.SalesUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON SO.ProductCode = UOM1.ItemNumber
AND SO.CompanyCode = UOM1.CompanyCode
AND SO.SalesUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON SO.ProductCode = UOM2.ItemNumber
AND SO.CompanyCode = UOM2.CompanyCode
AND SO.SalesUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit
;
