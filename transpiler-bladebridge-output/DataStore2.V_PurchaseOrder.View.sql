/****** Object:  View [DataStore2].[V_PurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/


















CREATE OR REPLACE VIEW `DataStore2`.`V_PurchaseOrder` AS 


WITH TEMP1 AS
(
SELECT PO.RecId
	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTransport, 0) -- Fixed surcharge
		    WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTransport, 0) * PO.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
		    WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTransport, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTransport, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTransport, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END AS SurchargeTransport /* ALTER/ADD if required */

	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargePurchase, 0) -- Fixed surcharge
		 WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargePurchase, 0) * PO.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
		 WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargePurchase, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargePurchase, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargePurchase, 0) /100.0 *(PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END AS SurchargePurchase

	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeDelivery, 0) -- Fixed surcharge
		 WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeDelivery, 0) * PO.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
		 WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeDelivery, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Delivery
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeDelivery, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeDelivery, 0) /100.0 *(PO.GrossPurchaseTC) -- Surcharge is % of gross Delivery
	   END AS SurchargeDelivery

	 , CASE WHEN COALESCE(MT1.MarkupCategory, 0) = 0 THEN COALESCE(MT1.SurchargeTotal, 0) -- Fixed surcharge
			WHEN MT1.MarkupCategory = 1 THEN COALESCE(MT1.SurchargeTotal, 0) * PO.PurchasePricePerUnitTC --Surcharge is # pieces * Unit price (only on line level)
			WHEN MT1.MarkupCategory = 2 THEN COALESCE(MT1.SurchargeTotal, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END 
	   + 
	   CASE WHEN COALESCE(MT2.MarkupCategory, 0) = 0 THEN COALESCE(MT2.SurchargeTotal, 0) -- Fixed surcharge
	   		WHEN MT2.MarkupCategory = 2 THEN COALESCE(MT2.SurchargeTotal, 0) /100.0 * (PO.GrossPurchaseTC) -- Surcharge is % of gross Purchase
	   END AS SurchargeTotal
FROM DataStore.PurchaseOrder PO
--Necessary for determining the surcharges on order line level
LEFT JOIN DataStore.Markup AS MT1 
ON PO.RecId = MT1.TransRecId 
AND MT1.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('PurchLine'))

--Necessary for determining surcharges on order header level
LEFT JOIN DataStore.Markup AS MT2
ON PO.RecId = MT2.TransRecId 
AND MT2.TransTableCode IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName IN ('PurchTable'))
AND PO.PurchaseOrderLineNumber = 1 --Header surcharges are taken into account on the first Purchase order line only
),

TEMP2 AS
(
SELECT	RecId
	  , SUM(SurchargeTransport) AS SurchargeTransport
	  , SUM(SurchargePurchase) AS SurchargePurchase
	  , SUM(SurchargeDelivery) AS SurchargeDelivery
	  , SUM(SurchargeTotal) AS SurchargeTotal
FROM TEMP1

GROUP BY RecId
)
SELECT Concat(PO.PurchaseOrderCode,PO.PurchaseOrderLineNumber,PO.CompanyCode) AS PurchaseOrderIdScreening
	  
	   --Information on fields
	 , PO.PurchaseOrderCode
	 , PO.PurchaseOrderLineNumber
	 , PO.OrderLineNumberCombination
	 , PO.DeliveryAddress
	  
	   --Dimensions
	 , PO.CompanyCode
	 , PO.InventTransCode
	 , PO.InventDimCode
	 , PO.ProductCode
	 , PO.SupplierCode
	 , PO.OrderSupplierCode
	 , PO.DeliveryModeCode
	 , PO.PaymentTermsCode
	 , PO.DeliveryTermsCode
	 , PO.PurchaseOrderStatus AS PurchaseOrderStatus
	 , PO.TransactionCurrencyCode
	 , L.ExchangeRateType AS DefaultExchangeRateTypeCode
	 , L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
	 , COALESCE(CAST(L.AccountingCurrency AS STRING), '_N/A') AS AccountingCurrencyCode
	 , COALESCE(CAST(L.ReportingCurrency AS STRING), '_N/A') AS ReportingCurrencyCode
	 , COALESCE(CAST(L.GroupCurrency AS STRING), '_N/A') AS GroupCurrencyCode

	   --Dates
	 , PO.CreationDate AS CreationDate
	 , PO.RequestedDeliveryDate AS RequestedDeliveryDate
	 , PO.ConfirmedDeliveryDate AS ConfirmedDeliveryDate

	   --Measures: Volume
	 , PO.PurchaseUnit

	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductInventoryUnit 
THEN PO.OrderedQuantity
ELSE PO.OrderedQuantity * UOM0.Factor 
END, 0) AS OrderedQuantity_InventoryUnit
	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductPurchaseUnit 
THEN PO.OrderedQuantity
ELSE PO.OrderedQuantity * UOM1.Factor 
END, 0) AS OrderedQuantity_PurchaseUnit
	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductSalesUnit 
THEN PO.OrderedQuantity 
ELSE PO.OrderedQuantity * UOM2.Factor 
END, 0) AS OrderedQuantity_SalesUnit

	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductInventoryUnit 
THEN PO.DeliveredQuantity
ELSE PO.DeliveredQuantity * UOM0.Factor 
END, 0) AS DeliveredQuantity_InventoryUnit
	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductPurchaseUnit 
THEN PO.DeliveredQuantity
ELSE PO.DeliveredQuantity * UOM1.Factor 
END, 0) AS DeliveredQuantity_PurchaseUnit
	 , COALESCE(CASE WHEN PO.PurchaseUnit = P.ProductSalesUnit 
THEN PO.DeliveredQuantity 
ELSE PO.DeliveredQuantity * UOM2.Factor 
END, 0) AS DeliveredQuantity_SalesUnit

	   --Measures = €
	   /* Purchase Price Per Unit */
	 , COALESCE(PO.PurchasePricePerUnitTC, 0) AS PurchasePricePerUnitTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * AC.ExchangeRate END, 0) AS PurchasePricePerUnitAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * RC.ExchangeRate END, 0)	AS PurchasePricePerUnitRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * GC.ExchangeRate END, 0) AS PurchasePricePerUnitGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * AC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * RC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.PurchasePricePerUnitTC 
ELSE PO.PurchasePricePerUnitTC * GC_Budget.ExchangeRate END, 0) AS PurchasePricePerUnitGC_Budget

	   /* Gross Purchase */
	 , COALESCE(GrossPurchaseTC, 0) AS GrossPurchaseTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.GrossPurchaseTC 
ELSE GrossPurchaseTC * AC.ExchangeRate END, 0) AS GrossPurchaseAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN GrossPurchaseTC 
ELSE GrossPurchaseTC * RC.ExchangeRate END, 0)	AS GrossPurchaseRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN GrossPurchaseTC 
ELSE GrossPurchaseTC * GC.ExchangeRate END, 0)	AS GrossPurchaseGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN GrossPurchaseTC 
ELSE GrossPurchaseTC * AC_Budget.ExchangeRate END, 0) AS GrossPurchaseAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN GrossPurchaseTC 
ELSE GrossPurchaseTC * RC_Budget.ExchangeRate END, 0) AS GrossPurchaseRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN GrossPurchaseTC 
ELSE GrossPurchaseTC * GC_Budget.ExchangeRate END, 0) AS GrossPurchaseGC_Budget

	   /* Discount Amount */
	 , COALESCE(PO.DiscountAmountTC, 0) AS DiscountAmountTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC * AC.ExchangeRate END, 0) AS DiscountAmountAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC* RC.ExchangeRate END, 0) AS DiscountAmountRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC * GC.ExchangeRate END, 0)	AS DiscountAmountGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC * AC_Budget.ExchangeRate END, 0) AS DiscountAmountAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC* RC_Budget.ExchangeRate END, 0) AS DiscountAmountRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.DiscountAmountTC 
ELSE PO.DiscountAmountTC * GC_Budget.ExchangeRate END, 0) AS DiscountAmountGC_Budget

	   /* Line Amount */
	 , COALESCE(PO.InvoicedPurchaseAmountTC, 0) AS InvoicedPurchaseAmountTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * AC.ExchangeRate END, 0) AS InvoicedPurchaseAmountAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * RC.ExchangeRate END, 0) AS InvoicedPurchaseAmountRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * GC.ExchangeRate END, 0) AS InvoicedPurchaseAmountGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * AC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * RC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.InvoicedPurchaseAmountTC 
ELSE PO.InvoicedPurchaseAmountTC * GC_Budget.ExchangeRate END, 0) AS InvoicedPurchaseAmountGC_Budget

	   /* Markup Amount */
	 , COALESCE(PO.MarkupAmountTC, 0) AS MarkupAmountTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * AC.ExchangeRate END, 0) AS MarkupAmountAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * RC.ExchangeRate END, 0) AS MarkupAmountRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * GC.ExchangeRate END, 0) AS MarkupAmountGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * AC_Budget.ExchangeRate END, 0) AS MarkupAmountAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * RC_Budget.ExchangeRate END, 0) AS MarkupAmountRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.MarkupAmountTC 
ELSE PO.MarkupAmountTC * GC_Budget.ExchangeRate END, 0) AS MarkupAmountGC_Budget

	 /* Net Purchase */
	 , COALESCE(PO.NetPurchaseTC, 0) AS NetPurchaseAmountTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * AC.ExchangeRate END, 0) AS NetPurchaseAmountAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * RC.ExchangeRate END, 0) AS NetPurchaseAmountRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * GC.ExchangeRate END, 0)	   AS NetPurchaseAmountGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * AC_Budget.ExchangeRate END, 0) AS NetPurchaseAmountAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * RC_Budget.ExchangeRate END, 0) AS NetPurchaseAmountRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN PO.NetPurchaseTC 
ELSE PO.NetPurchaseTC * GC_Budget.ExchangeRate END, 0)	  AS NetPurchaseAmountGC_Budget
	 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
	 , COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
	 , COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
	 , COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC	 
	 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

	   /* ALTER/ADD if required */
	 , T2.SurchargeTransport AS SurchargeTransportTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * AC.ExchangeRate END, 0) AS SurchargeTransportAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * RC.ExchangeRate END, 0) AS SurchargeTransportRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * GC.ExchangeRate END, 0) AS SurchargeTransportGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * AC_Budget.ExchangeRate END, 0) AS SurchargeTransportAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * RC_Budget.ExchangeRate END, 0) AS SurchargeTransportRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTransport 
ELSE T2.SurchargeTransport * GC_Budget.ExchangeRate END, 0) AS SurchargeTransportGC_Budget
	 , T2.SurchargePurchase AS SurchargePurchaseTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * AC.ExchangeRate END, 0) AS SurchargePurchaseAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * RC.ExchangeRate END, 0) AS SurchargePurchaseRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * GC.ExchangeRate END, 0) AS SurchargePurchaseGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * AC_Budget.ExchangeRate END, 0) AS SurchargePurchaseAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * RC_Budget.ExchangeRate END, 0) AS SurchargePurchaseRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargePurchase 
ELSE T2.SurchargePurchase * GC_Budget.ExchangeRate END, 0) AS SurchargePurchaseGC_Budget
	 , T2.SurchargeDelivery AS SurchargeDeliveryTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * AC.ExchangeRate END, 0) AS SurchargeDeliveryAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * RC.ExchangeRate END, 0) AS SurchargeDeliveryRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * GC.ExchangeRate END, 0) AS SurchargeDeliveryGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * AC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * RC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeDelivery 
ELSE T2.SurchargeDelivery * GC_Budget.ExchangeRate END, 0) AS SurchargeDeliveryGC_Budget
	 , T2.SurchargeTotal AS SurchargeTotalTC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC.ExchangeRate END, 0) AS SurchargeTotalAC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC.ExchangeRate END, 0) AS SurchargeTotalRC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC.ExchangeRate END, 0) AS SurchargeTotalGC
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.AccountingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * AC_Budget.ExchangeRate END, 0) AS SurchargeTotalAC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.ReportingCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * RC_Budget.ExchangeRate END, 0) AS SurchargeTotalRC_Budget
	 , COALESCE(CASE WHEN PO.TransactionCurrencyCode = L.GroupCurrency THEN T2.SurchargeTotal 
ELSE T2.SurchargeTotal * GC_Budget.ExchangeRate END, 0) AS SurchargeTotalGC_Budget

FROM DataStore.PurchaseOrder PO
/* Only use this join when one need to report on the markups independently */
--Link with TEMP2 table for knowing the markups

JOIN TEMP2 T2
ON T2.RecId = PO.RecId

--Required for Actual rates:
INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			       , LES.ReportingCurrency
			       , LES.`Name`
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , GroupCurrency = G.GroupCurrencyCode
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON PO.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrency
ON RC.FromCurrencyCode = PO.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND PO.CreationDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = PO.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND PO.CreationDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = PO.TransactionCurrencyCode 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND PO.CreationDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for analytical dimensions:
--LEFT JOIN DataStore.AnalyticalDimensionLedgerSalesAndPurchase ADL
--ON PO.DefaultDimension = ADL.DefaultDimensionId

--Required for Budget rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = PO.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PO.CreationDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = PO.TransactionCurrencyCode
AND AC_Budget.ToCurrencyCode = L.ReportingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PO.CreationDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = PO.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.ReportingCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PO.CreationDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

LEFT JOIN DataStore.Product P
ON PO.CompanyCode = P.CompanyCode
	and PO.ProductCode = P.ProductCode

/* ALTER/ADD if Required, for financial measures*/ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON PO.ProductCode = UOM0.ItemNumber
AND PO.CompanyCode = UOM0.CompanyCode
AND PO.PurchaseUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON PO.ProductCode = UOM1.ItemNumber
AND PO.CompanyCode = UOM1.CompanyCode
AND PO.PurchaseUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON PO.ProductCode = UOM2.ItemNumber
AND PO.CompanyCode = UOM2.CompanyCode
AND PO.PurchaseUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit
;
