/****** Object:  View [DataStore].[V_SalesOrder]    Script Date: 03/03/2026 16:26:09 ******/












CREATE OR REPLACE VIEW `DataStore`.`V_SalesOrder` AS

/* Additional grouping level is added since it is possible to have multiple markup(s) (categories) applied on the same line*/
;
SELECT	  SalesOrderId AS SalesOrderCode
		, SalesOrderLineNumber
		, SalesOrderLineNumberCombination
		, OrderTransaction
		, DeliveryAddress
		, DocumentStatus
		, HeaderRecId
		, LineRecId
		, DefaultDimension
		, CompanyId AS CompanyCode
		, InventTransId AS InventTransCode
		, InventDimId AS InventDimCode
		, OrderCustomerId AS OrderCustomerCode
		, InvoiceCustomerId AS CustomerCode
		, ProductId AS ProductCode
		, DeliveryModeId AS DeliveryModeCode
		, PaymentTermsId AS PaymentTermsCode
		, DeliveryTermsId AS DeliveryTermsCode
		, SalesOrderStatus 
		, TransactionCurrencyId AS TransactionCurrencyCode
		, CreationDate
		, RequestedShippingDate
		, ConfirmedShippingDate
		, RequestedDeliveryDate
		, ConfirmedDeliveryDate
		, FirstShipmentDate
		, LastShipmentDate
		, SalesUnit
		, OrderedQuantity
		, OrderedQuantityRemaining
		, DeliveredQuantity
		, SalesPricePerUnitTC
		, SUM(GrossSalesTC) AS GrossSalesTC
		, SUM(DiscountAmountTC) AS DiscountAmountTC
		, SUM(InvoicedSalesAmountTC) AS InvoicedSalesAmountTC
		, SUM(MarkupAmountTC) AS MarkupAmountTC
		, SUM(NetSalesTC) AS NetSalesTC
FROM (

SELECT --Information on fields
		COALESCE(CASE WHEN SOHS.SalesOrderNumber = '' THEN NULL ELSE SOHS.SalesOrderNumber END, '_N/A') AS SalesOrderId
		, SOLS.LineNum AS SalesOrderLineNumber
		, SOHS.SalesOrderNumber || ' - ' || CAST(CAST(SOLS.LineNum AS INT) AS STRING) AS SalesOrderLineNumberCombination
		, CAST(CASE WHEN NULLIF(SOHS.ReturnItemNum, '') is not NULL THEN 'Return Order' ELSE 'Sales Order' END AS STRING) AS OrderTransaction
		, COALESCE(CASE WHEN SOHS.DeliveryAddressStreet = '' 
THEN NULL 
ELSE SOHS.DeliveryAddressStreetNumber || ' ' || SOHS.DeliveryAddressStreet || ', ' || SOHS.DeliveryAddressZipCode || ' ' 
|| SOHS.DeliveryAddressCity || ', ' || SOHS.DeliveryAddressCountryRegionId
END, '_N/A') AS DeliveryAddress
		, DocumentStatus = COALESCE(STRM.Name, '_N/A')
	   --Dimensions
		, SOHS.SalesTableRecId AS HeaderRecId
		, SOLS.SalesLineRecId AS LineRecId
		, SOLS.DefaultDimension AS DefaultDimension
		, UPPER(SOHS.DataAreaId) AS CompanyId
		, COALESCE(SOLS.InventoryLotId, '_N/A') AS InventTransId -- Required for relation between orders and invoices (via Fact)
		, COALESCE(SOLS.InventDimId, '_N/A') AS InventDimId
		
		, COALESCE(CASE WHEN SOHS.InvoiceCustomerAccountNumber = '' THEN NULL ELSE UPPER(SOHS.InvoiceCustomerAccountNumber) END, '_N/A') AS InvoiceCustomerId
		, COALESCE(CASE WHEN SOHS.OrderingCustomerAccountNumber = '' THEN NULL ELSE UPPER(SOHS.OrderingCustomerAccountNumber) END, '_N/A') AS OrderCustomerId
			
		, COALESCE(CASE WHEN SOLS.ItemNumber = '' THEN NULL ELSE UPPER(SOLS.ItemNumber) END, '_N/A') AS ProductId --Note! ProductNumber and ItemNumber are 2 concepts in AX. Always use the Itemnumber as this is the number applicable across entities
		, COALESCE(CASE WHEN SOHS.DeliveryModeCode = '' THEN NULL ELSE UPPER(SOHS.DeliveryModeCode) END, '_N/A') AS DeliveryModeId
		, COALESCE(CASE WHEN SOHS.PaymentTermsName = '' THEN NULL ELSE UPPER(SOHS.PaymentTermsName) END, '_N/A') AS PaymentTermsId
		, COALESCE(CASE WHEN SOHS.DeliveryTermsCode = '' THEN NULL ELSE UPPER(SOHS.DeliveryTermsCode) END, '_N/A') AS DeliveryTermsId
		, COALESCE(SM.Name, 'Unknown') AS SalesOrderStatus
		, UPPER(SOHS.CurrencyCode) AS TransactionCurrencyId

	--Dates
		, CAST(COALESCE(SOHS.SalesTableCreatedDateTime, '1900-01-01') AS DATE) AS CreationDate
		, CAST(COALESCE(COALESCE(SOHS.RequestedShippingDate,SOLS.RequestedShippingDate), '1900-01-01') AS DATE) AS RequestedShippingDate
		, CAST(COALESCE(COALESCE(SOLS.ConfirmedShippingDate,SOHS.ConfirmedShippingDate), '1900-01-01') AS DATE) AS ConfirmedShippingDate
		, CAST(COALESCE(COALESCE(SOHS.RequestedReceiptDate,SOLS.RequestedReceiptDate), '1900-01-01') AS DATE) AS RequestedDeliveryDate
		, CAST(COALESCE(COALESCE(SOLS.ConfirmedReceiptDate,SOHS.ConfirmedReceiptDate), '1900-01-01') AS DATE) AS ConfirmedDeliveryDate
		, CAST(COALESCE(CPST.FirstShipmentDate, '1900-01-01') AS DATE) AS FirstShipmentDate
		, CAST(COALESCE(CPST.LastShipmentDate, '1900-01-01') AS DATE) AS LastShipmentDate
	--Measures: Volume
		, COALESCE(UPPER(SOLS.SalesUnitSymbol), '_N/A') AS SalesUnit
		, COALESCE(SOLS.OrderedSalesQuantity, 0) AS OrderedQuantity
		, COALESCE(SOLS.RemainSalesPhysical, 0) AS OrderedQuantityRemaining
		, COALESCE(SOLS.OrderedSalesQuantity, 0) - COALESCE(SOLS.RemainSalesPhysical, 0) AS DeliveredQuantity
	--Measures: €
	--SalesPricePerUnit
		, SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END) AS SalesPricePerUnitTC
	--Gross Sales
		, SOLS.LineAmount 
			+ (SOLS.LineDiscountPercentage/100.0 * (SOLS.OrderedSalesQuantity * SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- % discount on gross sales (= Quantity * Price per unit)
				+ SOLS.LineDiscountAmount * SOLS.OrderedSalesQuantity) AS GrossSalesTC -- Fixed discount per unit	
	--Discount Amount
		, SOLS.LineDiscountPercentage/100.0 * (SOLS.OrderedSalesQuantity * SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- % discount on gross sales (= Quantity * Price per unit)
			+ SOLS.LineDiscountAmount * SOLS.OrderedSalesQuantity AS DiscountAmountTC -- Fixed discount per unit
	--Line Amount
		, SOLS.LineAmount AS InvoicedSalesAmountTC
	--Markup Amount (Header + Line)
		, CASE WHEN COALESCE(MDCE1.MarkupCategory, 0) = 0 
		  		THEN COALESCE(MDCE1.Markup, 0) -- Fixed markup
		  	WHEN MDCE1.MarkupCategory = 1
		  		THEN COALESCE(MDCE1.Markup, 0) * SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END) -- Markup is #pieces (= Markup qty * Price per unit ) (only applicable on lines, not header)
		  	WHEN MDCE1.MarkupCategory = 2 
		  		THEN COALESCE(MDCE1.Markup, 0) / 100.0 * (SOLS.OrderedSalesQuantity * SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- Markup is % of gross sales (= Quantity * Price per unit)
		  	END 
		  +
		  CASE WHEN COALESCE(MDCE2.MarkupCategory, 0) = 0
		  		THEN COALESCE(MDCE2.Markup, 0) -- Fixed markup
		  	WHEN MDCE2.MarkupCategory = 2 
		  		THEN COALESCE(MDCE2.Markup, 0) / 100.0 * (SOLS.OrderedSalesQuantity*SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- Markup is % of gross sales (= Quantity * Price per unit)
		  END AS MarkupAmountTC
	--NetSales
		, SOLS.LineAmount 
		  - (CASE WHEN COALESCE(MDCE1.MarkupCategory, 0) = 0 
		  			THEN COALESCE(MDCE1.Markup, 0) -- Fixed Markup
		  		  WHEN MDCE1.MarkupCategory = 1
		  			THEN COALESCE(MDCE1.Markup, 0) * SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END) -- Markup is #pieces (= Markup qty * Price per unit ) (only applicable on lines, not header)
		  		  WHEN MDCE1.MarkupCategory = 2 
		  			THEN COALESCE(MDCE1.Markup, 0) /100.0 * (SOLS.OrderedSalesQuantity*SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- Markup is % of gross sales (= Quantity * Price per unit)
		  		END 
		  +
		  CASE WHEN COALESCE(MDCE2.MarkupCategory, 0) = 0 
		  		 THEN COALESCE(MDCE2.Markup, 0) -- Fixed Markup
		  	   WHEN MDCE2.MarkupCategory = 2 
		  		 THEN COALESCE(MDCE2.Markup, 0) /100.0 * (SOLS.OrderedSalesQuantity*SOLS.SalesPrice/(CASE WHEN SOLS.SalesPriceQuantity = 0 THEN 1 ELSE SOLS.SalesPriceQuantity END)) -- Markup is % of gross sales (= Quantity * Price per unit)
		  END) AS NetSalesTC-- Gross Sales - Discount Amount - Markup Amount

FROM dbo.SMRBISalesOrderHeaderStaging SOHS

JOIN dbo.SMRBISalesOrderLineStaging SOLS
ON SOHS.SalesOrderNumber = SOLS.SalesOrderNumber
AND SOHS.DataAreaId=SOLS.DataAreaId

-- Required for Markup on Line Level	
LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , SUM(`Value`) AS Markup
	 FROM dbo.SMRBIMarkupTransStaging
	 WHERE 1=1
	 AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'SalesLine')
	 GROUP BY DataAreaId, MarkupCategory, TransRecId
	) MDCE1
ON SOLS.SalesLineRecId = MDCE1.TransRecId
	and SOLS.DataAreaId = MDCE1.DataAreaId

-- Required for Markup on Header Level
LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , SUM(`Value`) AS Markup
		FROM dbo.SMRBIMarkupTransStaging
		WHERE 1=1
		AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'SalesTable')
		GROUP BY DataAreaId, MarkupCategory, TransRecId
	) MDCE2
ON SOHS.SalesTableRecId = MDCE2.TransRecId
AND SOHS.DataAreaId = MDCE2.DataAreaId
AND SOLS.LineNum = 1 -- Header surcharges are taken into account on the first sales order line only

-- Required for OTIF (First/Latest shipping date)
LEFT JOIN 
	(SELECT	DataAreaId
			, ItemId
			, SalesId --Use SalesId and not OrigSalesId
			, InventTransId
			, InventDimId
			, FirstShipmentDate = MIN(DeliveryDate)
			, LastShipmentDate = MAX(DeliveryDate)
			, Ordered = SUM(Ordered)
			, Qty = SUM(Qty)
		FROM dbo.SMRBICustPackingSlipTransStaging
		GROUP BY DataAreaId
			, ItemId
			, SalesId
			, InventTransId
			, InventDimId
	) CPST
ON CPST.DataAreaId = SOLS.DataAreaId
	and CPST.InventDimId = SOLS.InventDimId
	and CPST.SalesId = SOLS.SalesOrderNumber
	and CPST.ItemId = SOLS.ItemNumber
	and CPST.InventTransId = SOLS.InventoryLotId


LEFT JOIN ETL.StringMap STRM --Required for document status
ON STRM.SourceTable = 'SalesOrder'
AND STRM.SourceColumn = 'DocumentStatus'
AND SOHS.DocumentStatus = STRM.Enum
LEFT JOIN ETL.StringMap SM -- Required for sales order status
ON SM.SourceSystem = 'D365FO'
AND SM.SourceTable = 'SalesOrderLine'
AND SM.SourceColumn = 'SalesOrderLineStatus'
AND SM.Enum = COALESCE(NULLIF(SOLS.SalesOrderLineStatus, ''), 0)

) SO

GROUP BY SalesOrderId
		, SalesOrderLineNumber
		, SalesOrderLineNumberCombination
		, OrderTransaction
		, DeliveryAddress
		, DocumentStatus
		, HeaderRecId
		, LineRecId
		, DefaultDimension
		, CompanyId
		, InventTransId
		, InventDimId
		, OrderCustomerId
		, InvoiceCustomerId
		, ProductId
		, DeliveryModeId
		, PaymentTermsId
		, DeliveryTermsId
		, SalesOrderStatus
		, TransactionCurrencyId 
		, CreationDate
		, RequestedShippingDate
		, ConfirmedShippingDate
		, RequestedDeliveryDate
		, ConfirmedDeliveryDate
		, FirstShipmentDate
		, LastShipmentDate
		, SalesUnit
		, OrderedQuantity
		, OrderedQuantityRemaining
		, DeliveredQuantity
		, SalesPricePerUnitTC
;
