/****** Object:  View [DataStore].[V_PurchaseOrder]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_PurchaseOrder` AS 

/* Additional grouping level is added since it is possible to have multiple markup(s) (categories) applied on the same line*/
;
SELECT	PurchaseOrderId AS PurchaseOrderCode
	  , RecId
	  , PurchaseOrderLineNumber
	  , OrderLineNumberCombination
	  , DeliveryAddress
	  , CompanyId AS CompanyCode
	  , InventTransId AS InventTransCode
	  , ProductId AS ProductCode
	  , SupplierId AS SupplierCode
	  , OrderSupplierId AS OrderSupplierCode
	  , DeliveryModeId AS DeliveryModeCode
	  , PaymentTermsId AS PaymentTermsCode
	  , DeliveryTermsId AS DeliveryTermsCode
	  , PurchaseOrderStatus 
	  , TransactionCurrencyId AS TransactionCurrencyCode
	  , InventDimId AS InventDimCode
	  , CreationDate
	  , RequestedDeliveryDate
	  , ConfirmedDeliveryDate
	  , OrderedQuantity
	  , OrderedQuantityRemaining
	  , DeliveredQuantity
	  , PurchaseUnit
	  , PurchasePricePerUnitTC
	  , SUM(GrossPurchaseTC) AS GrossPurchaseTC
	  , SUM(DiscountAmountTC) AS DiscountAmountTC
	  , SUM(InvoicedPurchaseAmountTC) AS InvoicedPurchaseAmountTC
	  , SUM(MarkupAmountTC) AS MarkupAmountTC
	  , SUM(NetPurchaseTC) AS NetPurchaseTC
FROM (

SELECT --Information on fields
	   UPPER(PPOHS.PurchaseOrderNumber) AS PurchaseOrderId
	  , PPOLS.PurchLineRecId AS RecId
	  , PPOLS.LineNumber AS PurchaseOrderLineNumber
	  , UPPER(PPOHS.PurchaseOrderNumber) || ' - ' || CAST(CAST(PPOLS.LineNumber As INT) AS STRING) AS OrderLineNumberCombination
	  , COALESCE(CASE WHEN PPOHS.DeliveryAddressStreet = '' 
THEN NULL 
ELSE PPOHS.DeliveryAddressStreetNumber || ' ' || PPOHS.DeliveryAddressStreet || ', ' 
|| PPOHS.DeliveryAddressZipCode || ' ' || PPOHS.DeliveryAddressCity || ', ' || PPOHS.DeliveryAddressCountryRegionId
END, '_N/A') AS DeliveryAddress
	--Dimensions
		, UPPER(PPOHS.DataAreaId) AS CompanyId
		, COALESCE(PPOLS.InventTransId, '_N/A') AS InventTransId
		, COALESCE(CASE WHEN PPOLS.ItemNumber = '' THEN NULL ELSE UPPER(PPOLS.ItemNumber) END, '_N/A') AS ProductId
		, UPPER(COALESCE(CASE WHEN PPOHS.InvoiceVendorAccountNumber = '' THEN NULL ELSE UPPER(PPOHS.InvoiceVendorAccountNumber) END, '_N/A')) AS SupplierId
		, UPPER(COALESCE(CASE WHEN PPOHS.ORDERVENDORACCOUNTNUMBER = '' THEN NULL ELSE UPPER(PPOHS.InvoiceVendorAccountNumber) END, '_N/A')) AS OrderSupplierId
		, COALESCE(CASE WHEN PPOHS.DeliveryModeId = '' THEN NULL ELSE UPPER(PPOHS.DeliveryModeId) END, '_N/A') AS DeliveryModeId
		, COALESCE(CASE WHEN PPOHS.PaymentTermsName = '' THEN NULL ELSE UPPER(PPOHS.PaymentTermsName) END, '_N/A') AS PaymentTermsId
		, COALESCE(CASE WHEN PPOHS.DeliveryTermsId = '' THEN NULL ELSE UPPER(PPOHS.DeliveryTermsId) END, '_N/A') AS DeliveryTermsId
		, COALESCE(SM.Name, '_N/A') AS PurchaseOrderStatus
		, UPPER(COALESCE(PPOLS.CurrencyCode, PPOHS.CurrencyCode)) AS TransactionCurrencyId
		, COALESCE(PPOLS.InventDimId, '_N/A') AS InventDimId
		, CAST(PPOHS.PurchTableCreatedDateTime AS DATE)  AS CreationDate
		, COALESCE(COALESCE(PPOLS.RequestedDeliveryDate, PPOHS.RequestedDeliveryDate), '1900-01-01') AS RequestedDeliveryDate
		, COALESCE(PPOLS.ConfirmedDeliveryDate, '1900-01-01') AS ConfirmedDeliveryDate
		, COALESCE(PPOLS.OrderedPurchaseQuantity, 0) AS OrderedQuantity
		, COALESCE(RemainPurchPhysical, 0) AS OrderedQuantityRemaining
		, COALESCE(PPOLS.OrderedPurchaseQuantity, 0) - COALESCE(RemainPurchPhysical, 0) AS DeliveredQuantity
		, COALESCE(NULLIF(PPOLS.PurchaseUnitSymbol,''), 'N/A') AS PurchaseUnit
		, COALESCE(PPOLS.PurchasePrice/COALESCE(NULLIF(PPOLS.PriceUnit, 0), 1), 0) AS PurchasePricePerUnitTC
		, PPOLS.LineAmount 
			+ (PPOLS.LineDiscountPercentage/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- % discount on gross Purchase (= Quantity * Price per unit)
				+ PPOLS.LineDiscountAmount * PPOLS.OrderedPurchaseQuantity) AS GrossPurchaseTC
		, PPOLS.LineDiscountPercentage/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- % discount on gross Purchase (= Quantity * Price per unit)
			+ PPOLS.LineDiscountAmount * PPOLS.OrderedPurchaseQuantity AS DiscountAmountTC
		, COALESCE(PPOLS.LineAmount, 0) AS InvoicedPurchaseAmountTC
		, CASE WHEN COALESCE(MDCE1.MarkupCategory, 0) = 0 
			   	  THEN COALESCE(MDCE1.Markup, 0)
			   WHEN MDCE1.MarkupCategory = 1
			   	  THEN COALESCE(MDCE1.Markup, 0) * PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END) -- Markup is #pieces (= Markup qty * Price per unit ) (only applicable on lines, not header)
			   WHEN MDCE1.MarkupCategory = 2 
			   	  THEN COALESCE(MDCE1.Markup, 0) / 100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- Markup is % of gross Purchase (= Quantity * Price per unit)
		  END 
		  + CASE WHEN COALESCE(MDCE2.MarkupCategory, 0) = 0
				  THEN COALESCE(MDCE2.Markup, 0)
			     WHEN MDCE2.MarkupCategory = 2 
				  THEN COALESCE(MDCE2.Markup, 0) / 100.0 * (PPOLS.OrderedPurchaseQuantity*PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- Markup is % of gross Purchase (= Quantity * Price per unit)
		    END AS MarkupAmountTC
		, PPOLS.LineAmount 
		  - (CASE WHEN COALESCE(MDCE1.MarkupCategory, 0) = 0 
					THEN COALESCE(MDCE1.Markup, 0)
				  WHEN MDCE1.MarkupCategory = 1
					THEN COALESCE(MDCE1.Markup, 0) * PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END) -- Markup is #pieces (= Markup qty * Price per unit ) (only applicable on lines, not header)
				  WHEN MDCE1.MarkupCategory = 2 
					THEN COALESCE(MDCE1.Markup, 0) /100.0 * (PPOLS.OrderedPurchaseQuantity*PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- Markup is % of gross Purchase (= Quantity * Price per unit)
			 END )
		  + CASE WHEN COALESCE(MDCE2.MarkupCategory, 0) = 0 
					THEN COALESCE(MDCE2.Markup, 0)
				 WHEN MDCE2.MarkupCategory = 2 
					THEN COALESCE(MDCE2.Markup, 0) /100.0 * (PPOLS.OrderedPurchaseQuantity*PPOLS.PurchasePrice/(CASE WHEN PPOLS.PurchasePriceQuantity = 0 THEN 1 ELSE PPOLS.PurchasePriceQuantity END)) -- Markup is % of gross Purchase (= Quantity * Price per unit)
			END AS NetPurchaseTC -- Gross Purchase - Discount Amount - Markup Amount
FROM dbo.SMRBIPurchPurchaseOrderHeaderStaging PPOHS

JOIN dbo.SMRBIPurchPurchaseOrderLineStaging PPOLS
ON PPOHS.PurchaseOrderNumber = PPOLS.PurchaseOrderNumber
AND PPOHS.DataAreaId = PPOLS.DataAreaId

LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , SUM(`Value`) AS Markup
	 FROM dbo.SMRBIMarkupTransStaging
	 WHERE 1=1
	 AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'PurchLine')
	 GROUP BY DataAreaId, MarkupCategory, TransRecId
	) MDCE1
ON PPOLS.PurchLineRecId = MDCE1.TransRecId
AND PPOLS.DataAreaId = MDCE1.DataAreaId
LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , SUM(`Value`) AS Markup
	 FROM dbo.SMRBIMarkupTransStaging
	 WHERE 1=1
	 AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'PurchTable')
	 GROUP BY DataAreaId, MarkupCategory, TransRecId
	) MDCE2
ON PPOHS.PurchTableRecId = MDCE2.TransRecId
AND PPOHS.DataAreaId = MDCE2.DataAreaId
AND PPOLS.LineNumber = 1 -- Header surcharges are taken into account on the first sales order line only

--Required for OTIF calculation:
LEFT JOIN 
	(SELECT	VPSTS.DataAreaId
		  , OrderAccount
		  , OrigPurchId
		  , LineNum
		  , ItemId
		  , InventDimId
		  , SUM(Qty) AS DeliveredQty
		  , MAX(VPSTS.DeliveryDate) AS MaxActualDeliveryDate --In the case of split deliveries, sum up the deliveries (irrespective of the delivery date, and take the latest delivery date for comparison)
	 FROM dbo.SMRBIVendPackingSlipJourStaging VPSJS
	 JOIN dbo.SMRBIVendPackingSlipTransStaging VPSTS
	 ON VPSJS.DataAreaId = VPSTS.DataAreaId
	 AND VPSJS.VendPackingSlipJourRecId = VPSTS.VendPackingSlipJour
	 AND VPSJS.PackingSlipId = VPSTS.PackingSlipId
	 GROUP BY VPSTS.DataAreaId
			, OrigPurchId
			, OrderAccount
			, LineNum
			, ItemId
			, InventDimId) VPSTS
ON VPSTS.DataAreaId = PPOLS.DataAreaId
AND VPSTS.OrigPurchId = PPOLS.PurchaseOrderNumber
AND VPSTS.LineNum = PPOLS.LineNumber
AND VPSTS.ItemId = PPOLS.ItemNumber
AND VPSTS.InventDimId = PPOLS.InventDimId
AND VPSTS.OrderAccount = PPOHS.OrderVendorAccountNumber
LEFT JOIN DataStore.V_Date D1
ON PPOLS.ConfirmedDeliveryDate = D1.TIMESTAMP

LEFT JOIN ETL.StringMap SM
ON SM.SourceSystem = 'D365FO'
AND SM.SourceTable = 'PurchPurchaseOrderLine'
AND SM.SourceColumn = 'PurchaseOrderLineStatus'
AND SM.Enum = COALESCE(NULLIF(PPOLS.PURCHASEORDERLINESTATUS, ''), -1)

) PO

GROUP BY  PurchaseOrderId
		, RecId
		, PurchaseOrderLineNumber
		, OrderLineNumberCombination
		, DeliveryAddress
		, CompanyId
		, InventTransId
		, ProductId
		, SupplierId
		, OrderSupplierId
		, DeliveryModeId
		, PaymentTermsId
		, DeliveryTermsId
		, PurchaseOrderStatus
		, TransactionCurrencyId
		, InventDimId
		, CreationDate
		, RequestedDeliveryDate
		, ConfirmedDeliveryDate
		, OrderedQuantity
		, OrderedQuantityRemaining
		, DeliveredQuantity
		, PurchaseUnit
		, PurchasePricePerUnitTC
;
