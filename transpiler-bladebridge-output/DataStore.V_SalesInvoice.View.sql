/****** Object:  View [DataStore].[V_SalesInvoice]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_SalesInvoice` AS

/* Additional grouping level is added since it is possible to have multiple markup(s) (categories) applied on the same line*/
;
SELECT	SalesInvoiceId AS SalesInvoiceCode
	  , TransactionType
	  , SalesInvoiceLineNumber
	  , SalesInvoiceLineNumberCombination
	  , HeaderRecId
	  , LineRecId
	  , SalesOrderId AS SalesOrderCode
	  , InventTransId AS InventTransCode
	  , InventDimId AS InventDimCode
	  , TaxWriteCode
	  , SalesOrderStatus
	  , CompanyId AS CompanyCode
	  , ProductId AS ProductCode
	  , OrderCustomerId AS OrderCustomerCode
	  , InvoiceCustomerId AS CustomerCode
	  , DeliveryModeId AS DeliveryModeCode
	  , PaymentTermsId AS PaymentTermsCode
	  , DeliveryTermsId AS DeliveryTermsCode
	  , TransactionCurrencyId AS TransactionCurrencyCode
	  , LedgerCode 
	  , OrigSalesOrderId
	  , InvoiceDate
	  , RequestedDeliveryDate
	  , ConfirmedDeliveryDate
	  , SalesUnit
	  , InvoicedQuantity
	  , SalesPricePerUnitTC
	  , SUM(GrossSalesTC) AS GrossSalesTC
	  , SUM(DiscountAmountTC) AS DiscountAmountTC
	  , SUM(InvoicedSalesAmountTC) AS InvoicedSalesAmountTC
	  , SUM(MarkupAmountTC) AS MarkupAmountTC
	  , SUM(NetSalesTC) AS NetSalesTC
FROM (

SELECT --Information on fields
	   CIJS.InvoiceId AS SalesInvoiceId
	 , CAST(CASE WHEN LEFT(CIJS.InvoiceId, 2) = 'CI' THEN 'Customer Invoice'
	 			 WHEN LEFT(CIJS.InvoiceId, 3) = 'FTI' THEN 'Customer Free STRING Invoice'
	 			 WHEN LEFT(CIJS.InvoiceId, 3) = 'SCN' THEN 'Customer Credit Note'
	 			 WHEN LEFT(CIJS.InvoiceId, 3) = 'FTC' THEN 'Customer Free STRING Credit Note'
	 			 WHEN LEFT(CIJS.InvoiceId, 2) = 'RE' THEN 'Customer Rebate'
	 			 ELSE '_N/A' --Check if this is still applicable!
	 		END AS STRING) AS TransactionType
	 , CITS.LineNum AS SalesInvoiceLineNumber
	 , UPPER(CIJS.InvoiceId) || ' - ' || CAST(CAST(CITS.LineNum AS INT) AS STRING) AS SalesInvoiceLineNumberCombination

	   --Dimensions
	 , CIJS.CustInvoiceJourRecId AS HeaderRecId
	 , CITS.CustInvoiceTransRecId AS LineRecId
	 , COALESCE(CASE WHEN CIJS.SalesId = '' THEN NULL ELSE CIJS.SalesId END, '_N/A') AS SalesOrderId
	 , COALESCE(CASE WHEN CITS.InventTransId = '' THEN NULL ELSE CITS.InventTransId END, '_N/A') AS InventTransId --Necessary for link between sales order and sales invoice
	 , CITS.InventDimId AS InventDimId
	 , CASE WHEN TAXWRITECODE NOT IN ('21%', '12%', '6%')
			THEN 0
			WHEN TAXWRITECODE = '6%'
			THEN 6
			ELSE LTRIM(RTRIM(COALESCE(NULLIF(NULLIF(LEFT(REPLACE(CITS.TaxWriteCode,',',''), 2), '0'), ''), 0))) 
		END AS TaxWriteCode
	 , CAST('Invoiced' AS STRING) AS SalesOrderStatus	 
	 , UPPER(CIJS.DataAreaId) AS CompanyId	 
	 , COALESCE(NULLIF(UPPER(CITS.ItemId), ''), '_N/A') AS ProductId --Note! ProductNumber and ItemNumber are 2 concepts in AX. Always use the Itemnumber as this is the number applicable across entities	 
	 , COALESCE(NULLIF(UPPER(CIJS.OrderAccount), ''), '_N/A') AS OrderCustomerId
	 , COALESCE(CASE WHEN CIJS.InvoiceAccount = '' THEN NULL ELSE CIJS.InvoiceAccount END, '_N/A') AS InvoiceCustomerId	 
	 , COALESCE(CASE WHEN CIJS.DlvMode = '' THEN NULL ELSE UPPER(CIJS.DlvMode) END, '_N/A') AS DeliveryModeId
	 , COALESCE(CASE WHEN CIJS.Payment = '' THEN NULL ELSE UPPER(CIJS.Payment) END, '_N/A') AS PaymentTermsId
	 , COALESCE(CASE WHEN CIJS.DlvTerm = '' THEN NULL ELSE UPPER(CIJS.DlvTerm) END, '_N/A') AS DeliveryTermsId
	 , UPPER(CITS.CurrencyCode) AS TransactionCurrencyId
	 --, CITS.CustInvoiceTransDimension AS DefaultDimension
	 , CITS.LEDGERDIMENSIONDISPLAYVALUE AS LedgerCode
	 , COALESCE(NULLIF(CITS.OrigSalesId,''), '_N/A') AS OrigSalesOrderId

	   --Dates
	 , CAST(COALESCE(NULLIF(CIJS.InvoiceDate, ''), '1900-01-01') AS DATE) AS InvoiceDate
	 , CAST('1900-01-01' AS DATE) AS RequestedDeliveryDate
	 , CAST(COALESCE(CITS.DLVDate, '1900-01-01') AS DATE) AS ConfirmedDeliveryDate

	   --Measures: Volume
	 , COALESCE(NULLIF(CITS.SalesUnit,''), '_N/A') AS SalesUnit
	 , COALESCE(CITS.Qty, 0) AS InvoicedQuantity

	   --Measures: €
	   --SalesPricePerUnit
	 , CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END) AS SalesPricePerUnitTC
	   --Gross Sales
	 , CITS.LineAmount 
	   + (CITS.LinePercent/100.0 * (CITS.Qty * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --% discount on gross sales (=quantity * price per unit)
	   + CITS.LineDisc * CITS.Qty) AS GrossSalesTC --Fixed discount per unit)		
	   --DiscountAmount
	 , CITS.LinePercent/100.0 * (CITS.Qty*CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --% discount on gross sales (=quantity * price per unit)
	   + CITS.LineDisc * CITS.Qty AS DiscountAmountTC --Fixed discount per unit
	   --LineAmountTransactionCurrency
	 , CITS.LineAmount AS InvoicedSalesAmountTC
	   --MarkupAmount (header + line)
	 , CASE 
			WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 
				THEN COALESCE(MTS1.Markup, 0) -- Fixed markup
			WHEN MTS1.MarkupCategory = 1
				THEN COALESCE(MTS1.Markup, 0) * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END) --Surcharge is # pieces * Unit price (only on line level)
			WHEN MTS1.MarkupCategory = 2
				THEN COALESCE(MTS1.Markup, 0) /100.0 * (CITS.Qty * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --markup is % of gross sales (=quantity * price per unit)
	   END 
	   + (CASE
			   WHEN COALESCE(MTS2.MarkupCategory, 0) = 0
					THEN COALESCE(MTS2.Markup, 0) -- Fixed markup
			   WHEN MTS2.MarkupCategory = 2
					THEN COALESCE(MTS2.Markup, 0) /100.0 * (CITS.Qty * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --markup is % of gross sales (=quantity * price per unit)
		  END) AS MarkupAmountTC
	--NetSales
		, CITS.LineAmount 
		  - (CASE WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 
						THEN COALESCE(MTS1.Markup, 0) -- Fixed markup
					WHEN MTS1.MarkupCategory = 1
						THEN COALESCE(MTS1.Markup, 0) * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
					WHEN MTS1.MarkupCategory = 2
						THEN COALESCE(MTS1.Markup, 0) /100.0 * (CITS.Qty * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --markup is % of gross sales (=quantity * price per unit)
			 END 
		  + CASE 
				WHEN COALESCE(MTS2.MarkupCategory, 0) = 0
					THEN COALESCE(MTS2.Markup, 0) -- Fixed markup
				WHEN MTS2.MarkupCategory = 2
					THEN COALESCE(MTS2.Markup, 0) /100.0 * (CITS.Qty * CITS.SalesPrice/(CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)) --markup is % of gross sales (=quantity * price per unit)
			END) AS NetSalesTC--GrossSales - DiscountAmount - MarkupAmount (Rebate amount must also be excluded, but that will be done in DataStore3)

FROM dbo.SMRBICustInvoiceJourStaging CIJS

JOIN dbo.SMRBICustInvoiceTransStaging CITS
ON CIJS.InvoiceId = CITS.InvoiceId
AND CIJS.DataAreaId = CITS.DataAreaId 
AND CIJS.InvoiceDate = CITS.InvoiceDate
AND CIJS.SalesId = CITS.SalesId

-- Required for Markup on Line Level	
LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , MarkupCode
		  , SUM(`Value`) AS Markup
	 FROM dbo.SMRBIMarkupTransStaging
	 WHERE 1=1
	 AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'CustInvoiceTrans')
	 GROUP BY DataAreaId, MarkupCategory, TransRecId, MarkupCode
	) MTS1
ON CITS.CustInvoiceTransRecId = MTS1.TransRecId
AND CITS.DataAreaId = MTS1.DataAreaId

-- Required for Markup on Header Level
LEFT JOIN 
	(SELECT	DataAreaId
		  , MarkupCategory
		  , TransRecId
		  , MarkupCode
		  , SUM(`Value`) AS Markup
	 FROM dbo.SMRBIMarkupTransStaging
	 WHERE 1=1
	 AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary WHERE TableName = 'CustInvoiceJour')
	 GROUP BY DataAreaId, MarkupCategory, TransRecId, MarkupCode
	) MTS2
ON CIJS.CustInvoiceJourRecId = MTS2.TransRecId
AND CIJS.DataAreaId = MTS2.DataAreaId
AND CITS.LineNum = 1 -- Header surcharges are taken into account on the first sales order line only

) SI

WHERE 1=1

GROUP BY SalesInvoiceId
	   , TransactionType
	   , SalesInvoiceLineNumber
	   , SalesInvoiceLineNumberCombination
	   , HeaderRecId
	   , LineRecId
	   , SalesOrderId
	   , InventTransId
	   , InventDimId
	   , TaxWriteCode
	   , SalesOrderStatus
	   , CompanyId
	   , ProductId
	   , OrderCustomerId
	   , InvoiceCustomerId
	   , DeliveryModeId
	   , PaymentTermsId
	   , DeliveryTermsId
	   , TransactionCurrencyId
	   , LedgerCode
	   , OrigSalesOrderId
	   , InvoiceDate
	   , RequestedDeliveryDate
	   , ConfirmedDeliveryDate
	   , SalesUnit
	   , InvoicedQuantity
	   , SalesPricePerUnitTC
;
