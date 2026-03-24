/****** Object:  View [DataStore].[V_PurchaseInvoice]    Script Date: 03/03/2026 16:26:09 ******/













CREATE OR REPLACE VIEW `DataStore`.`V_PurchaseInvoice` AS 

/* Additional grouping level is added since it is possible to have multiple markup(s) (categories) applied on the same line*/
;
SELECT	  --PurchaseInvoiceId AS PurchaseInvoiceCode
		CAST(REPLACE(CAST(UPPER(PurchaseInvoiceId) AS STRING), '?', '') AS STRING) AS PurchaseInvoiceCode
		, InternalInvoiceId AS InternalInvoiceCode
		, TransactionType
		, PurchaseInvoiceLineNumber
		, InvoiceLineNumberCombination
		, LineDescription
		, LineRecId
		, HeaderRecId
		, CompanyId AS CompanyCode
		, ProductId AS ProductCode
		, PurchaseOrderId AS PurchaseOrderCode
		, InventTransId AS InventTransCode
		, InventDimId AS InventDimCode
		, TaxWriteCode
		, SupplierId AS SupplierCode
		, DeliveryModeId AS DeliveryModeCode
		, PaymentTermsId AS PaymentTermsCode
		, DeliveryTermsId AS DeliveryTermsCode
		, PurchaseOrderStatus
		, TransactionCurrencyId AS TransactionCurrencyCode
		, DefaultDimension
		, InvoiceDate
		, PurchaseUnit
		, InvoicedQuantity
		, PurchasePricePerUnitTC
		, SUM(GrossPurchaseTC) AS GrossPurchaseTC
		, SUM(DiscountAmountTC) AS DiscountAmountTC
		, SUM(InvoicedPurchaseAmountTC) AS InvoicedPurchaseAmountTC
		, SUM(MarkupAmountTC) AS MarkupAmountTC
		, SUM(NetPurchaseTC) AS NetPurchaseTC
		, SUM(NetPurchaseInclTaxTC) AS NetPurchaseInclTaxTC

FROM (

SELECT
	--Information on fields
		  VIJS.InvoiceId AS PurchaseInvoiceId
		, VIJS.InternalInvoiceId AS InternalInvoiceId
		, CAST(CASE WHEN LEFT(VIJS.InternalInvoiceId, 1) = 'I' THEN 'Vendor Invoice'
					WHEN LEFT(VIJS.InternalInvoiceId, 3) = 'VIR' THEN 'Vendor Invoice'
					WHEN LEFT(VIJS.InternalInvoiceId, 1) = 'C' THEN 'Vendor Credit Note'
					WHEN LEFT(VIJS.InternalInvoiceId, 3) = 'RIN' THEN 'Rebate Vendor Invoice'
					ELSE '_N/A'
					END AS STRING) AS TransactionType
		, COALESCE(VITS.LineNum, -1) AS PurchaseInvoiceLineNumber
		, VITS.InvoiceId || ' - ' || CAST(CAST(VITS.LineNum AS int) AS STRING) AS InvoiceLineNumberCombination
		, COALESCE(NULLIF(VITS.Description, ''), '_N/A') AS LineDescription
		, VITS.VendInvoiceTransRecId AS LineRecId
		, VIJS.VendInvoiceJourRecId AS HeaderRecId
	--Dimensions
		, UPPER(VIJS.DataAreaId) AS CompanyId
		, COALESCE(NULLIF(UPPER(VITS.ItemId), ''), '_N/A') AS ProductId
		
		, COALESCE(NULLIF(PurchId, ''), '_N/A') AS PurchaseOrderId
		, COALESCE(VITS.InventTransId, '_N/A') AS InventTransId -- Required for the link to the purchase order
		, COALESCE(VITS.InventDimId, '_N/A') AS InventDimId
		, CASE WHEN TAXWRITECODE NOT IN ('21%', '12%', '6%')
			THEN 0
			WHEN TAXWRITECODE = '6%'
			THEN 6
			ELSE LTRIM(RTRIM(COALESCE(NULLIF(LEFT(REPLACE(VITS.TaxWriteCode, ',', ''), 2), ''), 0)))
		END AS TaxWriteCode
		, UPPER(COALESCE(NULLIF(VIJS.InvoiceAccount, ''), '_N/A')) AS SupplierId
		
		, UPPER(COALESCE(NULLIF(VIJS.DlvMode, ''), '_N/A')) AS DeliveryModeId
		, UPPER(COALESCE(NULLIF(VIJS.Payment, ''), '_N/A')) AS PaymentTermsId
		, UPPER(COALESCE(NULLIF(VIJS.DlvTerm, ''), '_N/A')) AS DeliveryTermsId
		
		, CAST('Invoiced' AS STRING) AS PurchaseOrderStatus
		
		, UPPER(COALESCE(VITS.CurrencyCode,VIJS.CurrencyCode)) AS TransactionCurrencyId
		
		--, VITS.DefaultDimension AS DefaultDimension
		, VITS.VendInvoiceTransDimension AS DefaultDimension
	--Dates
		, COALESCE(NULLIF(VIJS.InvoiceDate, ''), '1900-01-01') AS InvoiceDate

	--Measures: Volume
		, VITS.PurchUnit AS PurchaseUnit
		, COALESCE(VITS.Qty, 0) AS InvoicedQuantity
	--Measures: €
	--PurchPricePerUnit
		, COALESCE(VITS.PurchPrice/COALESCE((CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END), 0), 0) AS PurchasePricePerUnitTC
	
	--Gross Purchase
		, VITS.LineAmount
			+ (VITS.LinePercent/100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) --% discount on gross Purchase (=quantity * price per unit)
				+ VITS.LineDisc * VITS.Qty) -- Fixed discount per unit)	
			+ CASE 
				WHEN COALESCE(MTS1.MarkupCategory, 0) = 0
					THEN COALESCE(MTS1.Markup, 0) -- Fixed markup
				WHEN MTS1.MarkupCategory = 1
					THEN COALESCE(MTS1.Markup, 0) * VITS.PurchPrice/(CASE WHEN VITS.PurchPrice = 0 THEN 1 ELSE VITS.PurchPrice END) --Surcharge is # pieces * Unit price (only on line level)
				WHEN MTS1.MarkupCategory = 2 
					THEN COALESCE(MTS1.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
				END 
					+ CASE
					WHEN COALESCE(MTS2.MarkupCategory, 0) = 0
					THEN COALESCE(MTS2.Markup, 0) -- Fixed markup
					WHEN MTS2.MarkupCategory = 2 
					THEN COALESCE(MTS2.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
					END 
		  AS GrossPurchaseTC 
	--DiscountAmount
		, VITS.LinePercent/100.0 * (VITS.Qty*VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 
																   THEN 1 
																   ELSE VITS.PriceUnit 
															  END)) --% discount on gross Purchase (=quantity * price per unit)
								+ VITS.LineDisc * VITS.Qty 
		  AS DiscountAmountTC -- Fixed discount per unit
	--LineAmountTransactionCurrency
		, VITS.LineAmount
			+ CASE 
				WHEN COALESCE(MTS1.MarkupCategory, 0) = 0
					THEN COALESCE(MTS1.Markup, 0) -- Fixed markup
				WHEN MTS1.MarkupCategory = 1
					THEN COALESCE(MTS1.Markup, 0) * VITS.PurchPrice/(CASE WHEN VITS.PurchPrice = 0 THEN 1 ELSE VITS.PurchPrice END) --Surcharge is # pieces * Unit price (only on line level)
				WHEN MTS1.MarkupCategory = 2 
					THEN COALESCE(MTS1.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
				END 
					+ CASE
						WHEN COALESCE(MTS2.MarkupCategory, 0) = 0
						THEN COALESCE(MTS2.Markup, 0) -- Fixed markup
						WHEN MTS2.MarkupCategory = 2 
						THEN COALESCE(MTS2.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
					END
			AS InvoicedPurchaseAmountTC
		--, InvoicedPurchaseAmountAC = VITS.LineAmountMst --> Will be calculated in the next DataStore
	--MarkupAmount (header + line)
		,   CASE WHEN COALESCE(MTS1.MarkupCategory, 0) = 0
				  THEN COALESCE(MTS1.Markup, 0) -- Fixed markup
			     WHEN MTS1.MarkupCategory = 1
				  THEN COALESCE(MTS1.Markup, 0) * VITS.PurchPrice/(CASE WHEN VITS.PurchPrice = 0 
																    THEN 1 
																	ELSE VITS.PurchPrice 
																END) --Surcharge is # pieces * Unit price (only on line level)
				WHEN MTS1.MarkupCategory = 2 
				 THEN COALESCE(MTS1.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 
																					  THEN 1 
																					  ELSE VITS.PriceUnit 
																				  END)) -- Markup is % of gross Purchase (=quantity * price per unit)
			END 
			 + CASE
			   WHEN COALESCE(MTS2.MarkupCategory, 0) = 0
				THEN COALESCE(MTS2.Markup, 0) -- Fixed markup
			   WHEN MTS2.MarkupCategory = 2 
				THEN COALESCE(MTS2.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
			   END
			AS MarkupAmountTC
	--NetPurchase
		, VITS.LineAmount AS NetPurchaseTC
		--, (CAST(LTRIM(RTRIM(ISNULL(NULLIF(LEFT(REPLACE(VITS.TaxWriteCode, ',', ''), 2), ''), 0))) as numeric(20,6))/100+1) * --tax 
			,VITS.LineAmount AS NetPurchaseInclTaxTC

FROM dbo.SMRBIVendInvoiceJourStaging VIJS

INNER JOIN dbo.SMRBIVendInvoiceTransStaging VITS
ON VIJS.InvoiceId = VITS.InvoiceId
	and VIJS.DataAreaId = VITS.DataAreaId
	and VITS.InvoiceDate = VIJS.InvoiceDate 
	and VITS.InternalInvoiceId = VIJS.InternalInvoiceId

-- Required for Markup on Line Level
LEFT JOIN 
	(SELECT	MTS.DataAreaId
			, MTS.MarkupCategory
			, MTS.TransRecId
			, Markup = SUM(CASE WHEN MTS.CurrencyCode = VITS.CurrencyCode THEN `Value` ELSE `Value` * COALESCE(ER.ExchangeRate, 1) END)
		FROM dbo.SMRBIMarkupTransStaging MTS
		LEFT JOIN dbo.SMRBIVendInvoiceTransStaging VITS
		ON VITS.VendInvoiceTransRecId = MTS.TransRecId
			and VITS.DataAreaId = MTS.DataAreaId
			and MTS.TransTableId IN (SELECT TableId FROM ETL.SQLDictionary WHERE TableName = 'VendInvoiceTrans')
		--Required for Currencies:
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
		ON MTS.DataAreaId = L.`Name`

		LEFT JOIN DataStore.ExchangeRate ER --Markups do not need to be booked in the Invoice currency -> Convert the markup to the invoice transaction currency!
		ON ER.FromCurrencyCode = MTS.CurrencyCode
			and ER.ToCurrencyCode = VITS.CurrencyCode
			and ER.ExchangeRateTypeCode = L.ExchangeRateType
			and VITS.InvoiceDate BETWEEN ER.ValidFrom AND ER.ValidTo

		WHERE 1=1
			and TransTableId IN (SELECT TableId FROM ETL.SQLDictionary WHERE TableName = 'VendInvoiceTrans')
		GROUP BY MTS.DataAreaId, MTS.MarkupCategory, MTS.TransRecId) MTS1
ON VITS.VendInvoiceTransRecId = MTS1.TransRecId
	and VITS.DataAreaId = MTS1.DataAreaId

-- Required for Markup on Header Level
LEFT JOIN 
	(SELECT	MTS.DataAreaId
			, MTS.MarkupCategory
			, MTS.TransRecId
			, Markup = SUM(CASE WHEN MTS.CurrencyCode = VIJS.CurrencyCode THEN `Value` ELSE `Value` * COALESCE(ER.ExchangeRate, 1) END)
		FROM dbo.SMRBIMarkupTransStaging MTS
		LEFT JOIN dbo.SMRBIVendInvoiceJourStaging VIJS
		ON VIJS.VendInvoiceJourRecId = MTS.TransRecId
			and VIJS.DataAreaId = MTS.DataAreaId
			and MTS.TransTableId IN (SELECT TableId FROM ETL.SQLDictionary WHERE TableName = 'VendInvoiceJour')
		--Required for Currencies:
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
		ON MTS.DataAreaId = L.`Name`

		LEFT JOIN DataStore.ExchangeRate ER --Markups do not need to be booked in the Invoice currency -> Convert the markup to the invoice transaction currency!
		ON ER.FromCurrencyCode = MTS.CurrencyCode
			and ER.ToCurrencyCode = VIJS.CurrencyCode
			and ER.ExchangeRateTypeCode = L.ExchangeRateType
			and VIJS.InvoiceDate BETWEEN ER.ValidFrom AND ER.ValidTo

		WHERE 1=1
			and TransTableId IN (SELECT TableId FROM ETL.SQLDictionary WHERE TableName = 'VendInvoiceJour')
		GROUP BY MTS.DataAreaId, MTS.MarkupCategory, MTS.TransRecId
	) MTS2
ON VIJS.VendInvoiceJourRecId = MTS2.TransRecId
	and VIJS.DataAreaId = MTS2.DataAreaId
	and VITS.LineNum = 1 -- Header surcharges are taken into account on the first Purchase Invoice line only


--Add Cost Invoices

UNION ALL

SELECT
	--Information on fields
		  VIJS.InvoiceId AS PurchaseInvoiceId 
		, VIJS.InternalInvoiceId AS InternalInvoiceId
		, CAST(CASE WHEN LEFT(VIJS.InternalInvoiceId, 1) = 'I' THEN 'Vendor Invoice'
									WHEN LEFT(VIJS.InternalInvoiceId, 3) = 'VIR' THEN 'Vendor Invoice'
									WHEN LEFT(VIJS.InternalInvoiceId, 1) = 'C' THEN 'Vendor Credit Note'
									WHEN LEFT(VIJS.InternalInvoiceId, 3) = 'RIN' THEN 'Rebate Vendor Invoice'
									ELSE '_N/A' --Check if this 
									END AS STRING) AS TransactionType
		, 0 AS PurchaseInvoiceLineNumber
		, VIJS.InvoiceId || ' - ' || CAST(0 AS STRING) AS InvoiceLineNumberCombination
		, 'No lines' AS LineDescription
		, 0 AS LineRecId
		, VIJS.VendInvoiceJourRecId AS HeaderRecId
	--Dimensions
		, UPPER(VIJS.DataAreaId) AS CompanyId
		, 'Cost Bill' AS ProductId
		
		, COALESCE(NULLIF(VIJS.PurchId, ''), '_N/A') AS PurchaseOrderId
		, COALESCE(VITS.InventTransId, '_N/A') AS InventTransId --Required for the link to the purchase order
		, COALESCE(VITS.InventDimId, '_N/A') AS InventDimId
		, COALESCE(CAST(ROUND(
/*--VIJS.SumTax >>> FIELD NEEDS TO BE ADDED BACK IN!*/
1 / 
NULLIF(
(VIJS.InvoiceAmount
-- - VIJS.SumTax >>> FIELD NEEDS TO BE ADDED BACK IN! --Tax is by default included in the Invoice amount
- (CASE 
WHEN COALESCE(MTS.MarkupCategory, 0) = 0
THEN COALESCE(MTS.Markup, 0) -- Fixed markup
WHEN MTS.MarkupCategory = 2
THEN COALESCE(MTS.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) --markup is % of gross Purchase (=quantity * price per unit)
--
END)
),0)
, 2)
* 100 AS int), 0) AS TaxWriteCode --GrossPurchase - DiscountAmount - MarkupAmount
		, UPPER(COALESCE(NULLIF(VIJS.InvoiceAccount, ''), '_N/A')) AS SupplierId
		
		, UPPER(COALESCE(NULLIF(VIJS.DlvMode, ''), '_N/A')) AS DeliveryModeId
		, UPPER(COALESCE(NULLIF(VIJS.Payment, ''), '_N/A')) AS PaymentTermsId
		, UPPER(COALESCE(NULLIF(VIJS.DlvTerm, ''), '_N/A')) AS DeliveryTermsId
		
		, CAST('Invoiced' AS STRING) AS PurchaseOrderStatus
		
		, UPPER(VIJS.CurrencyCode) AS TransactionCurrencyId
		
		, 0 AS DefaultDimension
	--Dates
		, COALESCE(NULLIF(VIJS.InvoiceDate, ''), '1900-01-01') AS InvoiceDate

	--Measures: Volume
		, CAST('_N/A' AS STRING) AS PurchaseUnit 
		, CAST(0 AS DECIMAL(32,17)) AS InvoicedQuantity
	--Measures: €
	--PurchPricePerUnit
		, CAST(0 AS DECIMAL(32,17)) AS PurchasePricePerUnitTC 
	--Gross Purchase
		, VIJS.InvoiceAmount -- - VIJS.SumTax >>> FIELD NEEDS TO BE ADDED BACK IN!
			 AS GrossPurchaseTC
	--DiscountAmount
		, CAST(0 AS DECIMAL(32,17)) AS DiscountAmountTC
	--LineAmountTransactionCurrency
		, VIJS.InvoiceAmount -- - VIJS.SumTax >>> FIELD NEEDS TO BE ADDED BACK IN!
			AS InvoicedPurchaseAmountTC
	--MarkupAmount (header + line)
		,  CASE
			   WHEN COALESCE(MTS.MarkupCategory, 0) = 0
				THEN COALESCE(MTS.Markup, 0) -- Fixed markup
			   WHEN MTS.MarkupCategory = 2 
				THEN COALESCE(MTS.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) -- Markup is % of gross Purchase (=quantity * price per unit)
		   END AS MarkupAmountTC
	--NetPurchase
		, VIJS.InvoiceAmount
			-- - VIJS.SumTax >>> FIELD NEEDS TO BE ADDED BACK IN! --Tax is by default included in the Invoice amount
			- (CASE 
					WHEN COALESCE(MTS.MarkupCategory, 0) = 0
						THEN COALESCE(MTS.Markup, 0) -- Fixed markup
					WHEN MTS.MarkupCategory = 2
						THEN COALESCE(MTS.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) --markup is % of gross Purchase (=quantity * price per unit)
														--
				END) AS NetPurchaseTC --GrossPurchase - DiscountAmount - MarkupAmount
		, VIJS.InvoiceAmount
			- (CASE 
					WHEN COALESCE(MTS.MarkupCategory, 0) = 0
						THEN COALESCE(MTS.Markup, 0) -- Fixed markup
					WHEN MTS.MarkupCategory = 2
						THEN COALESCE(MTS.Markup, 0) /100.0 * (VITS.Qty * VITS.PurchPrice/(CASE WHEN VITS.PriceUnit = 0 THEN 1 ELSE VITS.PriceUnit END)) --markup is % of gross Purchase (=quantity * price per unit)
														--
				END) AS NetPurchaseInclTaxTC --GrossPurchase - DiscountAmount - MarkupAmount

FROM dbo.SMRBIVendInvoiceJourStaging VIJS

LEFT JOIN dbo.SMRBIVendInvoiceTransStaging VITS
ON VIJS.InvoiceId = VITS.InvoiceId
	and VIJS.DataAreaId = VITS.DataAreaId
	and VITS.InvoiceDate = VIJS.InvoiceDate 
	and VITS.InternalInvoiceId = VIJS.InternalInvoiceId

-- Required for Markup on Header Level
LEFT JOIN 
	(SELECT	MTS.DataAreaId
			, MTS.MarkupCategory
			, MTS.TransRecId
			, Markup = SUM(CASE WHEN MTS.CurrencyCode = VIJS.CurrencyCode THEN `Value` ELSE `Value` * COALESCE(ER.ExchangeRate, 1) END)
		FROM dbo.SMRBIMarkupTransStaging MTS
		LEFT JOIN dbo.SMRBIVendInvoiceJourStaging VIJS
		ON VIJS.VendInvoiceJourRecId = MTS.TransRecId
			and VIJS.DataAreaId = MTS.DataAreaId
		--Required for Currencies:
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
		ON MTS.DataAreaId = L.`Name`

		LEFT JOIN DataStore.ExchangeRate ER --Markups do not need to be booked in the Invoice currency -> Convert the markup to the invoice transaction currency!
		ON ER.FromCurrencyCode = MTS.CurrencyCode
			and ER.ToCurrencyCode = VIJS.CurrencyCode
			and ER.ExchangeRateTypeCode = L.ExchangeRateType
			and VIJS.InvoiceDate BETWEEN ER.ValidFrom AND ER.ValidTo

		WHERE 1=1
			and TransTableId IN (SELECT TableId FROM ETL.SQLDictionary WHERE TableName = 'VendInvoiceJour')
		GROUP BY MTS.DataAreaId, MTS.MarkupCategory, MTS.TransRecId
	) MTS
ON VIJS.VendInvoiceJourRecId = MTS.TransRecId
	and VIJS.DataAreaId = MTS.DataAreaId

WHERE 1=1
	and VITS.InvoiceId IS NULL --No lines attached

) PCHI

WHERE 1=1

GROUP BY  PurchaseInvoiceId
		, InternalInvoiceId
		, TransactionType
		, PurchaseInvoiceLineNumber
		, InvoiceLineNumberCombination
		, LineDescription
		, LineRecId
		, HeaderRecId
		, CompanyId
		, ProductId
		, PurchaseOrderId
		, InventTransId
		, InventDimId
		, TaxWriteCode
		, SupplierId
		, DeliveryModeId
		, PaymentTermsId
		, DeliveryTermsId
		, PurchaseOrderStatus
		, TransactionCurrencyId
		, DefaultDimension
		, InvoiceDate
		, PurchaseUnit
		, InvoicedQuantity
		, PurchasePricePerUnitTC
;
