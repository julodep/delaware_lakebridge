/****** Object:  View [DWH].[V_DimPurchaseInvoice]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DWH`.`V_DimPurchaseInvoice` AS


SELECT	  --UPPER(PurchaseInvoiceCode) AS PurchaseInvoiceCode
		CAST(REPLACE(CAST(UPPER(PurchaseInvoiceCode) AS STRING), '?', '') AS STRING) AS PurchaseInvoiceCode
		, UPPER(CompanyCode) AS CompanyCode
		, MIN(SupplierCode) AS SupplierCode

FROM DataStore2.PurchaseInvoice
WHERE PurchaseInvoiceCode <> '_N/A'
GROUP BY PurchaseInvoiceCode, CompanyCode

/* Create Unknown Member */

UNION ALL

SELECT	'_N/A' AS PurchaseInvoiceCode
	  , UPPER(CompanyCode) AS CompanyCode
	  , 'N/A' AS SupplierCode

FROM DataStore.Company
;
