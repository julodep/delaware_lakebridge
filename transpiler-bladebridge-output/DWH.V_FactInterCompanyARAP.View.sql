/****** Object:  View [DWH].[V_FactInterCompanyARAP]    Script Date: 03/03/2026 16:26:08 ******/











/****** Script for SelectTopNRows command from SSMS  ******/

CREATE OR REPLACE VIEW `DWH`.`V_FactInterCompanyARAP` AS 


SELECT
	  UPPER(`SalesInvoiceCode`				) AS SalesInvoiceCode
	, CAST(REPLACE(CAST(UPPER(PurchaseInvoiceCode) AS STRING), '?', '') AS STRING) AS PurchaseInvoiceCode
	--, UPPER([PurchaseInvoiceCode]			) AS PurchaseInvoiceCode
	, UPPER(`SupplierCode`					) AS SupplierCode
	, UPPER(`CustomerCode`					) AS CustomerCode
	, `DefaultExchangeRateTypeCode`			
	, UPPER(`TransactionCurrencyCode`		) AS TransactionCurrencyCode
	, UPPER(`AccountingCurrencyCode`		) AS AccountingCurrencyCode
	, UPPER(`ReportingCurrencyCode`			) AS ReportingCurrencyCode
	, UPPER(`GroupCurrencyCode`				) AS GroupCurrencyCode
	, `InvoiceAmountTC`
	, `InvoiceAmountAC`
	, `InvoiceAmountRC`
	, `InvoiceAmountGC`
	, `OpenAmountTC`
	, `OpenAmountAC`
	, `OpenAmountRC`
	, `OpenAmountGC`
	, `AmountInvoiceTC`
	, `AmountInvoiceAC`
	, `AmountInvoiceRC`
	, `AmountInvoiceGC`
	, `SupplierSettlement`
	, `CustomerSettlement`
	, `DimInvoiceDateId`
	, `DimDueDateId`
	, ReportDate AS DimReportDateId
	, `PostedDate`
	, `ETA`
	, `ETD`
	, `Branch`
	, `Departement`
	, BusinessType
	, `AR_AP_Type`
	, `Type`
	, `JobInvoice`
	, POD
	, `House`
	, ShipmentNumber
	, `Master`
	, `OrigCountry`
	, `OrigCountryName`
	, POL
	, UPPER(`CompanyCode`) AS CompanyCode
  FROM `DataStore2`.`InterCompanyARAP`


;
