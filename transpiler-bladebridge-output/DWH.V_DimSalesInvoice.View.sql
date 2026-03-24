/****** Object:  View [DWH].[V_DimSalesInvoice]    Script Date: 03/03/2026 16:26:09 ******/














CREATE OR REPLACE VIEW `DWH`.`V_DimSalesInvoice` AS


SELECT	DISTINCT 
		  UPPER(SI.SalesInvoiceCode) AS SalesInvoiceCode
		, UPPER(SI.CompanyCode) AS CompanyCode
		, '_N/A' AS CustomerCode
		, UPPER(SI.TransactionType) AS TransactionType

FROM DataStore3.SalesInvoice SI

/* Create unknown member */

UNION ALL

SELECT	 '_N/A' AS SalesInvoiceCode
		, UPPER(CompanyCode) AS CompanyCode
		, '_N/A' AS CustomerCode
		, '_N/A' AS TransactionType

FROM DataStore.Company
;
