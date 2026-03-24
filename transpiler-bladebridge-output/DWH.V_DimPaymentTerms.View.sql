/****** Object:  View [DWH].[V_DimPaymentTerms]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimPaymentTerms` AS 


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, UPPER(PaymentTermsCode) PaymentTermsCode
		, PaymentTermsName

FROM DataStore.PaymentTerms

/* Create unknown member */

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode)
		       , '_N/A'
		       , '_N/A'
FROM DataStore.Company
;
