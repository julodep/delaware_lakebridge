/****** Object:  View [DWH].[V_DimDeliveryTerms]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DWH`.`V_DimDeliveryTerms` AS 


SELECT    UPPER(CompanyCode) AS CompanyCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, DeliveryTermsName

FROM DataStore.DeliveryTerms

/* Create unknown member */

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode)
		       , '_N/A'
		       , '_N/A'

FROM DataStore.Company
;
