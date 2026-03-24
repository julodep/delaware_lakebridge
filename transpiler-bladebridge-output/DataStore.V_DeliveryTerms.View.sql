/****** Object:  View [DataStore].[V_DeliveryTerms]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore`.`V_DeliveryTerms` AS 


SELECT	  UPPER(DTS.DataAreaId) AS CompanyCode 
		, UPPER(DTS.TermsCode) AS DeliveryTermsCode
		, COALESCE(DTS.TermsDescription, '_N/A') AS DeliveryTermsName

FROM dbo.SMRBIDeliveryTermsStaging DTS
;
