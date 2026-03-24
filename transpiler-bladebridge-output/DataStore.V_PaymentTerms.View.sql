/****** Object:  View [DataStore].[V_PaymentTerms]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore`.`V_PaymentTerms` AS 


SELECT	UPPER(PTS.DataAreaId) AS CompanyCode
	  , UPPER(PTS.`Name`) AS PaymentTermsCode
	  , COALESCE(PTS.`Description`, '_N/A') AS PaymentTermsName

FROM dbo.SMRBIPaymentTermStaging PTS
;
