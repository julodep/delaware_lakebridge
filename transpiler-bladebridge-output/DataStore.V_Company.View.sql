/****** Object:  View [DataStore].[V_Company]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_Company` AS 


SELECT	DISTINCT UPPER(OALES.CompanyId) AS CompanyCode
			   , OALES.CompanyName AS CompanyName
			   , UPPER(OALES.CompanyId) || ' ' || OALES.CompanyName AS CompanyCodeName
			   , COALESCE(CASE WHEN OALES.BusinessActivity = '' THEN '_N/A' ELSE OALES.BusinessActivity END, '_N/A') AS CompanyType

FROM dbo.SMRBIOfficeAddinLegalEntityStaging OALES
;
