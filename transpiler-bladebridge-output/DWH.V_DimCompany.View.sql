/****** Object:  View [DWH].[V_DimCompany]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimCompany` AS 


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, CompanyName
		, CompanyCodeName
		, CompanyType

FROM DataStore.Company

/*CREATE UNKNOWN MEMBER*/

UNION ALL

SELECT	'_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
;
