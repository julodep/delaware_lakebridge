/****** Object:  View [DWH].[V_DimForecastModel]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimForecastModel` AS 


SELECT  UPPER(CompanyCode) AS CompanyCode, 
		UPPER(ForecastModelCode) AS ForecastModelCode,
		ForecastModelName,
		ForecastSubModelCode

FROM DataStore.ForecastModel

/* Create unknown member */

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode) AS CompanyCode
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
FROM DataStore.Company
;
