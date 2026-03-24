/****** Object:  View [DataStore].[V_ForecastModel]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_ForecastModel` AS 


SELECT	UPPER(FMS.DataAreaId) AS CompanyCode
	  , UPPER(FMS.ModelId) AS ForecastModelCode
	  , COALESCE(FMS.ModelName, '_N/A') AS ForecastModelName
	  , COALESCE(UPPER(FSMS.SubModelId), '_N/A') AS ForecastSubModelCode
FROM dbo.SMRBIForecastModelStaging FMS

LEFT JOIN (SELECT DISTINCT * 
		   FROM dbo.SMRBIForecastSubModelStaging) FSMS

ON FMS.ModelId = FSMS.ModelId
;
