/****** Object:  View [DataStore].[V_Currency]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore`.`V_Currency` AS


SELECT  UPPER(CS.CurrencyCode) AS CurrencyCode
	  , COALESCE(CS.`Name`, '_N/A') AS CurrencyName
FROM dbo.SMRBICurrencyStaging CS
;
